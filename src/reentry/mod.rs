//! Re-entry shadow watcher.
//!
//! Polls Supabase for newly-closed positions, tracks each closed mint for
//! `window_seconds` after its exit, and periodically evaluates whether a
//! re-entry *would* pass our gates. Every evaluation is logged to
//! `reentry_candidates`. A separate loop backfills realized-price outcomes
//! at +30m / +2h / +6h so we can later correlate score & gates vs. PnL.
//!
//! SHADOW MODE: this module never places a trade. It only logs data.
//! When config `shadow_mode=false` is eventually flipped, the execution
//! wiring will be added here (TODO: not yet implemented).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::detection::types::{DetectionSource, GraduatedToken, PipelineTiming};
use crate::execution::jupiter::JupiterClient;
use crate::filters::types::{FilteredToken, FilterSummary};
use crate::logger::SupabaseClient;
use crate::narrative::{self, NarrativeContext, NarrativeResult};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tokio::sync::mpsc;

// ── In-memory tracked exit state ─────────────────────────────

struct TrackedExit {
    mint: String,
    symbol: String,
    token_name: String,
    position_id: i64,
    exit_price_usd: f64,
    exit_time: DateTime<Utc>,
    exit_pnl_pct: f64,
    exit_reason: String,
    exit_was_profitable: bool,
    /// Cumulative re-entry attempts recorded for this mint (in-memory counter).
    attempts: u16,
    /// Last narrative score recorded for this mint (0..100) — nullable.
    previous_score: Option<u8>,
}

// ── Row shape for Supabase query on positions ────────────────

#[derive(Debug, Deserialize)]
struct ClosedPositionRow {
    id: i64,
    mint: String,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    exit_price_usd: Option<f64>,
    #[serde(default)]
    pnl_pct: Option<f64>,
    #[serde(default)]
    exit_reason: Option<String>,
    #[serde(default)]
    exit_time: Option<DateTime<Utc>>,
    #[serde(default)]
    closed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    peak_multiplier: Option<f64>,
}

// ── Row shape for outcome backfill ───────────────────────────

#[derive(Debug, Deserialize)]
struct PendingOutcomeRow {
    id: i64,
    mint: String,
    created_at: DateTime<Utc>,
    current_price_usd: f64,
    price_30m_after: Option<f64>,
    price_2h_after: Option<f64>,
    price_6h_after: Option<f64>,
}

// ── Gate evaluation ──────────────────────────────────────────

#[derive(Debug, Default, Serialize)]
struct Gates {
    window: bool,
    dip: bool,
    narrative_available: bool,
    narrative_threshold: bool,
}

impl Gates {
    fn all_pass(&self) -> bool {
        self.window && self.dip && self.narrative_available && self.narrative_threshold
    }

    fn first_failing(&self) -> Option<&'static str> {
        if !self.window {
            Some("window_expired")
        } else if !self.dip {
            Some("insufficient_dip")
        } else if !self.narrative_available {
            Some("narrative_unavailable")
        } else if !self.narrative_threshold {
            Some("narrative_below_threshold")
        } else {
            None
        }
    }
}

// ── Main entry point ─────────────────────────────────────────

/// Spawn the re-entry watcher. Takes ownership of the config + supabase clone.
/// Does nothing if `reentry.enabled = false` or required API keys are missing.
///
/// `filter_tx` is the same channel the filter engine pushes `FilteredToken`s
/// into. When `shadow_mode=false` and gates pass, the watcher injects a
/// synthetic `FilteredToken` so the execution engine treats it like any other
/// buy. With `PAPER_TRADE=true`, the result is a paper-trade re-entry.
pub fn start(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    filter_tx: mpsc::Sender<FilteredToken>,
) {
    if !cfg.strategy.reentry.enabled {
        info!("Re-entry watcher disabled (reentry.enabled = false)");
        return;
    }

    let openai_key = cfg.env.openai_api_key.clone();
    let x_bearer = cfg.env.x_api_bearer_token.clone();
    let birdeye_key = cfg.env.birdeye_api_key.clone();

    if openai_key.is_none() || x_bearer.is_none() || birdeye_key.is_none() {
        warn!(
            has_openai = openai_key.is_some(),
            has_x = x_bearer.is_some(),
            has_birdeye = birdeye_key.is_some(),
            "Re-entry watcher: narrative scoring requires OPENAI_API_KEY, X_API_BEARER_TOKEN, \
             and BIRDEYE_API_KEY. Watcher will run but narrative_available gate will always fail."
        );
    }

    let shadow = cfg.strategy.reentry.shadow_mode;
    info!(
        shadow_mode = shadow,
        window_seconds = cfg.strategy.reentry.window_seconds,
        check_interval_seconds = cfg.strategy.reentry.check_interval_seconds,
        "Re-entry watcher starting"
    );

    // Evaluation loop — enqueue newly-closed positions + evaluate tracked set.
    let eval_cfg = Arc::clone(&cfg);
    let eval_supa = Arc::clone(&supabase);
    let eval_tx = filter_tx.clone();
    tokio::spawn(async move {
        run_evaluator(eval_cfg, eval_supa, eval_tx).await;
    });

    // Outcome backfill loop — separate cadence.
    let outcome_cfg = Arc::clone(&cfg);
    let outcome_supa = Arc::clone(&supabase);
    tokio::spawn(async move {
        run_outcome_backfill(outcome_cfg, outcome_supa).await;
    });
}

// ── Evaluator loop ───────────────────────────────────────────

async fn run_evaluator(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    filter_tx: mpsc::Sender<FilteredToken>,
) {
    let jupiter = Arc::new(JupiterClient::new(
        cfg.strategy.execution.api_request_timeout_secs,
        cfg.strategy.execution.max_retries,
    ));
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("reqwest client");

    let mut tracked: HashMap<String, TrackedExit> = HashMap::new();
    let mut seen_positions: HashSet<i64> = HashSet::new();

    let interval = Duration::from_secs(cfg.strategy.reentry.check_interval_seconds.max(10));

    loop {
        // 1. Enqueue newly-closed positions
        if let Err(e) = enqueue_new_closed(
            &supabase,
            &cfg,
            &mut tracked,
            &mut seen_positions,
        )
        .await
        {
            warn!("Re-entry enqueue poll failed: {}", e);
        }

        // 2. Evaluate each tracked mint
        let window_secs = cfg.strategy.reentry.window_seconds as i64;
        let now = Utc::now();
        let mut expired: Vec<String> = Vec::new();

        for (mint, exit) in tracked.iter_mut() {
            let age = (now - exit.exit_time).num_seconds();
            if age > window_secs {
                expired.push(mint.clone());
                continue;
            }

            match evaluate_candidate(&cfg, &supabase, &jupiter, &http, &filter_tx, exit).await {
                Ok(Some(score)) => exit.previous_score = Some(score),
                Ok(None) => {}
                Err(e) => warn!(mint = %mint, "Re-entry evaluation failed: {}", e),
            }
        }

        for mint in expired {
            if let Some(exit) = tracked.remove(&mint) {
                info!(
                    mint = %mint,
                    attempts = exit.attempts,
                    "Re-entry watch window closed"
                );
            }
        }

        tokio::time::sleep(interval).await;
    }
}

async fn enqueue_new_closed(
    supabase: &SupabaseClient,
    cfg: &AppConfig,
    tracked: &mut HashMap<String, TrackedExit>,
    seen_positions: &mut HashSet<i64>,
) -> Result<()> {
    let lookback = cfg.strategy.reentry.enqueue_lookback_seconds as i64;
    let cutoff = (Utc::now() - chrono::Duration::seconds(lookback))
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

    let url = format!(
        "{}/positions?status=eq.closed&closed_at=gte.{}&select=id,mint,symbol,name,exit_price_usd,pnl_pct,exit_reason,exit_time,closed_at,peak_multiplier&order=closed_at.desc&limit=100",
        supabase.base_url, cutoff
    );

    let resp = supabase
        .client
        .get(&url)
        .send()
        .await
        .context("positions poll failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("positions poll HTTP {}: {}", status, body);
    }

    let rows: Vec<ClosedPositionRow> = resp.json().await.context("positions poll decode")?;

    for row in rows {
        if seen_positions.contains(&row.id) {
            continue;
        }
        let exit_time = match row.exit_time.or(row.closed_at) {
            Some(t) => t,
            None => continue,
        };
        let exit_price = row.exit_price_usd.unwrap_or(0.0);
        if exit_price <= 0.0 {
            seen_positions.insert(row.id);
            continue;
        }
        let pnl_pct = row.pnl_pct.unwrap_or(0.0);
        let profitable = pnl_pct >= 0.0;

        // Piggyback gate: only track exits that hit the moonbag peak floor.
        let peak = row.peak_multiplier.unwrap_or(0.0);
        let peak_floor = cfg.strategy.reentry.min_peak_multiplier_to_track;
        if peak_floor > 0.0 && peak < peak_floor {
            seen_positions.insert(row.id);
            debug!(
                mint = %row.mint,
                position_id = row.id,
                peak,
                floor = peak_floor,
                "Re-entry watcher: skipped (peak below moonbag floor)"
            );
            continue;
        }

        tracked.insert(
            row.mint.clone(),
            TrackedExit {
                mint: row.mint.clone(),
                symbol: row.symbol.unwrap_or_default(),
                token_name: row.name.unwrap_or_default(),
                position_id: row.id,
                exit_price_usd: exit_price,
                exit_time,
                exit_pnl_pct: pnl_pct,
                exit_reason: row.exit_reason.unwrap_or_default(),
                exit_was_profitable: profitable,
                attempts: 0,
                previous_score: None,
            },
        );
        seen_positions.insert(row.id);
        info!(
            mint = %row.mint,
            position_id = row.id,
            peak,
            exit_pnl_pct = pnl_pct,
            profitable = profitable,
            "Re-entry watcher: enqueued closed position"
        );
    }
    Ok(())
}

async fn evaluate_candidate(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    jupiter: &JupiterClient,
    http: &reqwest::Client,
    filter_tx: &mpsc::Sender<FilteredToken>,
    exit: &mut TrackedExit,
) -> Result<Option<u8>> {
    let now = Utc::now();
    let seconds_since_exit = (now - exit.exit_time).num_seconds().max(0) as i32;

    // 1. Current price via Jupiter.
    let current_price = match jupiter.get_price(&exit.mint).await {
        Ok(p) if p > 0.0 => p,
        Ok(_) => {
            debug!(mint = %exit.mint, "Re-entry: Jupiter price is 0 — skipping");
            return Ok(None);
        }
        Err(e) => {
            debug!(mint = %exit.mint, "Re-entry: Jupiter price fetch failed: {}", e);
            return Ok(None);
        }
    };

    let dip_pct = if exit.exit_price_usd > 0.0 {
        (exit.exit_price_usd - current_price) / exit.exit_price_usd
    } else {
        0.0
    };

    let mut gates = Gates::default();
    gates.window = true; // already guarded by caller
    gates.dip = dip_pct >= cfg.strategy.reentry.min_dip_pct;

    // 2. Narrative score — only if keys available, dip gate passed, AND
    // require_narrative is true. When require_narrative=false (dip-only
    // piggyback shadow), auto-pass the narrative gates so we can collect
    // post-moonbag-exit price data without burning OpenAI/X credits per tick.
    let mut narrative_result: Option<NarrativeResult> = None;
    if !cfg.strategy.reentry.require_narrative {
        gates.narrative_available = true;
        gates.narrative_threshold = true;
    } else if gates.dip {
        if let (Some(openai), Some(x_bearer), Some(birdeye)) = (
            cfg.env.openai_api_key.as_deref(),
            cfg.env.x_api_bearer_token.as_deref(),
            cfg.env.birdeye_api_key.as_deref(),
        ) {
            let ctx = NarrativeContext {
                mint: exit.mint.clone(),
                name: exit.token_name.clone(),
                symbol: exit.symbol.clone(),
                current_price_usd: current_price,
                entry_price_usd: exit.exit_price_usd,
                peak_multiplier: 1.0,
                hold_seconds: seconds_since_exit as u64,
                buy_count: 0,
                sell_count: 0,
                momentum_ratio: 0.0,
                buy_volume_sol: 0.0,
                sell_volume_sol: 0.0,
            };
            match narrative::check_narrative(http, openai, birdeye, x_bearer, &ctx).await {
                Ok(res) => {
                    gates.narrative_available = true;
                    gates.narrative_threshold =
                        res.score >= cfg.strategy.reentry.min_narrative_score;
                    narrative_result = Some(res);
                }
                Err(e) => {
                    warn!(mint = %exit.mint, "Re-entry narrative check failed: {}", e);
                }
            }
        }
    }

    // If dip hasn't hit, don't record a row — otherwise we'd spam rows every 90s
    // for every tracked mint. Only record when dip gate passes (= meaningful
    // candidate moment). Consumers can infer "no candidate row" = dip never hit.
    if !gates.dip {
        return Ok(None);
    }

    exit.attempts = exit.attempts.saturating_add(1);
    let attempt_num = exit.attempts;
    let previous_score = exit.previous_score;

    let would_enter_permissive = gates.all_pass();
    let would_enter_strict = would_enter_permissive && exit.exit_was_profitable;
    let block_reason = gates.first_failing().map(|s| s.to_string()).or_else(|| {
        if !exit.exit_was_profitable {
            Some("exit_not_profitable".to_string())
        } else {
            None
        }
    });

    let (narrative_score, narrative_state, narrative_tier, narrative_latency_ms, narrative_json) =
        match &narrative_result {
            Some(r) => (
                Some(r.score as i16),
                Some(r.state.to_string()),
                Some(r.tier.clone()),
                Some(r.total_ms as i64),
                serde_json::to_value(r).ok(),
            ),
            None => (None, None, None, None, None),
        };

    let payload = serde_json::json!({
        "mint": exit.mint,
        "symbol": exit.symbol,
        "token_name": exit.token_name,
        "position_id": exit.position_id,
        "reentry_attempt": attempt_num as i32,
        "moonbag_exit_price_usd": exit.exit_price_usd,
        "moonbag_exit_time": exit.exit_time.to_rfc3339(),
        "moonbag_exit_pnl_pct": exit.exit_pnl_pct,
        "moonbag_exit_reason": exit.exit_reason,
        "exit_was_profitable": exit.exit_was_profitable,
        "check_time": now.to_rfc3339(),
        "current_price_usd": current_price,
        "dip_pct_from_exit": dip_pct,
        "seconds_since_exit": seconds_since_exit,
        "narrative_score": narrative_score,
        "narrative_state": narrative_state,
        "narrative_tier": narrative_tier,
        "narrative_result": narrative_json,
        "narrative_latency_ms": narrative_latency_ms,
        "previous_attempt_score": previous_score.map(|s| s as i16),
        "gates_passed": serde_json::to_value(&gates).unwrap_or(serde_json::Value::Null),
        "would_enter_strict": would_enter_strict,
        "would_enter_permissive": would_enter_permissive,
        "block_reason": block_reason,
    });

    let url = format!("{}/reentry_candidates", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!("reentry_candidates insert failed: HTTP {} — {}", status, body);
        }
        Err(e) => warn!("reentry_candidates insert failed: {}", e),
    }

    info!(
        mint = %exit.mint,
        attempt = attempt_num,
        dip_pct = format!("{:.2}%", dip_pct * 100.0),
        narrative_score = ?narrative_score,
        would_enter_strict,
        would_enter_permissive,
        shadow = cfg.strategy.reentry.shadow_mode,
        "🔁 Re-entry candidate evaluated"
    );

    // ── Inject paper-trade re-entry into execution pipeline ──
    // When shadow_mode=false and gates pass, build a synthetic FilteredToken
    // and push it onto the filter→execution channel. With PAPER_TRADE=true
    // this becomes a paper-trade re-entry; with PAPER_TRADE=false a real one.
    if !cfg.strategy.reentry.shadow_mode && would_enter_permissive {
        match build_synthetic_filtered_token(exit, current_price) {
            Ok(ft) => match filter_tx.try_send(ft) {
                Ok(()) => info!(
                    mint = %exit.mint,
                    attempt = attempt_num,
                    "🔁⚡ Re-entry injected into execution pipeline"
                ),
                Err(e) => warn!(mint = %exit.mint, "Re-entry inject failed: {}", e),
            },
            Err(e) => warn!(mint = %exit.mint, "Synthetic FilteredToken build failed: {}", e),
        }
    }

    Ok(narrative_score.map(|s| s as u8))
}

// ── Synthetic FilteredToken builder (re-entry injection) ─────────────────

fn build_synthetic_filtered_token(
    exit: &TrackedExit,
    current_price_usd: f64,
) -> Result<FilteredToken> {
    let mint = Pubkey::from_str(&exit.mint).context("parse mint pubkey")?;
    let creator = Pubkey::default();

    let mut sniper_features = serde_json::Map::new();
    sniper_features.insert(
        "entry_tier".to_string(),
        serde_json::Value::String("reentry".to_string()),
    );
    sniper_features.insert(
        "reentry_attempt".to_string(),
        serde_json::Value::from(exit.attempts as i64),
    );
    sniper_features.insert(
        "reentry_origin_position_id".to_string(),
        serde_json::Value::from(exit.position_id),
    );
    sniper_features.insert(
        "reentry_origin_exit_pnl_pct".to_string(),
        serde_json::Value::from(exit.exit_pnl_pct),
    );

    let event = GraduatedToken {
        mint,
        pool_address: None,
        creator_wallet: creator,
        bonding_curve_volume_sol: 0.0,
        buy_pressure_pct: 0.0,
        time_to_graduate_seconds: 0.0,
        detected_at: chrono::Utc::now().timestamp_millis(),
        source: DetectionSource::PumpFun,
        unique_buyer_count: 0,
        buy_count: 0,
        sell_count: 0,
        trade_timestamps: vec![],
        name: exit.token_name.clone(),
        symbol: exit.symbol.clone(),
        // High enough that compute_dynamic_buy_amount returns full buy size.
        initial_liquidity_sol: 1_000.0,
        creator_rebuy: false,
        buy_sell_ratio: 0.0,
        candidate_id: None,
        sniper_features: Some(serde_json::Value::Object(sniper_features)),
        sniper_score: None,
        pipeline_timing: PipelineTiming::default(),
    };

    Ok(FilteredToken {
        event,
        filter_summary: FilterSummary::from_results(vec![]),
        market_cap_usd: None,
        liquidity_usd: None,
        rugcheck_score: None,
        // Setting filter_price = current_price means the anti-chase check sees
        // 0% move at execution time — re-entries are intentionally entered at
        // the dipped price, not chasing.
        filter_price_usd: Some(current_price_usd),
        pipeline_timing: PipelineTiming::default(),
    })
}

// ── Outcome backfill loop ────────────────────────────────────

async fn run_outcome_backfill(cfg: Arc<AppConfig>, supabase: Arc<SupabaseClient>) {
    let jupiter = Arc::new(JupiterClient::new(
        cfg.strategy.execution.api_request_timeout_secs,
        cfg.strategy.execution.max_retries,
    ));
    let interval = Duration::from_secs(cfg.strategy.reentry.outcome_interval_seconds.max(60));

    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = backfill_outcomes_once(&cfg, &supabase, &jupiter).await {
            warn!("Re-entry outcome backfill failed: {}", e);
        }
    }
}

async fn backfill_outcomes_once(
    _cfg: &AppConfig,
    supabase: &SupabaseClient,
    jupiter: &JupiterClient,
) -> Result<()> {
    // Find rows created at least 30m ago where any of the outcome snapshots is null.
    // We cap at 50 per pass to bound API load.
    let min_age = (Utc::now() - chrono::Duration::minutes(30))
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let url = format!(
        "{}/reentry_candidates?created_at=lte.{}&outcome_checked_at=is.null&select=id,mint,created_at,current_price_usd,price_30m_after,price_2h_after,price_6h_after&order=created_at.asc&limit=50",
        supabase.base_url, min_age
    );
    let resp = supabase
        .client
        .get(&url)
        .send()
        .await
        .context("outcome poll failed")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("outcome poll HTTP {}: {}", status, body);
    }
    let rows: Vec<PendingOutcomeRow> = resp.json().await.context("outcome decode")?;
    if rows.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    for row in rows {
        let age = now - row.created_at;
        let age_mins = age.num_minutes();

        let need_30m = row.price_30m_after.is_none() && age_mins >= 30;
        let need_2h = row.price_2h_after.is_none() && age_mins >= 120;
        let need_6h = row.price_6h_after.is_none() && age_mins >= 360;

        if !need_30m && !need_2h && !need_6h {
            continue;
        }

        let current = match jupiter.get_price(&row.mint).await {
            Ok(p) if p > 0.0 => p,
            _ => continue,
        };

        let mut patch = serde_json::Map::new();
        if need_30m {
            patch.insert("price_30m_after".to_string(), serde_json::json!(current));
        }
        if need_2h {
            patch.insert("price_2h_after".to_string(), serde_json::json!(current));
        }
        if need_6h {
            patch.insert("price_6h_after".to_string(), serde_json::json!(current));
            // At 6h+, compute hypothetical PnL and close the row out.
            let pnl = if row.current_price_usd > 0.0 {
                (current - row.current_price_usd) / row.current_price_usd * 100.0
            } else {
                0.0
            };
            patch.insert("hypothetical_pnl_6h_pct".to_string(), serde_json::json!(pnl));
            patch.insert("peak_price_6h".to_string(), serde_json::json!(current));
            patch.insert(
                "outcome_checked_at".to_string(),
                serde_json::json!(now.to_rfc3339()),
            );
        }

        let patch_url = format!("{}/reentry_candidates?id=eq.{}", supabase.base_url, row.id);
        match supabase
            .client
            .patch(&patch_url)
            .json(&serde_json::Value::Object(patch))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                warn!("reentry outcome patch HTTP {} — {}", status, body);
            }
            Err(e) => warn!("reentry outcome patch failed: {}", e),
        }
    }
    Ok(())
}
