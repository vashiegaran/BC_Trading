//! Moonbag tracker — manages promoted positions independently from TradingState.
//!
//! When a token hits TP1 AND passes the combined score check (on-chain strength
//! + narrative bonus >= promotion threshold), the monitoring engine promotes it
//! to a moonbag. The moonbag tracker takes over with phased trailing stops and
//! decaying price checks.
//!
//! Narrative state can both upgrade AND downgrade based on re-checks.
//! Downgrade requires N consecutive below-threshold checks (configurable).
//!
//! No TradingState slot is consumed — the slot is freed at promotion time.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::logger::SupabaseClient;
use crate::monitoring::types::{ExitReason, ExitResult, ExitSignal};
use crate::narrative::{self, NarrativeContext, NarrativeResult, NarrativeState};

// ── Types ────────────────────────────────────────────────────

/// How a position was promoted to moonbag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionSource {
    /// TP1 + narrative score >= threshold
    NarrativeTp1,
    /// TP2 + cached narrative score >= threshold
    NarrativeTp2,
    /// TP2 + fast runner auto-promote (hold < threshold, no narrative yet)
    FastRunner,
    /// CTO evaluation — strong outcome
    CtoStrong,
    /// CTO evaluation — moderate outcome
    CtoModerate,
    // ── v14 data-driven paths (precision-ranked from this bot's own data) ──
    /// Path C: pre-discovery liquidity floor — `0 < be_liquidity_usd <= 10_000`.
    /// 13% precision, 50% recall, 2.34x lift on n=145 sample.
    LiquidityFloor,
    /// Path C: off-hours + low 24h volume — `!is_us_hours && be_volume_24h_usd <= 25_000`.
    /// 30% precision, 37% recall, 5.44x lift on n=145 sample.
    OffHoursLowVol,
    /// Path D: BC fast-track score — `bc_score >= 70` (lowered from 80 in v14.1
    /// after live data showed median bc_score=85 with 60% above 80 yet zero
    /// promotions; the bottleneck was the eval site, not the gate).
    BcScore80,
}

impl std::fmt::Display for PromotionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NarrativeTp1 => write!(f, "narrative_tp1"),
            Self::NarrativeTp2 => write!(f, "narrative_tp2"),
            Self::FastRunner => write!(f, "fast_runner"),
            Self::CtoStrong => write!(f, "cto_strong"),
            Self::CtoModerate => write!(f, "cto_moderate"),
            Self::LiquidityFloor => write!(f, "liquidity_floor"),
            Self::OffHoursLowVol => write!(f, "off_hours_low_vol"),
            Self::BcScore80 => write!(f, "bc_score_80"),
        }
    }
}

/// Detailed result of paper-path evaluation — used both for routing
/// decisions and for the `path_eval_log` debug table (migration 024).
#[derive(Debug, Clone, Default)]
pub struct PaperPathEval {
    pub is_us_hours: Option<bool>,
    pub be_volume_24h_usd: Option<f64>,
    pub be_liquidity_usd: Option<f64>,
    pub bc_score: Option<f64>,
    pub match_off_hours_low_vol: bool,
    pub match_liquidity_floor: bool,
    pub match_bc_score_80: bool,
    pub eligible_count: u32,
    pub selected: Option<PromotionSource>,
}

/// Evaluate the v14 data-driven paper paths against `sniper_features`.
/// Returns full eligibility detail. Selection priority is C → B → D
/// (highest precision first). Caller uses `.selected` for routing and
/// the rest for `path_eval_log`.
pub fn evaluate_paper_paths_detail(features: Option<&serde_json::Value>) -> PaperPathEval {
    let mut out = PaperPathEval::default();
    let Some(f) = features else {
        return out;
    };
    let num = |k: &str| f.get(k).and_then(|v| v.as_f64());
    let boolean = |k: &str| f.get(k).and_then(|v| v.as_bool());

    out.is_us_hours = boolean("is_us_hours");
    out.be_volume_24h_usd = num("be_volume_24h_usd");
    out.be_liquidity_usd = num("be_liquidity_usd");
    out.bc_score = num("bc_score");

    // Path C — off-hours + low 24h vol (highest precision)
    let off_hours = out.is_us_hours == Some(false);
    let vol = out.be_volume_24h_usd.unwrap_or(0.0);
    out.match_off_hours_low_vol = off_hours && vol > 0.0 && vol <= 25_000.0;

    // Path B — liquidity floor
    let liq = out.be_liquidity_usd.unwrap_or(0.0);
    out.match_liquidity_floor = liq > 0.0 && liq <= 10_000.0;

    // Path D — bc_score gate (lowered to 70 after live data; see PromotionSource::BcScore80)
    out.match_bc_score_80 = out.bc_score.unwrap_or(0.0) >= 70.0;

    out.eligible_count = u32::from(out.match_off_hours_low_vol)
        + u32::from(out.match_liquidity_floor)
        + u32::from(out.match_bc_score_80);

    out.selected = if out.match_off_hours_low_vol {
        Some(PromotionSource::OffHoursLowVol)
    } else if out.match_liquidity_floor {
        Some(PromotionSource::LiquidityFloor)
    } else if out.match_bc_score_80 {
        Some(PromotionSource::BcScore80)
    } else {
        None
    };
    out
}

/// Backwards-compatible thin wrapper — returns just the selected path.
pub fn evaluate_paper_paths(features: Option<&serde_json::Value>) -> Option<PromotionSource> {
    evaluate_paper_paths_detail(features).selected
}

/// Best-effort write to `path_eval_log`. Background-spawned by the caller.
pub async fn write_path_eval_log(
    supabase: &Arc<crate::logger::SupabaseClient>,
    position_id: i64,
    mint: &str,
    tp_intercept: &'static str,
    openai_score: f64,
    min_score_gate: f64,
    is_fast_runner: bool,
    eval: &PaperPathEval,
    decision: &'static str,
) {
    let payload = serde_json::json!({
        "position_id":         position_id,
        "mint":                mint,
        "tp_intercept":        tp_intercept,
        "openai_score":        openai_score,
        "min_score_gate":      min_score_gate,
        "is_fast_runner":      is_fast_runner,
        "is_us_hours":         eval.is_us_hours,
        "be_volume_24h_usd":   eval.be_volume_24h_usd,
        "be_liquidity_usd":    eval.be_liquidity_usd,
        "bc_score":            eval.bc_score,
        "matched_path_c_off_hours_low_vol": eval.match_off_hours_low_vol,
        "matched_path_b_liquidity_floor":   eval.match_liquidity_floor,
        "matched_path_d_bc_score_80":       eval.match_bc_score_80,
        "matched_path":        eval.selected.as_ref().map(|p| p.to_string()),
        "eligible_count":      eval.eligible_count,
        "decision":            decision,
    });
    let url = format!("{}/path_eval_log", supabase.base_url);
    if let Err(e) = supabase.client.post(&url).json(&payload).send().await {
        warn!(mint, "path_eval_log POST error: {}", e);
    }
}

/// Command to promote a position to a moonbag.
#[derive(Debug, Clone)]
pub struct MoonbagCommand {
    pub position_id: i64,
    pub mint: String,
    pub token_name: String,
    pub token_symbol: String,
    pub entry_price_usd: f64,
    pub token_amount: f64,
    pub sol_value: f64,
    pub peak_price: f64,
    pub narrative_state: NarrativeState,
    pub is_paper_trade: bool,
    pub narrative_result: Option<NarrativeResult>,
    /// How was this moonbag promoted.
    pub promotion_source: PromotionSource,
    /// Price at the moment of promotion (for latency/drift tracking).
    pub price_at_promotion: f64,
}

#[derive(Debug, Clone, Copy)]
struct MoonbagRuntimeConfig {
    early_trailing_grace_secs: u64,
    partial_3x_pct: u8,
    partial_5x_pct: u8,
    min_hold_secs: u64,
    trailing_confirm_checks: u32,
    trail_2x_5x: f64,
    trail_5x_10x: f64,
    trail_10x_15x: f64,
    trail_15x_20x: f64,
    trail_20x_plus: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoonbagPartialStage {
    ThreeX,
    FiveX,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoonbagExitAction {
    Partial(MoonbagPartialStage),
    Final,
}

impl MoonbagPartialStage {
    fn label(self) -> &'static str {
        match self {
            Self::ThreeX => "moonbag_partial_3x",
            Self::FiveX => "moonbag_partial_5x",
        }
    }

    fn trigger_multiplier(self) -> f64 {
        match self {
            Self::ThreeX => 3.0,
            Self::FiveX => 5.0,
        }
    }

    fn exit_reason(self) -> ExitReason {
        match self {
            Self::ThreeX => ExitReason::TakeProfit1,
            Self::FiveX => ExitReason::TakeProfit2,
        }
    }
}

/// Internal state for a tracked moonbag position.
struct MoonbagPosition {
    position_id: i64,
    mint: String,
    token_name: String,
    token_symbol: String,
    entry_price_usd: f64,
    original_token_amount: f64,
    token_amount: f64,
    sol_value: f64,
    peak_price: f64,
    narrative_state: NarrativeState,
    /// Initial trailing stop % set at promotion time (based on narrative state).
    initial_trailing_pct: f64,
    promoted_at: Instant,
    last_price_check: Instant,
    last_narrative_check: Instant,
    narrative_check_count: u32,
    is_paper_trade: bool,
    /// Max hold seconds for this position (based on narrative state at promotion).
    max_hold_secs: u64,
    /// Profit gate: peak multiplier must reach this before age-based decay activates.
    profit_gate_multiplier: f64,
    /// Early post-promotion grace where 2x-5x runners get wider trailing.
    early_trailing_grace_secs: u64,
    /// Moonbag split-exit percentages, expressed as % of the promoted stack.
    partial_3x_pct: u8,
    partial_5x_pct: u8,
    /// Partial-exit stage flags.
    partial_3x_done: bool,
    partial_5x_done: bool,
    /// Cumulative % of the promoted stack sold by moonbag split exits.
    partial_sold_pct: f64,
    /// Minimum age before soft/full trailing exits can close the moonbag tail.
    min_hold_secs: u64,
    /// Price-only confirmation checks required before a trailing tail exit.
    trailing_confirm_checks: u32,
    trailing_breach_count: u32,
    /// Configurable multiplier-based trailing tiers.
    trail_2x_5x: f64,
    trail_5x_10x: f64,
    trail_10x_15x: f64,
    trail_15x_20x: f64,
    trail_20x_plus: f64,
    /// Whether the profit gate has been reached at any point.
    profit_gate_reached: bool,
    /// Whether a 24h+ extension check has been performed (RUNNER_CONFIRMED only).
    extension_checked: bool,
    /// Consecutive re-checks where narrative score was below the current state threshold.
    /// When this reaches the configured limit, state is downgraded one level.
    consecutive_low_checks: u32,
    /// Supabase row ID for this moonbag in moonbag_positions table.
    moonbag_row_id: Option<i64>,
    /// How this moonbag was promoted.
    promotion_source: PromotionSource,
    /// Price at the moment of promotion.
    price_at_promotion: f64,
    /// Whether this is a fast-runner auto-promote awaiting background check.
    is_fast_runner: bool,
    /// Pending background narrative check result (fast-runner only).
    pending_narrative_rx: Option<tokio::sync::oneshot::Receiver<NarrativeResult>>,
}

impl MoonbagPosition {
    fn from_command(
        cmd: MoonbagCommand,
        initial_trailing_pct: f64,
        max_hold_secs: u64,
        profit_gate: f64,
        runtime_cfg: MoonbagRuntimeConfig,
    ) -> Self {
        let now = Instant::now();
        let peak_mult = if cmd.entry_price_usd > 0.0 {
            cmd.peak_price / cmd.entry_price_usd
        } else {
            1.0
        };
        Self {
            position_id: cmd.position_id,
            mint: cmd.mint,
            token_name: cmd.token_name,
            token_symbol: cmd.token_symbol,
            entry_price_usd: cmd.entry_price_usd,
            original_token_amount: cmd.token_amount,
            token_amount: cmd.token_amount,
            sol_value: cmd.sol_value,
            peak_price: cmd.peak_price,
            narrative_state: cmd.narrative_state,
            initial_trailing_pct,
            promoted_at: now,
            last_price_check: now,
            last_narrative_check: now,
            narrative_check_count: 0,
            is_paper_trade: cmd.is_paper_trade,
            max_hold_secs,
            profit_gate_multiplier: profit_gate,
            early_trailing_grace_secs: runtime_cfg.early_trailing_grace_secs,
            partial_3x_pct: runtime_cfg.partial_3x_pct,
            partial_5x_pct: runtime_cfg.partial_5x_pct,
            partial_3x_done: false,
            partial_5x_done: false,
            partial_sold_pct: 0.0,
            min_hold_secs: runtime_cfg.min_hold_secs,
            trailing_confirm_checks: runtime_cfg.trailing_confirm_checks,
            trailing_breach_count: 0,
            trail_2x_5x: runtime_cfg.trail_2x_5x,
            trail_5x_10x: runtime_cfg.trail_5x_10x,
            trail_10x_15x: runtime_cfg.trail_10x_15x,
            trail_15x_20x: runtime_cfg.trail_15x_20x,
            trail_20x_plus: runtime_cfg.trail_20x_plus,
            profit_gate_reached: peak_mult >= profit_gate,
            extension_checked: false,
            consecutive_low_checks: 0,
            moonbag_row_id: None,
            promotion_source: cmd.promotion_source.clone(),
            price_at_promotion: cmd.price_at_promotion,
            is_fast_runner: cmd.promotion_source == PromotionSource::FastRunner,
            pending_narrative_rx: None,
        }
    }

    /// Decaying poll interval based on age: 30s → 60s → 2m → 5m.
    /// Fast-runner moonbags poll every 5s for the first 60s to close the blind spot.
    fn poll_interval_secs(&self) -> u64 {
        let age_secs = self.promoted_at.elapsed().as_secs();
        if self.is_fast_runner && age_secs <= 60 {
            return 5; // Fast runner: 5s polling for first 60s
        }
        match age_secs {
            0..=300 => 30,     // first 5 min: every 30s
            301..=900 => 60,   // 5-15 min: every 60s
            901..=3600 => 120, // 15-60 min: every 2 min
            _ => 300,          // after 1h: every 5 min
        }
    }

    /// Whether it's time to check price again.
    fn is_price_check_due(&self) -> bool {
        self.last_price_check.elapsed().as_secs() >= self.poll_interval_secs()
    }

    /// Whether it's time for a narrative re-check.
    /// v4: disabled — once a token earns moonbag promotion, the trailing stop +
    /// max hold hours + floor multiplier handle exits mechanically based on price.
    /// Re-checking Twitter/OpenAI to adjust trail width adds cost without signal.
    /// The initial fast-runner background check still runs (sets starting state).
    fn is_narrative_recheck_due(&self) -> bool {
        false
    }

    /// Compute the effective trailing stop % based on current multiplier from entry.
    /// Wider early tiers prevent real moonbags from getting clipped by first-hour
    /// volatility; higher-multiple tiers tighten progressively to protect gains.
    fn effective_trailing_pct(&self, current_multiplier: f64) -> f64 {
        if !self.profit_gate_reached {
            return self.initial_trailing_pct;
        }

        let age_secs = self.promoted_at.elapsed().as_secs();
        let in_early_grace =
            self.early_trailing_grace_secs > 0 && age_secs < self.early_trailing_grace_secs;

        if in_early_grace && current_multiplier >= 2.0 && current_multiplier < 5.0 {
            self.initial_trailing_pct.max(self.trail_2x_5x)
        } else if current_multiplier >= 20.0 {
            self.trail_20x_plus
        } else if current_multiplier >= 15.0 {
            self.trail_15x_20x
        } else if current_multiplier >= 10.0 {
            self.trail_10x_15x
        } else if current_multiplier >= 5.0 {
            self.trail_5x_10x
        } else if current_multiplier >= 2.0 {
            self.trail_2x_5x
        } else {
            self.initial_trailing_pct
        }
    }

    fn next_partial_stage(&self, current_multiplier: f64) -> Option<MoonbagPartialStage> {
        if !self.partial_3x_done
            && self.partial_3x_pct > 0
            && current_multiplier >= MoonbagPartialStage::ThreeX.trigger_multiplier()
        {
            return Some(MoonbagPartialStage::ThreeX);
        }

        if self.partial_3x_done
            && !self.partial_5x_done
            && self.partial_5x_pct > 0
            && current_multiplier >= MoonbagPartialStage::FiveX.trigger_multiplier()
        {
            return Some(MoonbagPartialStage::FiveX);
        }

        None
    }

    /// Convert a stage target expressed as % of the promoted stack into the
    /// `ExitSignal` format: % of the currently remaining moonbag stack.
    fn pct_of_remaining_for_stage(&self, stage: MoonbagPartialStage) -> u8 {
        let target_original_pct = match stage {
            MoonbagPartialStage::ThreeX => self.partial_3x_pct as f64,
            MoonbagPartialStage::FiveX => self.partial_5x_pct as f64,
        };
        let remaining_original_pct = (100.0 - self.partial_sold_pct).max(1.0);
        ((target_original_pct / remaining_original_pct) * 100.0)
            .ceil()
            .clamp(1.0, 100.0) as u8
    }
}

// ── Moonbag tracker ──────────────────────────────────────────

/// Run the moonbag tracker loop. Receives promoted positions via the channel,
/// manages trailing stops, and sends exit signals when triggered.
pub async fn run_moonbag_tracker(
    mut rx: mpsc::Receiver<MoonbagCommand>,
    exit_tx: mpsc::Sender<ExitSignal>,
    mut confirm_rx: broadcast::Receiver<ExitResult>,
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
) {
    info!("🌙 Moonbag tracker started — waiting for promotions");

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    // ── Orphan recovery on startup ──────────────────────────────────────────
    // Moonbag positions live in memory. If the bot restarts while moonbags are
    // open, the in-memory state is lost but the DB rows linger forever with
    // exited_at IS NULL. Mark them closed so analytics + the parent positions
    // row reflect reality. We don't try to resume them — the price cache,
    // trailing high-water-mark, and narrative state are gone.
    {
        let url = format!(
            "{}/moonbag_positions?select=position_id,promoted_at&exited_at=is.null",
            supabase.base_url
        );
        match supabase.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                #[derive(serde::Deserialize)]
                struct OrphanRow {
                    position_id: i64,
                    #[serde(default)]
                    promoted_at: Option<chrono::DateTime<chrono::Utc>>,
                }
                match resp.json::<Vec<OrphanRow>>().await {
                    Ok(orphans) if !orphans.is_empty() => {
                        warn!(
                            count = orphans.len(),
                            "🌙 Found orphaned moonbag positions from prior run — marking closed"
                        );
                        let now = chrono::Utc::now();
                        for row in orphans {
                            let started = row.promoted_at.unwrap_or(now);
                            let hold_secs = (now - started).num_seconds().max(0);

                            let mb_url = format!(
                                "{}/moonbag_positions?position_id=eq.{}",
                                supabase.base_url, row.position_id
                            );
                            let mb_payload = serde_json::json!({
                                "exit_reason": "orphaned_on_restart",
                                "hold_duration_secs": hold_secs,
                                "exited_at": now.to_rfc3339(),
                            });
                            let _ = supabase
                                .client
                                .patch(&mb_url)
                                .json(&mb_payload)
                                .send()
                                .await;

                            let pos_url = format!(
                                "{}/positions?id=eq.{}",
                                supabase.base_url, row.position_id
                            );
                            let pos_payload = serde_json::json!({
                                "moonbag_exit_reason": "orphaned_on_restart",
                                "moonbag_hold_duration_secs": hold_secs,
                                "status": "closed",
                            });
                            let _ = supabase
                                .client
                                .patch(&pos_url)
                                .json(&pos_payload)
                                .send()
                                .await;

                            info!(
                                position_id = row.position_id,
                                hold_secs, "🌙 Orphan moonbag closed"
                            );
                        }
                    }
                    Ok(_) => {
                        debug!("🌙 No orphan moonbag positions found");
                    }
                    Err(e) => {
                        warn!("🌙 Orphan recovery decode failed: {}", e);
                    }
                }
            }
            Ok(resp) => {
                warn!(status = %resp.status(), "🌙 Orphan recovery query failed");
            }
            Err(e) => {
                warn!("🌙 Orphan recovery request error: {}", e);
            }
        }
    }

    let max_concurrent = cfg.strategy.monitoring.moonbag_max_concurrent;
    let max_hold_early_secs = cfg.strategy.monitoring.moonbag_max_hold_early_hours * 3600;
    let max_hold_expanding_secs = cfg.strategy.monitoring.moonbag_max_hold_expanding_hours * 3600;
    let max_hold_confirmed_secs = cfg.strategy.monitoring.moonbag_max_hold_confirmed_hours * 3600;
    let floor_mult = cfg.strategy.monitoring.moonbag_floor_multiplier;
    let trailing_early = cfg.strategy.monitoring.moonbag_trailing_early;
    let trailing_expanding = cfg.strategy.monitoring.moonbag_trailing_expanding;
    let trailing_confirmed = cfg.strategy.monitoring.moonbag_trailing_confirmed;
    let profit_gate = cfg.strategy.monitoring.moonbag_profit_gate_multiplier;
    let runtime_cfg = MoonbagRuntimeConfig {
        early_trailing_grace_secs: cfg.strategy.monitoring.moonbag_early_trailing_grace_secs,
        partial_3x_pct: cfg.strategy.monitoring.moonbag_partial_3x_pct,
        partial_5x_pct: cfg.strategy.monitoring.moonbag_partial_5x_pct,
        min_hold_secs: cfg.strategy.monitoring.moonbag_min_hold_secs,
        trailing_confirm_checks: cfg
            .strategy
            .monitoring
            .moonbag_trailing_confirm_checks
            .max(1),
        trail_2x_5x: cfg.strategy.monitoring.moonbag_trail_2x_5x,
        trail_5x_10x: cfg.strategy.monitoring.moonbag_trail_5x_10x,
        trail_10x_15x: cfg.strategy.monitoring.moonbag_trail_10x_15x,
        trail_15x_20x: cfg.strategy.monitoring.moonbag_trail_15x_20x,
        trail_20x_plus: cfg.strategy.monitoring.moonbag_trail_20x_plus,
    };
    let downgrade_threshold = cfg.strategy.monitoring.moonbag_downgrade_consecutive;

    let mut positions: Vec<MoonbagPosition> = Vec::new();

    let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
    tick.tick().await; // consume immediate first tick

    loop {
        tokio::select! {
            Some(cmd) = rx.recv() => {
                // Never promote dust/empty balances; this causes misleading moonbag lifecycle records.
                if cmd.token_amount <= 1.0 {
                    warn!(
                        mint = %cmd.mint,
                        token_amount = cmd.token_amount,
                        "🌙 Moonbag promotion skipped — token amount too low"
                    );
                    continue;
                }

                if positions.len() >= max_concurrent {
                    warn!(
                        mint = %cmd.mint,
                        concurrent = positions.len(),
                        max = max_concurrent,
                        "🌙 Moonbag at capacity — skipping promotion"
                    );
                    continue;
                }

                let trailing_pct = trailing_for_state(
                    cmd.narrative_state, trailing_early, trailing_expanding, trailing_confirmed,
                );
                let hold_cap = max_hold_for_state(
                    cmd.narrative_state, max_hold_early_secs, max_hold_expanding_secs, max_hold_confirmed_secs,
                );

                info!(
                    position_id = cmd.position_id,
                    mint = %cmd.mint,
                    name = %cmd.token_name,
                    state = %cmd.narrative_state,
                    trailing_pct = format!("{:.0}%", trailing_pct),
                    max_hold_hours = hold_cap / 3600,
                    tokens = cmd.token_amount,
                    source = %cmd.promotion_source,
                    "🌙 Moonbag PROMOTED — tracker takes over"
                );

                // Log to Supabase system_events + INSERT into moonbag_positions
                let supa = Arc::clone(&supabase);
                let mint_log = cmd.mint.clone();
                let state_log = cmd.narrative_state.to_string();
                let nr_json = cmd.narrative_result.as_ref()
                    .and_then(|nr| serde_json::to_value(nr).ok());
                let insert_supa = Arc::clone(&supabase);
                let insert_pos_id = cmd.position_id;
                let insert_mint = cmd.mint.clone();
                let insert_name = cmd.token_name.clone();
                let insert_sym = cmd.token_symbol.clone();
                let insert_state = cmd.narrative_state.to_string();
                let insert_entry = cmd.entry_price_usd;
                let insert_tokens = cmd.token_amount;
                let insert_sol = cmd.sol_value;
                let insert_peak = cmd.peak_price;
                let insert_peak_mult = if cmd.entry_price_usd > 0.0 {
                    cmd.peak_price / cmd.entry_price_usd
                } else { 0.0 };
                let insert_paper = cmd.is_paper_trade;
                let insert_nr = nr_json.clone();
                let insert_source = cmd.promotion_source.to_string();
                let insert_price_at_promo = cmd.price_at_promotion;
                let is_fast_runner = cmd.promotion_source == PromotionSource::FastRunner;
                let insert_version = cfg.strategy.strategy_version.clone().unwrap_or_else(|| "unknown".to_string());

                tokio::spawn(async move {
                    // system_events log
                    let payload = serde_json::json!({
                        "event_type": "moonbag_promoted",
                        "message": format!(
                            "Mint: {} | state: {} | trailing: {:.0}% | tokens: {:.0}",
                            mint_log, state_log, trailing_pct, insert_tokens
                        ),
                    });
                    let url = format!("{}/system_events", supa.base_url);
                    let _ = supa.client.post(&url).json(&payload).send().await;
                });

                // INSERT into moonbag_positions
                tokio::spawn(async move {
                    let insert_mint_log = insert_mint.clone();
                    let mut payload = serde_json::json!({
                        "position_id": insert_pos_id,
                        "mint": insert_mint,
                        "token_name": insert_name,
                        "token_symbol": insert_sym,
                        "narrative_state": insert_state,
                        "entry_price_usd": insert_entry,
                        "token_amount": insert_tokens,
                        "sol_value": insert_sol,
                        "peak_price_usd": insert_peak,
                        "peak_multiplier": insert_peak_mult,
                        "initial_trailing_pct": trailing_pct,
                        "max_hold_secs": hold_cap as i64,
                        "profit_gate_multiplier": profit_gate,
                        "is_paper_trade": insert_paper,
                        "promotion_source": insert_source,
                        "price_at_promotion": insert_price_at_promo,
                        "is_fast_runner": is_fast_runner,
                        "strategy_version": insert_version,
                        "promoted_at": chrono::Utc::now().to_rfc3339(),
                    });
                    if let Some(nr) = insert_nr {
                        payload.as_object_mut().unwrap().insert(
                            "narrative_result".to_string(), nr,
                        );
                    }
                    let url = format!(
                        "{}/moonbag_positions",
                        insert_supa.base_url
                    );
                    match insert_supa.client
                        .post(&url)
                        .json(&payload)
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => {
                            tracing::info!(mint = %insert_mint_log, "moonbag_positions INSERT OK ({})", resp.status());

                            // Mark the parent position as moonbag only after insert succeeds,
                            // so positions/moonbag_positions cannot diverge on failed inserts.
                            let pos_url = format!("{}/positions?id=eq.{}", insert_supa.base_url, insert_pos_id);
                            let pos_payload = serde_json::json!({
                                "exit_reason": "moonbag_promoted",
                                "moonbag_promoted": true,
                                "status": "moonbag",
                            });
                            match insert_supa.client.patch(&pos_url).json(&pos_payload).send().await {
                                Ok(pos_resp) if pos_resp.status().is_success() => {
                                    tracing::info!(mint = %insert_mint_log, "positions moonbag state synced");
                                }
                                Ok(pos_resp) => {
                                    let pos_status = pos_resp.status();
                                    let pos_body = pos_resp.text().await.unwrap_or_default();
                                    tracing::warn!(
                                        mint = %insert_mint_log,
                                        status = %pos_status,
                                        body = %pos_body,
                                        "positions moonbag sync failed"
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(mint = %insert_mint_log, "positions moonbag sync error: {}", e);
                                }
                            }
                        }
                        Ok(resp) => {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            tracing::error!(
                                mint = %insert_mint_log,
                                status = %status,
                                body = %body,
                                "moonbag_positions INSERT FAILED"
                            );
                        }
                        Err(e) => {
                            tracing::error!(mint = %insert_mint_log, "moonbag_positions INSERT error: {}", e);
                        }
                    }
                });

                let is_fr = cmd.promotion_source == PromotionSource::FastRunner;
                positions.push(MoonbagPosition::from_command(
                    cmd,
                    trailing_pct,
                    hold_cap,
                    profit_gate,
                    runtime_cfg,
                ));

                // For fast runners: fire background narrative check immediately
                if is_fr {
                    if let Some(api_key) = &cfg.env.openai_api_key {
                        let pos = positions.last_mut().unwrap();
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        pos.pending_narrative_rx = Some(rx);

                        let client = http_client.clone();
                        let key = api_key.clone();
                        let birdeye = cfg.env.birdeye_api_key.as_deref().unwrap_or("").to_string();
                        let x_bearer = cfg.env.x_api_bearer_token.as_deref().unwrap_or("").to_string();
                        let ctx = NarrativeContext {
                            mint: pos.mint.clone(),
                            name: pos.token_name.clone(),
                            symbol: pos.token_symbol.clone(),
                            current_price_usd: pos.peak_price,
                            entry_price_usd: pos.entry_price_usd,
                            peak_multiplier: if pos.entry_price_usd > 0.0 { pos.peak_price / pos.entry_price_usd } else { 1.0 },
                            hold_seconds: 0, // just promoted
                            buy_count: 0,
                            sell_count: 0,
                            momentum_ratio: 0.0,
                            buy_volume_sol: 0.0,
                            sell_volume_sol: 0.0,
                        };

                        info!(mint = %pos.mint, "🚀 Fast runner — firing background narrative check");
                        tokio::spawn(async move {
                            match narrative::check_narrative(&client, &key, &birdeye, &x_bearer, &ctx).await {
                                Ok(result) => {
                                    let _ = tx.send(result);
                                }
                                Err(e) => {
                                    warn!("Fast runner background narrative check failed: {}", e);
                                    // Don't send anything — receiver will stay None-ish
                                    drop(tx);
                                }
                            }
                        });
                    }
                }
            }
            _ = tick.tick() => {
                let mut exits_to_send: Vec<(usize, ExitSignal, String, MoonbagExitAction)> = Vec::new();

                for (idx, pos) in positions.iter_mut().enumerate() {
                    // ── Poll pending background narrative check (fast-runner) ──
                    if pos.pending_narrative_rx.is_some() {
                        let rx = pos.pending_narrative_rx.as_mut().unwrap();
                        match rx.try_recv() {
                            Ok(result) => {
                                let price_now = fetch_dexscreener_price(&http_client, &pos.mint).await;
                                let price_drift_pct = if pos.price_at_promotion > 0.0 {
                                    ((price_now - pos.price_at_promotion) / pos.price_at_promotion) * 100.0
                                } else { 0.0 };

                                info!(
                                    mint = %pos.mint,
                                    score = result.score,
                                    state = %result.state,
                                    tier = %result.tier,
                                    total_ms = result.total_ms,
                                    x_api_ms = result.x_api_ms,
                                    openai_ms = result.openai_ms,
                                    prefilter_ms = result.prefilter_ms,
                                    price_at_promotion = format!("{:.10}", pos.price_at_promotion),
                                    price_at_result = format!("{:.10}", price_now),
                                    price_drift_pct = format!("{:.1}%", price_drift_pct),
                                    "🚀 Fast runner background check COMPLETE — recording data"
                                );

                                // Log to narrative_checks for analysis
                                let sb = Arc::clone(&supabase);
                                let pid = pos.position_id;
                                let mint_bg = pos.mint.clone();
                                let name_bg = pos.token_name.clone();
                                let sym_bg = pos.token_symbol.clone();
                                let nr_json = serde_json::to_value(&result).ok();
                                let result_score = result.score;
                                let result_state = result.state.to_string();
                                let result_ns = result.narrative_strength.clone();
                                let result_ms = result.market_strength.clone();
                                let result_sources = result.web_sources_found;
                                let result_tier = result.tier.clone();
                                let promo_price = pos.price_at_promotion;
                                let total_ms = result.total_ms;
                                let x_api_ms = result.x_api_ms;
                                let openai_ms = result.openai_ms;
                                let prefilter_ms = result.prefilter_ms;
                                let entry_p = pos.entry_price_usd;
                                let pk_mult = if pos.entry_price_usd > 0.0 { pos.peak_price / pos.entry_price_usd } else { 1.0 };
                                tokio::spawn(async move {
                                    let payload = serde_json::json!({
                                        "position_id": pid,
                                        "mint": mint_bg,
                                        "token_name": name_bg,
                                        "token_symbol": sym_bg,
                                        "check_phase": "fast_runner_background",
                                        "check_index": 0,
                                        "narrative_state": result_state,
                                        "score": result_score as i64,
                                        "narrative_strength": result_ns,
                                        "market_strength": result_ms,
                                        "web_sources_found": result_sources,
                                        "reasons": serde_json::json!([{
                                            "scoring_method": "fast_runner_background",
                                            "tier": result_tier,
                                            "promotion_source": "fast_runner",
                                            "latency_total_ms": total_ms,
                                            "latency_x_api_ms": x_api_ms,
                                            "latency_openai_ms": openai_ms,
                                            "latency_prefilter_ms": prefilter_ms,
                                            "price_at_promotion": promo_price,
                                            "price_at_check_result": price_now,
                                            "price_drift_pct": price_drift_pct,
                                        }]),
                                        "current_price_usd": price_now,
                                        "entry_price_usd": entry_p,
                                        "peak_multiplier": pk_mult,
                                        "hold_seconds": 0_i64,
                                        "momentum_ratio": 0.0,
                                    });
                                    let url = format!("{}/narrative_checks", sb.base_url);
                                    let _ = sb.client.post(&url).json(&payload).send().await;
                                });

                                // Also update moonbag_positions with the narrative result
                                {
                                    let sb2 = Arc::clone(&supabase);
                                    let pid2 = pos.position_id;
                                    let nr_json2 = serde_json::to_value(&result).unwrap_or_default();
                                    let ns2 = result.state.to_string();
                                    tokio::spawn(async move {
                                        let url = format!(
                                            "{}/moonbag_positions?position_id=eq.{}",
                                            sb2.base_url, pid2
                                        );
                                        let payload = serde_json::json!({
                                            "narrative_result": nr_json2,
                                            "narrative_state": ns2,
                                            "fast_runner_check_score": result_score as i64,
                                            "fast_runner_check_latency_ms": total_ms,
                                            "fast_runner_price_at_result": price_now,
                                            "fast_runner_price_drift_pct": price_drift_pct,
                                        });
                                        let _ = sb2.client.patch(&url).json(&payload).send().await;
                                    });
                                }

                                // Update narrative state from the result (for data tracking only)
                                if result.state > pos.narrative_state {
                                    pos.narrative_state = result.state;
                                }

                                pos.pending_narrative_rx = None; // consumed
                            }
                            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                                // Still waiting — do nothing
                            }
                            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                                // Sender dropped (check errored out)
                                warn!(mint = %pos.mint, "Fast runner background check failed (sender dropped)");
                                pos.pending_narrative_rx = None;
                            }
                        }
                    }

                    // Check max hold time (per-state cap)
                    let age_secs = pos.promoted_at.elapsed().as_secs();
                    if age_secs > pos.max_hold_secs {
                        // For RUNNER_CONFIRMED at 24h+: do one extension check
                        if pos.narrative_state == NarrativeState::RunnerConfirmed
                            && !pos.extension_checked
                            && pos.max_hold_secs < max_hold_confirmed_secs
                        {
                            pos.extension_checked = true;
                            // Fire a narrative re-check to see if it still has strength
                            if let Some(api_key) = &cfg.env.openai_api_key {
                                let birdeye_key = cfg.env.birdeye_api_key.as_deref().unwrap_or("");
                                let ctx = NarrativeContext {
                                    mint: pos.mint.clone(),
                                    name: pos.token_name.clone(),
                                    symbol: pos.token_symbol.clone(),
                                    current_price_usd: pos.peak_price, // use peak as proxy
                                    entry_price_usd: pos.entry_price_usd,
                                    peak_multiplier: if pos.entry_price_usd > 0.0 { pos.peak_price / pos.entry_price_usd } else { 1.0 },
                                    hold_seconds: age_secs,
                                    buy_count: 0,
                                    sell_count: 0,
                                    momentum_ratio: 0.0,
                                    buy_volume_sol: 0.0,
                                    sell_volume_sol: 0.0,
                                };
                                let x_bearer = cfg.env.x_api_bearer_token.as_deref().unwrap_or("");
                                match narrative::check_narrative(&http_client, api_key, birdeye_key, x_bearer, &ctx).await {
                                    Ok(result) if result.score >= 76 => {
                                        pos.max_hold_secs = max_hold_confirmed_secs;
                                        info!(
                                            mint = %pos.mint,
                                            score = result.score,
                                            new_max_hours = max_hold_confirmed_secs / 3600,
                                            "🌙 Moonbag EXTENDED — still showing strong narrative"
                                        );
                                        continue;
                                    }
                                    Ok(result) => {
                                        info!(
                                            mint = %pos.mint,
                                            score = result.score,
                                            "🌙 Moonbag extension denied — narrative faded"
                                        );
                                    }
                                    Err(e) => {
                                        warn!(mint = %pos.mint, "Extension narrative check failed: {} — exiting", e);
                                    }
                                }
                            }
                        }

                        info!(
                            mint = %pos.mint,
                            age_hours = age_secs / 3600,
                            max_hold_hours = pos.max_hold_secs / 3600,
                            state = %pos.narrative_state,
                            "🌙 Moonbag max hold expired — exiting"
                        );
                        exits_to_send.push((idx, build_exit_signal(
                            pos,
                            pos.peak_price * 0.5,
                            100,
                            ExitReason::TimeStop,
                            Some("max_hold_expired".to_string()),
                        ), "max_hold_expired".to_string(), MoonbagExitAction::Final));
                        continue;
                    }

                    // Decaying price check
                    if !pos.is_price_check_due() {
                        continue;
                    }
                    pos.last_price_check = Instant::now();

                    // Fetch price via DexScreener
                    let current_price = fetch_dexscreener_price(&http_client, &pos.mint).await;

                    if current_price <= 0.0 {
                        debug!(mint = %pos.mint, "Moonbag price fetch returned 0 — skipping tick");
                        continue;
                    }

                    // Update peak
                    if current_price > pos.peak_price {
                        pos.peak_price = current_price;
                    }

                    let multiplier = if pos.entry_price_usd > 0.0 {
                        current_price / pos.entry_price_usd
                    } else { 1.0 };

                    // Check profit gate (once reached, stays reached)
                    let peak_mult = if pos.entry_price_usd > 0.0 {
                        pos.peak_price / pos.entry_price_usd
                    } else { 1.0 };
                    if !pos.profit_gate_reached && peak_mult >= pos.profit_gate_multiplier {
                        pos.profit_gate_reached = true;
                        info!(
                            mint = %pos.mint,
                            peak_mult = format!("{:.2}x", peak_mult),
                            "🌙 Profit gate reached — age-based trail decay now active"
                        );
                    }

                    // Floor check: never exit below floor_multiplier × entry
                    let floor_price = pos.entry_price_usd * floor_mult;

                    if current_price <= floor_price && pos.entry_price_usd > 0.0 {
                        info!(
                            mint = %pos.mint,
                            current_price,
                            floor_price,
                            floor_mult,
                            "🌙 Moonbag hit floor — exiting to protect profit"
                        );
                        exits_to_send.push((idx, build_exit_signal(
                            pos,
                            current_price,
                            100,
                            ExitReason::TrailingStop,
                            Some("moonbag_floor_hit".to_string()),
                        ), "floor_hit".to_string(), MoonbagExitAction::Final));
                        continue;
                    }

                    // Trailing stop check (multiplier-based tiers)
                    // Fast-runner grace period: skip trailing stop for first 45s to let
                    // the post-spike pullback settle before trailing activates.
                    let in_grace_period = pos.is_fast_runner && age_secs < 45;

                    let effective_trail = pos.effective_trailing_pct(multiplier);
                    let drawdown_from_peak = if pos.peak_price > 0.0 {
                        (pos.peak_price - current_price) / pos.peak_price * 100.0
                    } else { 0.0 };

                    if drawdown_from_peak >= effective_trail && !in_grace_period {
                        if age_secs < pos.min_hold_secs {
                            debug!(
                                mint = %pos.mint,
                                drawdown = format!("{:.1}%", drawdown_from_peak),
                                trailing = format!("{:.0}%", effective_trail),
                                age_min = age_secs / 60,
                                min_hold_min = pos.min_hold_secs / 60,
                                "🌙 Moonbag trailing breach suppressed — minimum tail hold active"
                            );
                        } else {
                            pos.trailing_breach_count = pos.trailing_breach_count.saturating_add(1);
                            if pos.trailing_breach_count >= pos.trailing_confirm_checks {
                                info!(
                                    mint = %pos.mint,
                                    peak = pos.peak_price,
                                    current = current_price,
                                    drawdown = format!("{:.1}%", drawdown_from_peak),
                                    trailing = format!("{:.0}%", effective_trail),
                                    multiplier = format!("{:.2}x", multiplier),
                                    age_min = age_secs / 60,
                                    confirmations = pos.trailing_breach_count,
                                    profit_gate = pos.profit_gate_reached,
                                    "🌙 Moonbag trailing stop confirmed — exiting tail"
                                );
                                exits_to_send.push((idx, build_exit_signal(
                                    pos,
                                    current_price,
                                    100,
                                    ExitReason::TrailingStop,
                                    Some("moonbag_tail_trailing_confirmed".to_string()),
                                ), "trailing_stop".to_string(), MoonbagExitAction::Final));
                                continue;
                            } else {
                                info!(
                                    mint = %pos.mint,
                                    drawdown = format!("{:.1}%", drawdown_from_peak),
                                    trailing = format!("{:.0}%", effective_trail),
                                    confirmations = pos.trailing_breach_count,
                                    required = pos.trailing_confirm_checks,
                                    "🌙 Moonbag trailing breach armed — waiting for confirmation"
                                );
                            }
                        }
                    } else {
                        pos.trailing_breach_count = 0;
                    }

                    if let Some(stage) = pos.next_partial_stage(multiplier) {
                        let pct_to_sell = pos.pct_of_remaining_for_stage(stage);
                        info!(
                            mint = %pos.mint,
                            stage = stage.label(),
                            pct_of_remaining = pct_to_sell,
                            multiplier = format!("{:.2}x", multiplier),
                            remaining_tokens = pos.token_amount,
                            "🌙 Moonbag split exit triggered"
                        );
                        exits_to_send.push((idx, build_exit_signal(
                            pos,
                            current_price,
                            pct_to_sell,
                            stage.exit_reason(),
                            Some(stage.label().to_string()),
                        ), stage.label().to_string(), MoonbagExitAction::Partial(stage)));
                        continue;
                    }

                    debug!(
                        mint = %pos.mint,
                        price = current_price,
                        peak = pos.peak_price,
                        drawdown = format!("{:.1}%", drawdown_from_peak),
                        trailing = format!("{:.0}%", effective_trail),
                        multiplier = format!("{:.2}x", multiplier),
                        state = %pos.narrative_state,
                        age_min = age_secs / 60,
                        profit_gate = pos.profit_gate_reached,
                        grace = in_grace_period,
                        "🌙 Moonbag tick"
                    );

                    // ── Narrative re-check (upgrade state → widen trailing) ──
                    if pos.is_narrative_recheck_due() {
                        if let Some(api_key) = &cfg.env.openai_api_key {
                            let birdeye_key = cfg.env.birdeye_api_key.as_deref().unwrap_or("");
                            let ctx = NarrativeContext {
                                mint: pos.mint.clone(),
                                name: pos.token_name.clone(),
                                symbol: pos.token_symbol.clone(),
                                current_price_usd: current_price,
                                entry_price_usd: pos.entry_price_usd,
                                peak_multiplier: multiplier,
                                hold_seconds: age_secs,
                                buy_count: 0,
                                sell_count: 0,
                                momentum_ratio: 0.0,
                                buy_volume_sol: 0.0,
                                sell_volume_sol: 0.0,
                            };

                            let x_bearer = cfg.env.x_api_bearer_token.as_deref().unwrap_or("");
                            match narrative::check_narrative(&http_client, api_key, birdeye_key, x_bearer, &ctx).await {
                                Ok(result) => {
                                    pos.narrative_check_count += 1;
                                    pos.last_narrative_check = Instant::now();

                                    // Upgrade: state can go UP on any higher score
                                    if result.state > pos.narrative_state {
                                        let old_state = pos.narrative_state;
                                        pos.narrative_state = result.state;
                                        pos.consecutive_low_checks = 0;

                                        // Upgrade initial trailing (ratchet-only — can widen)
                                        let new_trailing = trailing_for_state(
                                            result.state,
                                            trailing_early,
                                            trailing_expanding,
                                            trailing_confirmed,
                                        );
                                        if new_trailing > pos.initial_trailing_pct {
                                            pos.initial_trailing_pct = new_trailing;
                                        }

                                        // Upgrade max hold cap
                                        let new_hold = max_hold_for_state(
                                            result.state,
                                            max_hold_early_secs,
                                            max_hold_expanding_secs,
                                            max_hold_confirmed_secs,
                                        );
                                        if new_hold > pos.max_hold_secs {
                                            pos.max_hold_secs = new_hold;
                                        }

                                        info!(
                                            mint = %pos.mint,
                                            old_state = %old_state,
                                            new_state = %pos.narrative_state,
                                            trailing = format!("{:.0}%", pos.effective_trailing_pct(multiplier)),
                                            max_hold_hours = pos.max_hold_secs / 3600,
                                            score = result.score,
                                            "🌙 Moonbag narrative UPGRADED"
                                        );
                                    } else if result.state == pos.narrative_state {
                                        // Same state — reset consecutive counter
                                        pos.consecutive_low_checks = 0;
                                    } else if downgrade_threshold > 0 {
                                        // Lower state — increment consecutive counter
                                        pos.consecutive_low_checks += 1;

                                        if pos.consecutive_low_checks >= downgrade_threshold
                                            && pos.narrative_state > NarrativeState::NoSignal
                                        {
                                            let old_state = pos.narrative_state;
                                            // Step down one level
                                            pos.narrative_state = match pos.narrative_state {
                                                NarrativeState::RunnerConfirmed => NarrativeState::ExpandingAttention,
                                                NarrativeState::ExpandingAttention => NarrativeState::EarlyAttention,
                                                NarrativeState::EarlyAttention => NarrativeState::NoSignal,
                                                NarrativeState::NoSignal => NarrativeState::NoSignal,
                                            };
                                            pos.consecutive_low_checks = 0;

                                            // Tighten trailing stop to match lower state
                                            let new_trailing = trailing_for_state(
                                                pos.narrative_state,
                                                trailing_early,
                                                trailing_expanding,
                                                trailing_confirmed,
                                            );
                                            pos.initial_trailing_pct = new_trailing;

                                            // Shorten max hold to match lower state
                                            let new_hold = max_hold_for_state(
                                                pos.narrative_state,
                                                max_hold_early_secs,
                                                max_hold_expanding_secs,
                                                max_hold_confirmed_secs,
                                            );
                                            pos.max_hold_secs = new_hold;

                                            info!(
                                                mint = %pos.mint,
                                                old_state = %old_state,
                                                new_state = %pos.narrative_state,
                                                trailing = format!("{:.0}%", pos.effective_trailing_pct(multiplier)),
                                                max_hold_hours = pos.max_hold_secs / 3600,
                                                score = result.score,
                                                consecutive_low = downgrade_threshold,
                                                "🌙 Moonbag narrative DOWNGRADED — attention fading"
                                            );
                                        } else {
                                            debug!(
                                                mint = %pos.mint,
                                                score = result.score,
                                                consecutive_low = pos.consecutive_low_checks,
                                                threshold = downgrade_threshold,
                                                "🌙 Moonbag narrative below state — tracking"
                                            );
                                        }
                                    }

                                    // Update moonbag_positions with latest narrative result
                                    {
                                        let sb = Arc::clone(&supabase);
                                        let pid = pos.position_id;
                                        let nr_json = serde_json::to_value(&result).unwrap_or_default();
                                        let ns = pos.narrative_state.to_string();
                                        let recheck_count = pos.narrative_check_count as i32;
                                        tokio::spawn(async move {
                                            let url = format!(
                                                "{}/moonbag_positions?position_id=eq.{}",
                                                sb.base_url, pid
                                            );
                                            let payload = serde_json::json!({
                                                "narrative_result": nr_json,
                                                "narrative_state": ns,
                                                "narrative_recheck_count": recheck_count,
                                            });
                                            let _ = sb.client.patch(&url).json(&payload).send().await;
                                        });
                                    }
                                }
                                Err(e) => {
                                    warn!(mint = %pos.mint, "Moonbag narrative re-check failed: {}", e);
                                    pos.narrative_check_count += 1;
                                    pos.last_narrative_check = Instant::now();
                                }
                            }
                        }
                    }
                }

                // Process exits (iterate in reverse to preserve indices)
                exits_to_send.sort_by(|a, b| b.0.cmp(&a.0));
                for (idx, signal, reason, action) in exits_to_send {
                    let Some(pos) = positions.get(idx) else { continue; };
                    let mint_for_confirm = pos.mint.clone();
                    let position_id = pos.position_id;
                    let state_log = pos.narrative_state.to_string();
                    let hold_secs = pos.promoted_at.elapsed().as_secs() as i64;
                    let multiplier = if pos.entry_price_usd > 0.0 {
                        signal.current_price / pos.entry_price_usd
                    } else { 0.0 };
                    let peak_multiplier = if pos.entry_price_usd > 0.0 {
                        pos.peak_price / pos.entry_price_usd
                    } else { 0.0 };
                    let final_trail = pos.effective_trailing_pct(multiplier);
                    let token_amount_before = pos.token_amount;
                    let sell_fraction = signal.pct_to_sell as f64 / 100.0;
                    let token_amount_requested = token_amount_before * sell_fraction;
                    let estimated_sol_value = if pos.entry_price_usd > 0.0 {
                        pos.sol_value * (signal.current_price / pos.entry_price_usd) * sell_fraction
                    } else {
                        pos.sol_value * sell_fraction
                    };

                    if exit_tx.send(signal.clone()).await.is_err() {
                        warn!(mint = %mint_for_confirm, "Moonbag → exit channel closed");
                        continue;
                    }

                    let confirmation = wait_for_exit_confirmation(&mut confirm_rx, &mint_for_confirm).await;
                    if !confirmation.success {
                        warn!(
                            mint = %mint_for_confirm,
                            position_id,
                            reason = %reason,
                            permanent = confirmation.permanent,
                            "🌙 Moonbag exit did not confirm — state not advanced"
                        );
                        if confirmation.permanent {
                            log_moonbag_exit_event(
                                Arc::clone(&supabase),
                                position_id,
                                mint_for_confirm.clone(),
                                "permanent_failed".to_string(),
                                reason.clone(),
                                signal.pct_to_sell,
                                token_amount_before,
                                token_amount_requested,
                                token_amount_before,
                                signal.current_price,
                                multiplier,
                                peak_multiplier,
                                estimated_sol_value,
                                false,
                                signal.is_paper_trade,
                                cfg.strategy.strategy_version.clone().unwrap_or_else(|| "unknown".to_string()),
                            );
                            positions.remove(idx);
                        }
                        continue;
                    }

                    match action {
                        MoonbagExitAction::Partial(stage) => {
                            let Some(pos) = positions.get_mut(idx) else { continue; };
                            if pos.mint != mint_for_confirm {
                                warn!(
                                    mint = %mint_for_confirm,
                                    position_id,
                                    "Moonbag partial confirmation index mismatch — skipping state update"
                                );
                                continue;
                            }

                            let token_amount_after = (pos.token_amount - token_amount_requested).max(0.0);
                            let sold_pct_of_stack = if pos.original_token_amount > 0.0 {
                                token_amount_requested / pos.original_token_amount * 100.0
                            } else {
                                signal.pct_to_sell as f64
                            };
                            pos.token_amount = token_amount_after;
                            pos.sol_value *= 1.0 - sell_fraction;
                            pos.partial_sold_pct = (pos.partial_sold_pct + sold_pct_of_stack).min(100.0);
                            pos.trailing_breach_count = 0;
                            match stage {
                                MoonbagPartialStage::ThreeX => pos.partial_3x_done = true,
                                MoonbagPartialStage::FiveX => pos.partial_5x_done = true,
                            }

                            info!(
                                mint = %pos.mint,
                                stage = stage.label(),
                                sold_pct_remaining = signal.pct_to_sell,
                                cumulative_sold_pct = format!("{:.1}%", pos.partial_sold_pct),
                                remaining_tokens = pos.token_amount,
                                "🌙 Moonbag split exit confirmed — tail remains active"
                            );

                            log_moonbag_exit_event(
                                Arc::clone(&supabase),
                                position_id,
                                mint_for_confirm.clone(),
                                "partial_exit".to_string(),
                                stage.label().to_string(),
                                signal.pct_to_sell,
                                token_amount_before,
                                token_amount_requested,
                                token_amount_after,
                                signal.current_price,
                                multiplier,
                                peak_multiplier,
                                estimated_sol_value,
                                true,
                                signal.is_paper_trade,
                                cfg.strategy.strategy_version.clone().unwrap_or_else(|| "unknown".to_string()),
                            );
                            mark_parent_position_moonbag_after_partial(
                                Arc::clone(&supabase),
                                position_id,
                                mint_for_confirm,
                            );
                        }
                        MoonbagExitAction::Final => {
                            log_final_moonbag_exit(
                                Arc::clone(&supabase),
                                position_id,
                                mint_for_confirm.clone(),
                                reason.clone(),
                                state_log,
                                hold_secs,
                                signal.current_price,
                                multiplier,
                                final_trail,
                            );
                            log_moonbag_exit_event(
                                Arc::clone(&supabase),
                                position_id,
                                mint_for_confirm,
                                "tail_exit".to_string(),
                                reason,
                                signal.pct_to_sell,
                                token_amount_before,
                                token_amount_requested,
                                0.0,
                                signal.current_price,
                                multiplier,
                                peak_multiplier,
                                estimated_sol_value,
                                true,
                                signal.is_paper_trade,
                                cfg.strategy.strategy_version.clone().unwrap_or_else(|| "unknown".to_string()),
                            );
                            positions.remove(idx);
                        }
                    }
                }
            }
            else => break,
        }
    }

    info!("🌙 Moonbag tracker shutting down");
}

// ── Helpers ──────────────────────────────────────────────────

/// Get the initial trailing stop % for a given narrative state.
fn trailing_for_state(state: NarrativeState, early: f64, expanding: f64, confirmed: f64) -> f64 {
    match state {
        NarrativeState::NoSignal => early,
        NarrativeState::EarlyAttention => early,
        NarrativeState::ExpandingAttention => expanding,
        NarrativeState::RunnerConfirmed => confirmed,
    }
}

/// Get the max hold duration (seconds) for a given narrative state.
fn max_hold_for_state(
    state: NarrativeState,
    early_secs: u64,
    expanding_secs: u64,
    confirmed_secs: u64,
) -> u64 {
    match state {
        NarrativeState::NoSignal => early_secs,
        NarrativeState::EarlyAttention => early_secs,
        NarrativeState::ExpandingAttention => expanding_secs,
        NarrativeState::RunnerConfirmed => confirmed_secs,
    }
}

/// Fetch current price from DexScreener for a token.
async fn fetch_dexscreener_price(client: &reqwest::Client, mint: &str) -> f64 {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", mint);

    let resp = match client
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        _ => return 0.0,
    };

    let json: serde_json::Value = match resp.json().await {
        Ok(j) => j,
        Err(_) => return 0.0,
    };

    json.get("pairs")
        .and_then(|p| p.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|p| p.get("chainId").and_then(|c| c.as_str()) == Some("solana"))
                .or_else(|| arr.first())
        })
        .and_then(|pair| pair.get("priceUsd"))
        .and_then(|p| p.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0)
}

#[derive(Debug, Clone, Copy)]
struct ExitConfirmation {
    success: bool,
    permanent: bool,
}

async fn wait_for_exit_confirmation(
    confirm_rx: &mut broadcast::Receiver<ExitResult>,
    mint: &str,
) -> ExitConfirmation {
    let wait_timeout = tokio::time::sleep(std::time::Duration::from_secs(120));
    tokio::pin!(wait_timeout);

    loop {
        tokio::select! {
            result = confirm_rx.recv() => {
                match result {
                    Ok(r) if r.mint == mint => {
                        return ExitConfirmation { success: r.success, permanent: r.permanent };
                    }
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => {
                        return ExitConfirmation { success: false, permanent: false };
                    }
                }
            }
            _ = &mut wait_timeout => {
                return ExitConfirmation { success: false, permanent: false };
            }
        }
    }
}

fn log_final_moonbag_exit(
    supabase: Arc<SupabaseClient>,
    position_id: i64,
    mint: String,
    reason: String,
    narrative_state: String,
    hold_secs: i64,
    exit_price: f64,
    exit_multiplier: f64,
    final_trailing_pct: f64,
) {
    tokio::spawn(async move {
        let payload = serde_json::json!({
            "event_type": "moonbag_exit",
            "message": format!(
                "Mint: {} | reason: {} | state: {} | mult: {:.2}x",
                mint, reason, narrative_state, exit_multiplier
            ),
        });
        let url = format!("{}/system_events", supabase.base_url);
        let _ = supabase.client.post(&url).json(&payload).send().await;

        let url = format!(
            "{}/moonbag_positions?position_id=eq.{}",
            supabase.base_url, position_id
        );
        let payload = serde_json::json!({
            "exit_reason": reason.clone(),
            "exit_price_usd": exit_price,
            "exit_multiplier": exit_multiplier,
            "final_trailing_pct": final_trailing_pct,
            "hold_duration_secs": hold_secs,
            "exited_at": chrono::Utc::now().to_rfc3339(),
            "narrative_state": narrative_state.clone(),
        });
        let _ = supabase.client.patch(&url).json(&payload).send().await;

        let pos_url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
        let pos_payload = serde_json::json!({
            "moonbag_exit_reason": reason,
            "moonbag_exit_multiplier": exit_multiplier,
            "moonbag_hold_duration_secs": hold_secs,
        });
        if let Err(e) = supabase
            .client
            .patch(&pos_url)
            .json(&pos_payload)
            .send()
            .await
        {
            tracing::warn!(position_id, "positions moonbag_exit_* sync error: {}", e);
        }
    });
}

fn log_moonbag_exit_event(
    supabase: Arc<SupabaseClient>,
    position_id: i64,
    mint: String,
    event_type: String,
    stage: String,
    pct_requested: u8,
    token_amount_before: f64,
    token_amount_requested: f64,
    token_amount_after_est: f64,
    price_usd: f64,
    multiplier: f64,
    peak_multiplier: f64,
    estimated_sol_value: f64,
    success: bool,
    is_paper_trade: bool,
    strategy_version: String,
) {
    tokio::spawn(async move {
        let payload = serde_json::json!({
            "position_id": position_id,
            "mint": mint,
            "event_type": event_type,
            "stage": stage,
            "pct_requested": pct_requested as i64,
            "token_amount_before": token_amount_before,
            "token_amount_requested": token_amount_requested,
            "token_amount_after_est": token_amount_after_est,
            "price_usd": price_usd,
            "multiplier": multiplier,
            "peak_multiplier": peak_multiplier,
            "estimated_sol_value": estimated_sol_value,
            "success": success,
            "is_paper_trade": is_paper_trade,
            "strategy_version": strategy_version,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let url = format!("{}/moonbag_exit_events", supabase.base_url);
        if let Err(e) = supabase.client.post(&url).json(&payload).send().await {
            tracing::debug!(position_id, "moonbag_exit_events insert error: {}", e);
        }
    });
}

fn mark_parent_position_moonbag_after_partial(
    supabase: Arc<SupabaseClient>,
    position_id: i64,
    mint: String,
) {
    tokio::spawn(async move {
        let url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
        let payload = serde_json::json!({
            "status": "moonbag",
            "moonbag_promoted": true,
        });
        match supabase.client.patch(&url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(mint = %mint, status = %status, body = %body, "positions status restore after moonbag partial failed");
            }
            Err(e) => {
                tracing::warn!(mint = %mint, "positions status restore after moonbag partial error: {}", e);
            }
        }
    });
}

/// Build an ExitSignal from a moonbag position.
fn build_exit_signal(
    pos: &MoonbagPosition,
    current_price: f64,
    pct_to_sell: u8,
    reason: ExitReason,
    sub_reason: Option<String>,
) -> ExitSignal {
    ExitSignal {
        position_id: pos.position_id,
        mint: pos.mint.clone(),
        pct_to_sell,
        reason,
        current_price,
        entry_price_usd: pos.entry_price_usd,
        sol_spent: pos.sol_value,
        token_amount: pos.token_amount,
        is_paper_trade: pos.is_paper_trade,
        sub_reason,
    }
}
