//! Capped OR-combo live canary.
//!
//! Implements the validated rule:
//! `narrative_live_marker OR (phase2_shadow_passed AND post_grad_flow_shadow AND label_flow_shadow)`.
//!
//! The first leg is the existing `narrative_cluster_live_canary` path in the sniper pipeline.
//! This module adds only the delayed post-grad leg by polling completed shadow rows and
//! injecting a synthetic `FilteredToken` into the execution pipeline.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::detection::types::{DetectionSource, GraduatedToken, PipelineTiming};
use crate::filters::types::{FilterResult, FilterSummary, FilteredToken};
use crate::logger::SupabaseClient;
use crate::sniper::{enrichment as sniper_enrichment, filters as sniper_filters};

const OR_COMBO_ENTRY_TIER: &str = "narrative_cluster_live_canary";
const OR_COMBO_RULE: &str = "narrative_live_marker_or_phase2_postgrad_label";
const OR_COMBO_LEG: &str = "phase2_postgrad_label";

const POST_GRAD_SELECT: &str = concat!(
    "mint,symbol,name,creator_wallet,token_created_at_ms,graduated_at,updated_at,",
    "initial_liquidity_sol,baseline_price,price_60s,multiplier_60s,",
    "first_minute_close_multiplier,first_minute_peak_multiplier,",
    "first_minute_drawdown_pct,first_minute_recovery_pct,absorption_status,",
    "bc_score,narrative_score,narrative_sequence_score,normalized_label,cluster_rank,",
    "prior_same_label_mints_6h,prior_same_label_creators_6h,seconds_since_label_seen,",
    "entry_token_age_secs,entry_volume_sol,entry_buy_count,entry_sell_count,",
    "entry_unique_buyers,entry_buy_pressure_pct,entry_buy_sell_ratio,entry_creator_rebuy,",
    "creator_sold_during_bc,creator_buy_count_bc,creator_buy_sol_total_bc,",
    "creator_buy_max_sol_bc,creator_sell_count_bc,creator_sell_sol_total_bc,",
    "creator_net_sol_bc,whale_buy_count,whale_buy_sol_total,whale_buy_max_sol,",
    "whale_net_sol,proven_wallet_buy_count_bc,proven_wallet_buy_sol_total_bc,status"
);

#[derive(Debug, Clone, Deserialize)]
struct PostGradFlowRow {
    mint: String,
    symbol: Option<String>,
    name: Option<String>,
    creator_wallet: Option<String>,
    token_created_at_ms: Option<i64>,
    graduated_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    initial_liquidity_sol: Option<f64>,
    baseline_price: Option<f64>,
    price_60s: Option<f64>,
    multiplier_60s: Option<f64>,
    first_minute_close_multiplier: Option<f64>,
    first_minute_peak_multiplier: Option<f64>,
    first_minute_drawdown_pct: Option<f64>,
    first_minute_recovery_pct: Option<f64>,
    absorption_status: Option<String>,
    bc_score: Option<f64>,
    narrative_score: Option<f64>,
    narrative_sequence_score: Option<f64>,
    normalized_label: Option<String>,
    cluster_rank: Option<i64>,
    prior_same_label_mints_6h: Option<i64>,
    prior_same_label_creators_6h: Option<i64>,
    seconds_since_label_seen: Option<i64>,
    entry_token_age_secs: Option<f64>,
    entry_volume_sol: Option<f64>,
    entry_buy_count: Option<i64>,
    entry_sell_count: Option<i64>,
    entry_unique_buyers: Option<i64>,
    entry_buy_pressure_pct: Option<f64>,
    entry_buy_sell_ratio: Option<f64>,
    entry_creator_rebuy: Option<bool>,
    creator_sold_during_bc: Option<bool>,
    creator_buy_count_bc: Option<i64>,
    creator_buy_sol_total_bc: Option<f64>,
    creator_buy_max_sol_bc: Option<f64>,
    creator_sell_count_bc: Option<i64>,
    creator_sell_sol_total_bc: Option<f64>,
    creator_net_sol_bc: Option<f64>,
    whale_buy_count: Option<i64>,
    whale_buy_sol_total: Option<f64>,
    whale_buy_max_sol: Option<f64>,
    whale_net_sol: Option<f64>,
    proven_wallet_buy_count_bc: Option<i64>,
    proven_wallet_buy_sol_total_bc: Option<f64>,
    status: Option<String>,
}

#[derive(Debug, Clone)]
struct FastTrackSafetySnapshot {
    passed: bool,
    rejection_reason: Option<String>,
    filter_name: Option<String>,
    enrichment_ms: u64,
    sources_completed: Vec<String>,
    sources_timed_out: Vec<String>,
    per_source_ms: serde_json::Value,
    mint_authority_revoked: Option<bool>,
    freeze_authority_revoked: Option<bool>,
    goplus_honeypot: Option<String>,
    goplus_mintable: Option<String>,
    goplus_transfer_pausable: Option<String>,
    goplus_blacklisted: Option<String>,
    goplus_reclaim_ownership: Option<String>,
}

/// Start the delayed post-grad OR-combo watcher.
pub fn start(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    filter_tx: mpsc::Sender<FilteredToken>,
) {
    if !cfg.strategy.narrative_or_live_canary.enabled {
        info!("Narrative OR live canary disabled");
        return;
    }

    let interval_secs = cfg
        .strategy
        .narrative_or_live_canary
        .poll_interval_seconds
        .max(5);
    let startup_grace = cfg
        .strategy
        .narrative_or_live_canary
        .startup_grace_seconds
        .min(120);
    let started_since = Utc::now() - chrono::Duration::seconds(startup_grace as i64);

    info!(
        interval_secs,
        startup_grace_seconds = startup_grace,
        max_daily_trades = cfg.strategy.narrative_or_live_canary.max_daily_trades,
        max_daily_losses = cfg.strategy.narrative_or_live_canary.max_daily_losses,
        "Narrative OR live canary watcher starting"
    );

    let rpc = Arc::new(RpcClient::new_with_timeout(
        cfg.env.solana_rpc_url.clone(),
        Duration::from_secs(5),
    ));

    tokio::spawn(async move {
        let mut seen_mints: HashSet<String> = HashSet::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = poll_once(
                &cfg,
                &supabase,
                rpc.as_ref(),
                &filter_tx,
                started_since,
                &mut seen_mints,
            )
            .await
            {
                warn!("Narrative OR live canary poll failed: {}", e);
            }
        }
    });
}

async fn poll_once(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    rpc: &RpcClient,
    filter_tx: &mpsc::Sender<FilteredToken>,
    since: DateTime<Utc>,
    seen_mints: &mut HashSet<String>,
) -> Result<()> {
    let rows = fetch_completed_post_grad_rows(cfg, supabase, since).await?;
    for row in rows {
        let mint = row.mint.trim().to_string();
        if mint.is_empty() || seen_mints.contains(&mint) {
            continue;
        }

        let Some(filter_price_usd) = row.entry_price_for_filter() else {
            debug!(mint = %mint, "OR combo post-grad leg skipped — missing first-minute entry price");
            seen_mints.insert(mint);
            continue;
        };

        let Some(phase2_row) = fetch_phase2_row(supabase, &mint).await? else {
            debug!(mint = %mint, "OR combo post-grad leg waiting — no narrative row yet");
            continue;
        };
        if !phase2_row
            .get("phase2_shadow_passed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            debug!(mint = %mint, "OR combo post-grad leg skipped — phase2 shadow did not pass");
            seen_mints.insert(mint);
            continue;
        }

        if !has_label_flow_shadow(supabase, &mint).await? {
            debug!(mint = %mint, "OR combo post-grad leg skipped — label_flow_shadow missing");
            seen_mints.insert(mint);
            continue;
        }

        if position_exists_for_mint(supabase, &mint).await? {
            debug!(mint = %mint, "OR combo post-grad leg skipped — position already exists for mint");
            seen_mints.insert(mint);
            continue;
        }

        let mut candidate = build_synthetic_filtered_token(&row, &phase2_row, filter_price_usd)
            .with_context(|| format!("building OR combo token for {}", mint))?;

        let safety = run_fast_track_safety(rpc, &mint).await;
        attach_fast_track_safety_features(&mut candidate, &safety);
        if !safety.passed {
            let reason = safety
                .rejection_reason
                .as_deref()
                .unwrap_or("fast_track_safety_unknown");
            let filter_name = safety.filter_name.as_deref().unwrap_or("fast_track_safety");
            let _candidate_id = log_or_combo_candidate(
                supabase,
                &candidate,
                "rejected",
                Some(reason),
                Some(filter_name),
            )
            .await;
            warn!(
                mint = %mint,
                reason = reason,
                "❌ OR COMBO LIVE CANARY REJECT — Fast-Track safety blocked"
            );
            seen_mints.insert(mint);
            continue;
        }

        candidate.filter_summary = FilterSummary::from_results(vec![
            FilterResult::pass("narrative_or_live_canary"),
            FilterResult::pass("fast_track_safety"),
        ]);
        let candidate_id = log_or_combo_candidate(
            supabase,
            &candidate,
            "narrative_or_combo_live_canary_passed",
            None,
            Some("narrative_or_live_canary"),
        )
        .await;
        candidate.event.candidate_id = candidate_id;

        match filter_tx.send(candidate).await {
            Ok(()) => {
                info!(
                    mint = %mint,
                    close_multiplier = row.first_minute_close_multiplier.unwrap_or_default(),
                    absorption = row.absorption_status.as_deref().unwrap_or("unknown"),
                    "🚦 OR COMBO LIVE CANARY — phase2+postgrad+label leg injected"
                );
                seen_mints.insert(mint);
            }
            Err(e) => {
                warn!(mint = %mint, "OR combo live canary injection failed: {}", e);
                break;
            }
        }
    }
    Ok(())
}

async fn fetch_completed_post_grad_rows(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    since: DateTime<Utc>,
) -> Result<Vec<PostGradFlowRow>> {
    let since = since.to_rfc3339_opts(SecondsFormat::Micros, true);
    let limit = cfg
        .strategy
        .narrative_or_live_canary
        .max_candidates_per_poll
        .clamp(1, 200);
    let url = format!(
        "{}/post_grad_flow_shadow?select={}&status=eq.first_minute_completed&updated_at=gte.{}&order=updated_at.asc&limit={}",
        supabase.base_url, POST_GRAD_SELECT, since, limit
    );
    let resp = supabase.client.get(&url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("post_grad_flow_shadow HTTP {}: {}", status, body);
    }
    resp.json::<Vec<PostGradFlowRow>>()
        .await
        .context("decode post_grad_flow_shadow rows")
}

async fn fetch_phase2_row(
    supabase: &SupabaseClient,
    mint: &str,
) -> Result<Option<serde_json::Value>> {
    let url = format!(
        "{}/narrative_cluster_shadow?select=mint,phase2_shadow_passed,would_trade_live,narrative_score,normalized_label,cluster_rank,phase2_shadow_profile,phase2_shadow_checked_at&mint=eq.{}&limit=1",
        supabase.base_url, mint
    );
    fetch_first_json_row(supabase, &url).await
}

async fn has_label_flow_shadow(supabase: &SupabaseClient, mint: &str) -> Result<bool> {
    let url = format!(
        "{}/bc_paper_trades?select=id&mint=eq.{}&entry_trigger=eq.label_flow_shadow&limit=1",
        supabase.base_url, mint
    );
    Ok(fetch_first_json_row(supabase, &url).await?.is_some())
}

async fn position_exists_for_mint(supabase: &SupabaseClient, mint: &str) -> Result<bool> {
    let url = format!(
        "{}/positions?select=id&mint=eq.{}&limit=1",
        supabase.base_url, mint
    );
    Ok(fetch_first_json_row(supabase, &url).await?.is_some())
}

async fn fetch_first_json_row(
    supabase: &SupabaseClient,
    url: &str,
) -> Result<Option<serde_json::Value>> {
    let resp = supabase.client.get(url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Supabase GET HTTP {}: {}", status, body);
    }
    let rows: Vec<serde_json::Value> = resp.json().await?;
    Ok(rows.into_iter().next())
}

async fn run_fast_track_safety(rpc: &RpcClient, mint: &str) -> FastTrackSafetySnapshot {
    let enrichment = sniper_enrichment::enrich_token_fast(rpc, mint).await;
    let filter_result = sniper_filters::apply_fast_track_filters(&enrichment);
    let mint_data = enrichment.on_chain_mint.as_ref();
    let goplus = enrichment.goplus.as_ref();

    FastTrackSafetySnapshot {
        passed: filter_result.passed,
        rejection_reason: filter_result.rejection_reason,
        filter_name: filter_result.filter_name,
        enrichment_ms: enrichment.enrichment_duration_ms,
        sources_completed: enrichment.sources_completed.clone(),
        sources_timed_out: enrichment.sources_timed_out.clone(),
        per_source_ms: serde_json::json!(enrichment.per_source_ms),
        mint_authority_revoked: mint_data.map(|mint| mint.mint_authority_revoked),
        freeze_authority_revoked: mint_data.map(|mint| mint.freeze_authority_revoked),
        goplus_honeypot: goplus.and_then(|gp| gp.is_honeypot.clone()),
        goplus_mintable: goplus.and_then(|gp| gp.is_mintable.clone()),
        goplus_transfer_pausable: goplus.and_then(|gp| gp.transfer_pausable.clone()),
        goplus_blacklisted: goplus.and_then(|gp| gp.is_blacklisted.clone()),
        goplus_reclaim_ownership: goplus.and_then(|gp| gp.can_take_back_ownership.clone()),
    }
}

fn attach_fast_track_safety_features(token: &mut FilteredToken, safety: &FastTrackSafetySnapshot) {
    let additions = serde_json::json!({
        "fast_track_safety_passed": safety.passed,
        "fast_track_rejection_reason": safety.rejection_reason.as_deref(),
        "fast_track_filter_name": safety.filter_name.as_deref(),
        "fast_track_enrichment_ms": safety.enrichment_ms,
        "fast_track_sources_completed": safety.sources_completed,
        "fast_track_sources_timed_out": safety.sources_timed_out,
        "fast_track_per_source_ms": safety.per_source_ms,
        "mint_authority_revoked": safety.mint_authority_revoked,
        "freeze_authority_revoked": safety.freeze_authority_revoked,
        "goplus_honeypot": safety.goplus_honeypot,
        "goplus_mintable": safety.goplus_mintable,
        "goplus_transfer_pausable": safety.goplus_transfer_pausable,
        "goplus_blacklisted": safety.goplus_blacklisted,
        "goplus_reclaim_ownership": safety.goplus_reclaim_ownership,
    });

    let mut features = token
        .event
        .sniper_features
        .take()
        .unwrap_or_else(|| serde_json::json!({}));
    if let (Some(base), Some(additions)) = (features.as_object_mut(), additions.as_object()) {
        for (key, value) in additions {
            base.insert(key.clone(), value.clone());
        }
    }
    token.event.sniper_features = Some(features);
}

fn build_synthetic_filtered_token(
    row: &PostGradFlowRow,
    phase2_row: &serde_json::Value,
    filter_price_usd: f64,
) -> Result<FilteredToken> {
    let mint = Pubkey::from_str(row.mint.trim()).context("parse mint pubkey")?;
    let creator_wallet = row
        .creator_wallet
        .as_deref()
        .and_then(|wallet| Pubkey::from_str(wallet).ok())
        .unwrap_or_default();
    let now_ms = Utc::now().timestamp_millis();
    let sniper_score = row.narrative_score.or(row.bc_score).unwrap_or(0.0);
    let initial_liquidity_sol = row.initial_liquidity_sol.unwrap_or(0.0);

    let features = serde_json::json!({
        "entry_tier": OR_COMBO_ENTRY_TIER,
        "or_combo_live_canary": true,
        "or_combo_rule": OR_COMBO_RULE,
        "or_combo_leg": OR_COMBO_LEG,
        "narrative_cluster_phase2_shadow_passed": true,
        "phase2_shadow_profile": phase2_row.get("phase2_shadow_profile"),
        "phase2_shadow_checked_at": phase2_row.get("phase2_shadow_checked_at"),
        "label_flow_shadow": true,
        "post_grad_flow_shadow": true,
        "post_grad_status": row.status,
        "post_grad_updated_at": row.updated_at.map(|dt| dt.to_rfc3339()),
        "graduated_at": row.graduated_at.map(|dt| dt.to_rfc3339()),
        "token_created_at_ms": row.token_created_at_ms,
        "initial_liquidity_sol": row.initial_liquidity_sol,
        "filter_price_usd": filter_price_usd,
        "baseline_price": row.baseline_price,
        "price_60s": row.price_60s,
        "multiplier_60s": row.multiplier_60s,
        "first_minute_close_multiplier": row.first_minute_close_multiplier,
        "first_minute_peak_multiplier": row.first_minute_peak_multiplier,
        "first_minute_drawdown_pct": row.first_minute_drawdown_pct,
        "first_minute_recovery_pct": row.first_minute_recovery_pct,
        "absorption_status": row.absorption_status,
        "bc_score": row.bc_score,
        "narrative_score": row.narrative_score,
        "narrative_sequence_score": row.narrative_sequence_score,
        "normalized_label": row.normalized_label,
        "cluster_rank": row.cluster_rank,
        "prior_same_label_mints_6h": row.prior_same_label_mints_6h,
        "prior_same_label_creators_6h": row.prior_same_label_creators_6h,
        "seconds_since_label_seen": row.seconds_since_label_seen,
        "entry_token_age_secs": row.entry_token_age_secs,
        "entry_volume_sol": row.entry_volume_sol,
        "entry_buy_count": row.entry_buy_count,
        "entry_sell_count": row.entry_sell_count,
        "entry_unique_buyers": row.entry_unique_buyers,
        "entry_buy_pressure_pct": row.entry_buy_pressure_pct,
        "entry_buy_sell_ratio": row.entry_buy_sell_ratio,
        "entry_creator_rebuy": row.entry_creator_rebuy,
        "creator_sold_during_bc": row.creator_sold_during_bc,
        "creator_buy_count_bc": row.creator_buy_count_bc,
        "creator_buy_sol_total_bc": row.creator_buy_sol_total_bc,
        "creator_buy_max_sol_bc": row.creator_buy_max_sol_bc,
        "creator_sell_count_bc": row.creator_sell_count_bc,
        "creator_sell_sol_total_bc": row.creator_sell_sol_total_bc,
        "creator_net_sol_bc": row.creator_net_sol_bc,
        "whale_buy_count": row.whale_buy_count,
        "whale_buy_sol_total": row.whale_buy_sol_total,
        "whale_buy_max_sol": row.whale_buy_max_sol,
        "whale_net_sol": row.whale_net_sol,
        "proven_wallet_buy_count_bc": row.proven_wallet_buy_count_bc,
        "proven_wallet_buy_sol_total_bc": row.proven_wallet_buy_sol_total_bc,
    });

    let event = GraduatedToken {
        mint,
        pool_address: None,
        creator_wallet,
        bonding_curve_volume_sol: row.entry_volume_sol.unwrap_or(0.0),
        buy_pressure_pct: row.entry_buy_pressure_pct.unwrap_or(0.0),
        time_to_graduate_seconds: 0.0,
        detected_at: now_ms,
        source: DetectionSource::PumpFun,
        unique_buyer_count: row.entry_unique_buyers.unwrap_or(0).max(0) as usize,
        buy_count: row.entry_buy_count.unwrap_or(0).max(0) as u64,
        sell_count: row.entry_sell_count.unwrap_or(0).max(0) as u64,
        trade_timestamps: vec![],
        name: row.name.clone().unwrap_or_default(),
        symbol: row.symbol.clone().unwrap_or_default(),
        initial_liquidity_sol,
        creator_rebuy: row.entry_creator_rebuy.unwrap_or(false),
        buy_sell_ratio: row.entry_buy_sell_ratio.unwrap_or(0.0),
        narrative_cluster: None,
        candidate_id: None,
        sniper_features: Some(features),
        sniper_score: Some(sniper_score),
        pipeline_timing: PipelineTiming::new(now_ms),
    };

    Ok(FilteredToken {
        event,
        filter_summary: FilterSummary::from_results(vec![FilterResult::pass(
            "narrative_or_live_canary",
        )]),
        market_cap_usd: None,
        liquidity_usd: None,
        rugcheck_score: None,
        filter_price_usd: Some(filter_price_usd),
        pipeline_timing: PipelineTiming::new(now_ms),
    })
}

async fn log_or_combo_candidate(
    supabase: &SupabaseClient,
    token: &FilteredToken,
    action: &str,
    rejection_reason: Option<&str>,
    filter_name: Option<&str>,
) -> Option<i64> {
    let mint = token.event.mint.to_string();
    let features = token
        .event
        .sniper_features
        .clone()
        .unwrap_or_else(|| serde_json::json!({}));
    let payload = serde_json::json!({
        "mint": mint,
        "symbol": token.event.symbol,
        "name": token.event.name,
        "pool_address": token.event.pool_address.map(|p| p.to_string()),
        "creator_wallet": token.event.creator_wallet.to_string(),
        "initial_liquidity_sol": token.event.initial_liquidity_sol,
        "action": action,
        "rejection_reason": rejection_reason,
        "filter_name": filter_name,
        "sniper_score": token.event.sniper_score.unwrap_or(0.0),
        "sniper_features": features,
    });
    let url = format!("{}/sniper_candidates", supabase.base_url);
    match supabase
        .client
        .post(&url)
        .header("Prefer", "return=representation")
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
            rows.first()
                .and_then(|row| row.get("id"))
                .and_then(|id| id.as_i64())
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "Failed to log OR combo sniper candidate: {}", body);
            None
        }
        Err(e) => {
            warn!(mint = %mint, "OR combo sniper candidate log error: {}", e);
            None
        }
    }
}

impl PostGradFlowRow {
    fn entry_price_for_filter(&self) -> Option<f64> {
        if let Some(price) = self.price_60s.filter(|price| *price > 0.0) {
            return Some(price);
        }
        match (self.baseline_price, self.multiplier_60s) {
            (Some(base), Some(mult)) if base > 0.0 && mult > 0.0 => Some(base * mult),
            _ => None,
        }
    }
}
