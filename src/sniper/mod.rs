//! Sniper pipeline orchestrator — enrichment → hard filters → sniper_candidates logging.
//!
//! Sits between detection and the existing filter/execution pipeline.
//! For every graduated token:
//!   1. Run parallel enrichment (2s budget)
//!   2. Build sniper_features JSONB
//!   3. Apply 5 hard filters
//!   4. Log to sniper_candidates (bought/rejected/skipped)
//!   5. Forward passing tokens to filter engine (which sends to execution)

pub mod birdeye;
pub mod enrichment;
pub mod features;
pub mod filters;
pub mod post_trade;
pub mod scoring;
pub mod solana_tracker;
pub mod tracker;
pub mod types;

use std::sync::Arc;

use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::config::{AppConfig, FiltersConfig};
use crate::detection::types::{
    BcScoreCache, BcScoreEntry, GraduatedToken, NarrativeClusterContext,
};
use crate::execution::jupiter::{JupiterClient, SOL_MINT};
use crate::logger::SupabaseClient;

const SNIPER_CHANNEL_CAPACITY: usize = 100;
const CREATOR_REBUY_SHADOW_ENTRY_TIER: &str = "creator_rebuy_shadow_fast_track";
const CREATOR_REBUY_LIVE_TEST_ENTRY_TIER: &str = "creator_rebuy_live_test_fast_track";
const CREATOR_REBUY_MOONBAG_CANARY_ENTRY_TIER: &str = "creator_rebuy_moonbag_canary";
const CREATOR_REBUY_STRUCTURAL_RESCUE_ENTRY_TIER: &str = "creator_rebuy_structural_rescue";
const CREATOR_REBUY_STRICT_2X_SHADOW_ENTRY_TIER: &str = "creator_rebuy_strict_2x_shadow";
const NARRATIVE_CLUSTER_LIVE_CANARY_ENTRY_TIER: &str = "narrative_cluster_live_canary";
const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

fn narrative_cluster_live_canary_rejection_reason(
    token: &GraduatedToken,
    context: &NarrativeClusterContext,
    filters_cfg: &FiltersConfig,
    initial_liquidity_sol: f64,
) -> Option<String> {
    if !filters_cfg.narrative_cluster_live_canary_enabled {
        return Some("narrative_cluster_live_canary_disabled".to_string());
    }

    if filters_cfg.narrative_cluster_live_canary_require_valid_identity {
        if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
            return Some("narrative_cluster_live_missing_token_identity".to_string());
        }
        if token.creator_wallet.to_string() == SYSTEM_PROGRAM_ID {
            return Some("narrative_cluster_live_creator_wallet_system_program".to_string());
        }
    }

    if filters_cfg.reject_creator_rebuy
        && !filters_cfg.narrative_cluster_live_canary_allow_creator_rebuy
        && (token.creator_rebuy || context.creator_rebuy_bypassed)
    {
        return Some("narrative_cluster_live_creator_rebuy_blocked".to_string());
    }

    if context.narrative_score < filters_cfg.narrative_cluster_live_canary_min_score {
        return Some(format!(
            "narrative_score_{:.1}_below_live_min_{:.1}",
            context.narrative_score, filters_cfg.narrative_cluster_live_canary_min_score
        ));
    }

    if context.entry_buy_pressure_pct
        < filters_cfg.narrative_cluster_live_canary_min_buy_pressure_pct
    {
        return Some(format!(
            "entry_buy_pressure_{:.1}_below_live_min_{:.1}",
            context.entry_buy_pressure_pct,
            filters_cfg.narrative_cluster_live_canary_min_buy_pressure_pct
        ));
    }

    if context.entry_buy_sell_ratio < filters_cfg.narrative_cluster_live_canary_min_buy_sell_ratio {
        return Some(format!(
            "entry_buy_sell_ratio_{:.2}_below_live_min_{:.2}",
            context.entry_buy_sell_ratio,
            filters_cfg.narrative_cluster_live_canary_min_buy_sell_ratio
        ));
    }

    if context.entry_sell_count > filters_cfg.narrative_cluster_live_canary_max_sell_count {
        return Some(format!(
            "entry_sell_count_{}_above_live_max_{}",
            context.entry_sell_count, filters_cfg.narrative_cluster_live_canary_max_sell_count
        ));
    }

    if filters_cfg.narrative_cluster_live_canary_require_no_creator_sold
        && context.creator_sold_during_bc
    {
        return Some("creator_sold_during_bc".to_string());
    }

    let max_gap = filters_cfg.narrative_cluster_live_canary_max_label_gap_seconds;
    if max_gap > 0 {
        match context.seconds_since_label_seen {
            Some(gap) if gap <= max_gap as i64 => {}
            Some(gap) => {
                return Some(format!("label_gap_{}_above_live_max_{}", gap, max_gap));
            }
            None => return Some("label_gap_unknown_for_live_canary".to_string()),
        }
    }

    let min_liq = filters_cfg.narrative_cluster_live_canary_min_initial_liquidity_sol;
    if min_liq > 0.0 {
        if initial_liquidity_sol <= 0.0 {
            return Some("initial_liquidity_sol_unknown_for_narrative_live".to_string());
        }
        if initial_liquidity_sol < min_liq {
            return Some(format!(
                "initial_liquidity_sol_{:.1}_below_narrative_live_min_{:.1}",
                initial_liquidity_sol, min_liq
            ));
        }
    }

    let max_liq = filters_cfg.narrative_cluster_live_canary_max_initial_liquidity_sol;
    if max_liq > 0.0 && initial_liquidity_sol > max_liq {
        return Some(format!(
            "initial_liquidity_sol_{:.1}_above_narrative_live_max_{:.1}",
            initial_liquidity_sol, max_liq
        ));
    }

    None
}

fn narrative_cluster_phase2_shadow_rejection_reason(
    token: &GraduatedToken,
    context: &NarrativeClusterContext,
    filters_cfg: &FiltersConfig,
    initial_liquidity_sol: f64,
) -> Option<String> {
    if !filters_cfg.narrative_cluster_phase2_shadow_enabled {
        return Some("narrative_cluster_phase2_shadow_disabled".to_string());
    }

    if filters_cfg.narrative_cluster_phase2_shadow_require_valid_identity {
        if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
            return Some("narrative_cluster_phase2_missing_token_identity".to_string());
        }
        if token.creator_wallet.to_string() == SYSTEM_PROGRAM_ID {
            return Some("narrative_cluster_phase2_creator_wallet_system_program".to_string());
        }
    }

    if context.narrative_score < filters_cfg.narrative_cluster_phase2_shadow_min_score {
        return Some(format!(
            "narrative_score_{:.1}_below_phase2_min_{:.1}",
            context.narrative_score, filters_cfg.narrative_cluster_phase2_shadow_min_score
        ));
    }

    if context.entry_buy_pressure_pct
        < filters_cfg.narrative_cluster_phase2_shadow_min_buy_pressure_pct
    {
        return Some(format!(
            "entry_buy_pressure_{:.1}_below_phase2_min_{:.1}",
            context.entry_buy_pressure_pct,
            filters_cfg.narrative_cluster_phase2_shadow_min_buy_pressure_pct
        ));
    }

    if context.entry_buy_sell_ratio < filters_cfg.narrative_cluster_phase2_shadow_min_buy_sell_ratio
    {
        return Some(format!(
            "entry_buy_sell_ratio_{:.2}_below_phase2_min_{:.2}",
            context.entry_buy_sell_ratio,
            filters_cfg.narrative_cluster_phase2_shadow_min_buy_sell_ratio
        ));
    }

    if context.entry_sell_count > filters_cfg.narrative_cluster_phase2_shadow_max_sell_count {
        return Some(format!(
            "entry_sell_count_{}_above_phase2_max_{}",
            context.entry_sell_count, filters_cfg.narrative_cluster_phase2_shadow_max_sell_count
        ));
    }

    if filters_cfg.narrative_cluster_phase2_shadow_require_no_creator_sold
        && context.creator_sold_during_bc
    {
        return Some("creator_sold_during_bc".to_string());
    }

    let max_gap = filters_cfg.narrative_cluster_phase2_shadow_max_label_gap_seconds;
    if max_gap > 0 {
        match context.seconds_since_label_seen {
            Some(gap) if gap <= max_gap as i64 => {}
            Some(gap) => {
                return Some(format!("label_gap_{}_above_phase2_max_{}", gap, max_gap));
            }
            None => return Some("label_gap_unknown_for_phase2_shadow".to_string()),
        }
    }

    let min_liq = filters_cfg.narrative_cluster_phase2_shadow_min_initial_liquidity_sol;
    if min_liq > 0.0 {
        if initial_liquidity_sol <= 0.0 {
            return Some("initial_liquidity_sol_unknown_for_phase2_shadow".to_string());
        }
        if initial_liquidity_sol < min_liq {
            return Some(format!(
                "initial_liquidity_sol_{:.1}_below_phase2_min_{:.1}",
                initial_liquidity_sol, min_liq
            ));
        }
    }

    let max_liq = filters_cfg.narrative_cluster_phase2_shadow_max_initial_liquidity_sol;
    if max_liq > 0.0 && initial_liquidity_sol > max_liq {
        return Some(format!(
            "initial_liquidity_sol_{:.1}_above_phase2_max_{:.1}",
            initial_liquidity_sol, max_liq
        ));
    }

    None
}

fn creator_rebuy_live_test_rejection_reason(
    token: &GraduatedToken,
    bc_entry: &BcScoreEntry,
    filters_cfg: &FiltersConfig,
    initial_liquidity_sol: f64,
) -> Option<String> {
    if !filters_cfg.creator_rebuy_live_test_enabled {
        return Some("creator_rebuy_live_test_disabled".to_string());
    }

    if filters_cfg.creator_rebuy_live_test_require_valid_identity {
        if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
            return Some("creator_rebuy_live_test_missing_token_identity".to_string());
        }
        if token.creator_wallet.to_string() == SYSTEM_PROGRAM_ID {
            return Some("creator_rebuy_live_test_creator_wallet_system_program".to_string());
        }
    }

    if filters_cfg.creator_rebuy_live_test_min_total_volume_sol > 0.0
        && bc_entry.total_volume_sol < filters_cfg.creator_rebuy_live_test_min_total_volume_sol
    {
        return Some(format!(
            "total_volume_sol_{:.1}_below_live_test_min_{:.1}",
            bc_entry.total_volume_sol, filters_cfg.creator_rebuy_live_test_min_total_volume_sol
        ));
    }

    if filters_cfg.creator_rebuy_live_test_min_whale_buy_sol > 0.0
        && bc_entry.max_single_buy_sol < filters_cfg.creator_rebuy_live_test_min_whale_buy_sol
    {
        return Some(format!(
            "max_single_buy_sol_{:.2}_below_live_test_min_{:.2}",
            bc_entry.max_single_buy_sol, filters_cfg.creator_rebuy_live_test_min_whale_buy_sol
        ));
    }

    if filters_cfg.creator_rebuy_live_test_max_creator_buy_share_pct > 0.0 {
        if let Some(creator_buy_share_pct) = bc_entry.creator_buy_share_pct {
            if creator_buy_share_pct > filters_cfg.creator_rebuy_live_test_max_creator_buy_share_pct
            {
                return Some(format!(
                    "creator_buy_share_pct_{:.1}_above_live_test_max_{:.1}",
                    creator_buy_share_pct,
                    filters_cfg.creator_rebuy_live_test_max_creator_buy_share_pct
                ));
            }
        }
    }

    if filters_cfg.creator_rebuy_live_test_max_bc_progress_pct > 0.0 {
        if bc_entry.bc_progress_pct <= 0.0 {
            return Some("bc_progress_unknown_for_live_test".to_string());
        }
        if bc_entry.bc_progress_pct > filters_cfg.creator_rebuy_live_test_max_bc_progress_pct {
            return Some(format!(
                "bc_progress_{:.1}_above_live_test_max_{:.1}",
                bc_entry.bc_progress_pct, filters_cfg.creator_rebuy_live_test_max_bc_progress_pct
            ));
        }
    }

    let buy_sell_ratio = bc_entry.buy_sell_ratio;
    let unique_buyers = bc_entry.unique_buyers;
    let sell_count = bc_entry.sell_count;
    let buy_pressure_pct = if bc_entry.buy_count + bc_entry.sell_count > 0 {
        bc_entry.buy_count as f64 / (bc_entry.buy_count + bc_entry.sell_count) as f64 * 100.0
    } else {
        token.buy_pressure_pct
    };

    let min_liq = filters_cfg.creator_rebuy_live_test_min_initial_liquidity_sol;
    if min_liq > 0.0 {
        if initial_liquidity_sol <= 0.0 {
            return Some("initial_liquidity_sol_unknown_for_live_test".to_string());
        }
        if initial_liquidity_sol < min_liq {
            return Some(format!(
                "initial_liquidity_sol_{:.1}_below_live_test_min_{:.1}",
                initial_liquidity_sol, min_liq
            ));
        }
    }

    let max_liq = filters_cfg.creator_rebuy_live_test_max_initial_liquidity_sol;
    if max_liq > 0.0 && initial_liquidity_sol > max_liq {
        return Some(format!(
            "initial_liquidity_sol_{:.1}_above_live_test_max_{:.1}",
            initial_liquidity_sol, max_liq
        ));
    }

    let zero_sell_profile = filters_cfg.creator_rebuy_live_test_zero_sell_enabled
        && bc_entry.score >= filters_cfg.creator_rebuy_live_test_zero_sell_min_score
        && sell_count == 0
        && buy_pressure_pct >= filters_cfg.creator_rebuy_live_test_zero_sell_min_buy_pressure_pct
        && buy_sell_ratio >= filters_cfg.creator_rebuy_live_test_zero_sell_min_buy_sell_ratio;

    let strong_flow_profile = bc_entry.score >= filters_cfg.creator_rebuy_live_test_min_score
        && buy_pressure_pct >= filters_cfg.creator_rebuy_live_test_min_buy_pressure_pct
        && buy_sell_ratio >= filters_cfg.creator_rebuy_live_test_min_buy_sell_ratio
        && unique_buyers >= filters_cfg.creator_rebuy_live_test_min_unique_buyers
        && sell_count <= filters_cfg.creator_rebuy_live_test_max_sell_count
        && bc_entry.bc_progress_pct
            >= filters_cfg.creator_rebuy_live_test_strong_flow_min_bc_progress_pct;

    if !(zero_sell_profile || strong_flow_profile) {
        return Some(format!(
            "creator_rebuy_live_profile_miss_score_{:.1}_bsr_{:.2}_pressure_{:.1}_sells_{}_buyers_{}_progress_{:.1}",
            bc_entry.score,
            buy_sell_ratio,
            buy_pressure_pct,
            sell_count,
            unique_buyers,
            bc_entry.bc_progress_pct
        ));
    }

    None
}

fn creator_rebuy_moonbag_canary_rejection_reason(
    token: &GraduatedToken,
    bc_entry: &BcScoreEntry,
    filters_cfg: &FiltersConfig,
    initial_liquidity_sol: f64,
) -> Option<String> {
    if !filters_cfg.creator_rebuy_moonbag_canary_enabled {
        return Some("creator_rebuy_moonbag_canary_disabled".to_string());
    }

    if filters_cfg.creator_rebuy_live_test_require_valid_identity {
        if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
            return Some("creator_rebuy_moonbag_missing_token_identity".to_string());
        }
        if token.creator_wallet.to_string() == SYSTEM_PROGRAM_ID {
            return Some("creator_rebuy_moonbag_creator_wallet_system_program".to_string());
        }
    }

    if filters_cfg.creator_rebuy_moonbag_canary_min_total_volume_sol > 0.0
        && bc_entry.total_volume_sol < filters_cfg.creator_rebuy_moonbag_canary_min_total_volume_sol
    {
        return Some(format!(
            "total_volume_sol_{:.1}_below_moonbag_min_{:.1}",
            bc_entry.total_volume_sol,
            filters_cfg.creator_rebuy_moonbag_canary_min_total_volume_sol
        ));
    }

    if filters_cfg.creator_rebuy_moonbag_canary_max_bc_progress_pct > 0.0 {
        if bc_entry.bc_progress_pct <= 0.0 {
            return Some("bc_progress_unknown_for_moonbag_canary".to_string());
        }
        if bc_entry.bc_progress_pct > filters_cfg.creator_rebuy_moonbag_canary_max_bc_progress_pct {
            return Some(format!(
                "bc_progress_{:.1}_above_moonbag_max_{:.1}",
                bc_entry.bc_progress_pct,
                filters_cfg.creator_rebuy_moonbag_canary_max_bc_progress_pct
            ));
        }
    }

    let buy_pressure_pct = if bc_entry.buy_count + bc_entry.sell_count > 0 {
        bc_entry.buy_count as f64 / (bc_entry.buy_count + bc_entry.sell_count) as f64 * 100.0
    } else {
        token.buy_pressure_pct
    };

    if buy_pressure_pct < filters_cfg.creator_rebuy_moonbag_canary_min_buy_pressure_pct {
        return Some(format!(
            "buy_pressure_{:.1}_below_moonbag_min_{:.1}",
            buy_pressure_pct, filters_cfg.creator_rebuy_moonbag_canary_min_buy_pressure_pct
        ));
    }

    if bc_entry.buy_sell_ratio < filters_cfg.creator_rebuy_moonbag_canary_min_buy_sell_ratio {
        return Some(format!(
            "buy_sell_ratio_{:.2}_below_moonbag_min_{:.2}",
            bc_entry.buy_sell_ratio, filters_cfg.creator_rebuy_moonbag_canary_min_buy_sell_ratio
        ));
    }

    if bc_entry.unique_buyers < filters_cfg.creator_rebuy_moonbag_canary_min_unique_buyers {
        return Some(format!(
            "unique_buyers_{}_below_moonbag_min_{}",
            bc_entry.unique_buyers, filters_cfg.creator_rebuy_moonbag_canary_min_unique_buyers
        ));
    }

    if bc_entry.sell_count > filters_cfg.creator_rebuy_moonbag_canary_max_sell_count {
        return Some(format!(
            "sell_count_{}_above_moonbag_max_{}",
            bc_entry.sell_count, filters_cfg.creator_rebuy_moonbag_canary_max_sell_count
        ));
    }

    if bc_entry.creator_sell_count_bc
        > filters_cfg.creator_rebuy_moonbag_canary_max_creator_sell_count
    {
        return Some(format!(
            "creator_sell_count_{}_above_moonbag_max_{}",
            bc_entry.creator_sell_count_bc,
            filters_cfg.creator_rebuy_moonbag_canary_max_creator_sell_count
        ));
    }

    if bc_entry.creator_net_sol_bc < filters_cfg.creator_rebuy_moonbag_canary_min_creator_net_sol {
        return Some(format!(
            "creator_net_sol_{:.2}_below_moonbag_min_{:.2}",
            bc_entry.creator_net_sol_bc,
            filters_cfg.creator_rebuy_moonbag_canary_min_creator_net_sol
        ));
    }

    let min_liq = filters_cfg.creator_rebuy_moonbag_canary_min_initial_liquidity_sol;
    if min_liq > 0.0 {
        if initial_liquidity_sol <= 0.0 {
            return Some("initial_liquidity_sol_unknown_for_moonbag_canary".to_string());
        }
        if initial_liquidity_sol < min_liq {
            return Some(format!(
                "initial_liquidity_sol_{:.1}_below_moonbag_min_{:.1}",
                initial_liquidity_sol, min_liq
            ));
        }
    }

    let max_liq = filters_cfg.creator_rebuy_moonbag_canary_max_initial_liquidity_sol;
    if max_liq > 0.0 && initial_liquidity_sol > max_liq {
        return Some(format!(
            "initial_liquidity_sol_{:.1}_above_moonbag_max_{:.1}",
            initial_liquidity_sol, max_liq
        ));
    }

    let has_support = bc_entry.whale_buy
        || bc_entry.buy_sell_ratio
            >= filters_cfg.creator_rebuy_moonbag_canary_support_min_buy_sell_ratio
        || bc_entry.creator_net_sol_bc
            >= filters_cfg.creator_rebuy_moonbag_canary_support_min_creator_net_sol;
    if !has_support {
        return Some("creator_rebuy_moonbag_missing_whale_bsr_or_creator_support".to_string());
    }

    None
}

fn creator_rebuy_structural_rescue_rejection_reason(
    token: &GraduatedToken,
    bc_entry: &BcScoreEntry,
    filters_cfg: &FiltersConfig,
    initial_liquidity_sol: f64,
) -> Option<String> {
    if !filters_cfg.creator_rebuy_structural_rescue_enabled {
        return Some("creator_rebuy_structural_rescue_disabled".to_string());
    }

    if filters_cfg.creator_rebuy_live_test_require_valid_identity {
        if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
            return Some("creator_rebuy_structural_missing_token_identity".to_string());
        }
        if token.creator_wallet.to_string() == SYSTEM_PROGRAM_ID {
            return Some("creator_rebuy_structural_creator_wallet_system_program".to_string());
        }
    }

    if bc_entry.unique_buyers < filters_cfg.creator_rebuy_structural_rescue_min_unique_buyers {
        return Some(format!(
            "unique_buyers_{}_below_structural_min_{}",
            bc_entry.unique_buyers, filters_cfg.creator_rebuy_structural_rescue_min_unique_buyers
        ));
    }

    let max_volume = filters_cfg.creator_rebuy_structural_rescue_max_total_volume_sol;
    if max_volume > 0.0 && bc_entry.total_volume_sol > max_volume {
        return Some(format!(
            "total_volume_sol_{:.1}_above_structural_max_{:.1}",
            bc_entry.total_volume_sol, max_volume
        ));
    }

    let min_liq = filters_cfg.creator_rebuy_structural_rescue_min_initial_liquidity_sol;
    if min_liq > 0.0 {
        if initial_liquidity_sol <= 0.0 {
            return Some("initial_liquidity_sol_unknown_for_structural_rescue".to_string());
        }
        if initial_liquidity_sol < min_liq {
            return Some(format!(
                "initial_liquidity_sol_{:.1}_below_structural_min_{:.1}",
                initial_liquidity_sol, min_liq
            ));
        }
    }

    let max_liq = filters_cfg.creator_rebuy_structural_rescue_max_initial_liquidity_sol;
    if max_liq > 0.0 && initial_liquidity_sol > max_liq {
        return Some(format!(
            "initial_liquidity_sol_{:.1}_above_structural_max_{:.1}",
            initial_liquidity_sol, max_liq
        ));
    }

    let min_age_secs = filters_cfg.creator_rebuy_structural_rescue_min_token_age_secs;
    if min_age_secs > 0.0 && token.time_to_graduate_seconds < min_age_secs {
        return Some(format!(
            "token_age_secs_{:.1}_below_structural_min_{:.1}",
            token.time_to_graduate_seconds, min_age_secs
        ));
    }

    None
}

fn creator_rebuy_strict_2x_shadow_rejection_reason(
    structural_rescue_rejection_reason: Option<&str>,
    bc_entry: &BcScoreEntry,
    filters_cfg: &FiltersConfig,
) -> Option<String> {
    if !filters_cfg.creator_rebuy_strict_2x_shadow_enabled {
        return Some("creator_rebuy_strict_2x_shadow_disabled".to_string());
    }

    if let Some(reason) = structural_rescue_rejection_reason {
        return Some(format!("structural_rescue_not_passed_{}", reason));
    }

    let min_volume = filters_cfg.creator_rebuy_strict_2x_shadow_min_total_volume_sol;
    if min_volume > 0.0 && bc_entry.total_volume_sol < min_volume {
        return Some(format!(
            "total_volume_sol_{:.1}_below_strict_2x_min_{:.1}",
            bc_entry.total_volume_sol, min_volume
        ));
    }

    None
}

/// Start the sniper enrichment pipeline.
///
/// Consumes `GraduatedToken` events from the detection channel,
/// enriches them, applies hard filters, logs to sniper_candidates,
/// and forwards passing tokens to the downstream channel.
pub fn start(
    cfg: Arc<AppConfig>,
    mut detection_rx: mpsc::Receiver<GraduatedToken>,
    supabase: Arc<SupabaseClient>,
    bc_cache: BcScoreCache,
) -> mpsc::Receiver<GraduatedToken> {
    let (tx, rx) = mpsc::channel::<GraduatedToken>(SNIPER_CHANNEL_CAPACITY);

    tokio::spawn(async move {
        info!("Sniper enrichment pipeline started");

        let rpc = RpcClient::new(cfg.env.solana_rpc_url.clone());
        let jupiter = JupiterClient::new(
            cfg.strategy.execution.api_request_timeout_secs,
            cfg.strategy.execution.max_retries,
        );

        while let Some(mut token) = detection_rx.recv().await {
            let mint_str = token.mint.to_string();
            let creator_str = token.creator_wallet.to_string();
            let detected_at = token.detected_at;
            let initial_liquidity_sol = token.initial_liquidity_sol;

            // v5.1 guard: Raydium poller emits `mint = Pubkey::default()` (all-1s
            // = System Program) as a placeholder to be resolved from pool later.
            // If resolution failed upstream, this placeholder leaks into Birdeye /
            // GoPlus / RPC calls and corrupts the pipeline with junk data
            // (fake whale activity, fake risk scores). Drop it here.
            // Log analysis 2026-04-17: System Program triggered full enrichment
            // pipeline with 7-whale-buy false positive before liquidity filter
            // finally caught it.
            const SENTINEL_MINTS: &[&str] = &[
                "11111111111111111111111111111111",            // System Program
                "So11111111111111111111111111111111111111112", // Wrapped SOL
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // SPL Token Program
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", // Token-2022
            ];
            if SENTINEL_MINTS.contains(&mint_str.as_str()) {
                warn!(
                    mint = %mint_str,
                    "⛔ Dropping sentinel/placeholder mint — upstream pool→mint resolution failed"
                );
                continue;
            }

            info!(
                mint = %mint_str,
                liquidity_sol = initial_liquidity_sol,
                "🔬 Sniper enrichment starting"
            );

            let bc_entry = if cfg.strategy.filters.bc_fast_track_enabled
                && token.source == crate::detection::types::DetectionSource::PumpFun
            {
                let map = bc_cache.lock().await;
                map.get(&mint_str).cloned()
            } else {
                None
            };

            // ── Pre-enrichment: bonding curve pattern gate (free — no API calls) ──
            // Only applies to PumpFun-detected tokens that have BC data.
            if token.source == crate::detection::types::DetectionSource::PumpFun {
                let bc_score = bc_entry.as_ref().map(|entry| entry.score);

                let mut bc_pattern_reason =
                    filters::check_bc_pattern(&token, &cfg.strategy.filters, bc_score).or_else(
                        || {
                            if cfg.strategy.filters.reject_creator_rebuy
                                && bc_entry
                                    .as_ref()
                                    .map(|entry| entry.creator_rebuy)
                                    .unwrap_or(false)
                            {
                                Some("creator_rebuy_detected".to_string())
                            } else {
                                None
                            }
                        },
                    );

                if let Some(reason) = bc_pattern_reason.clone() {
                    if let Some(narrative_context) = token.narrative_cluster.clone() {
                        let phase2_passed = narrative_cluster_phase2_shadow_rejection_reason(
                            &token,
                            &narrative_context,
                            &cfg.strategy.filters,
                            initial_liquidity_sol,
                        )
                        .is_none();
                        let live_canary_can_bypass_creator_rebuy = reason
                            == "creator_rebuy_detected"
                            && narrative_cluster_live_canary_rejection_reason(
                                &token,
                                &narrative_context,
                                &cfg.strategy.filters,
                                initial_liquidity_sol,
                            )
                            .is_none();

                        if phase2_passed {
                            info!(
                                mint = %mint_str,
                                label = %narrative_context.normalized_label,
                                narrative_score = format!("{:.1}", narrative_context.narrative_score),
                                bc_rejection = %reason,
                                "🧪 NARRATIVE-CLUSTER PHASE2 SHADOW PASS — marking wider profile, no live execution"
                            );
                            let phase2_supabase = Arc::clone(&supabase);
                            let phase2_mint = mint_str.clone();
                            tokio::spawn(async move {
                                mark_narrative_cluster_phase2_shadow(
                                    &phase2_supabase,
                                    &phase2_mint,
                                )
                                .await;
                            });

                            if !live_canary_can_bypass_creator_rebuy {
                                log_narrative_cluster_phase2_shadow_candidate(
                                    &supabase,
                                    &token,
                                    &mint_str,
                                    &creator_str,
                                    initial_liquidity_sol,
                                    &narrative_context,
                                    Some("shadow_only_pre_enrichment_block"),
                                    Some(&reason),
                                    &cfg.strategy.filters,
                                )
                                .await;
                            }
                        }

                        if live_canary_can_bypass_creator_rebuy {
                            info!(
                                mint = %mint_str,
                                label = %narrative_context.normalized_label,
                                narrative_score = format!("{:.1}", narrative_context.narrative_score),
                                "🚦 NARRATIVE-CLUSTER LIVE CANARY — allowing creator-rebuy only inside strict narrative profile"
                            );
                            bc_pattern_reason = None;
                        }
                    }
                }

                if let Some(reason) = bc_pattern_reason {
                    let creator_rebuy_shadow_qualifies = reason == "creator_rebuy_detected"
                        && cfg.strategy.filters.creator_rebuy_shadow_enabled
                        && bc_entry
                            .as_ref()
                            .map(|entry| {
                                entry.score >= cfg.strategy.filters.creator_rebuy_shadow_min_score
                            })
                            .unwrap_or(false);
                    let creator_rebuy_live_test_score_qualifies = reason
                        == "creator_rebuy_detected"
                        && cfg.strategy.filters.creator_rebuy_live_test_enabled
                        && bc_entry
                            .as_ref()
                            .map(|entry| {
                                entry.score
                                    >= cfg.strategy.filters.creator_rebuy_live_test_min_score
                            })
                            .unwrap_or(false);
                    let creator_rebuy_live_test_profile_rejection_reason =
                        if creator_rebuy_live_test_score_qualifies {
                            bc_entry
                                .as_ref()
                                .map(|entry| {
                                    creator_rebuy_live_test_rejection_reason(
                                        &token,
                                        entry,
                                        &cfg.strategy.filters,
                                        initial_liquidity_sol,
                                    )
                                })
                                .unwrap_or_else(|| {
                                    Some("creator_rebuy_live_test_missing_bc_entry".to_string())
                                })
                        } else {
                            Some("creator_rebuy_live_test_score_below_min".to_string())
                        };
                    let creator_rebuy_live_test_profile_qualifies =
                        creator_rebuy_live_test_score_qualifies
                            && creator_rebuy_live_test_profile_rejection_reason.is_none();
                    let creator_rebuy_moonbag_canary_profile_rejection_reason = if reason
                        == "creator_rebuy_detected"
                    {
                        bc_entry
                            .as_ref()
                            .map(|entry| {
                                creator_rebuy_moonbag_canary_rejection_reason(
                                    &token,
                                    entry,
                                    &cfg.strategy.filters,
                                    initial_liquidity_sol,
                                )
                            })
                            .unwrap_or_else(|| {
                                Some("creator_rebuy_moonbag_canary_missing_bc_entry".to_string())
                            })
                    } else {
                        Some("creator_rebuy_moonbag_canary_not_creator_rebuy".to_string())
                    };
                    let creator_rebuy_moonbag_canary_profile_qualifies =
                        creator_rebuy_moonbag_canary_profile_rejection_reason.is_none();
                    let creator_rebuy_structural_rescue_rejection_reason = if reason
                        == "creator_rebuy_detected"
                    {
                        bc_entry
                            .as_ref()
                            .map(|entry| {
                                creator_rebuy_structural_rescue_rejection_reason(
                                    &token,
                                    entry,
                                    &cfg.strategy.filters,
                                    initial_liquidity_sol,
                                )
                            })
                            .unwrap_or_else(|| {
                                Some("creator_rebuy_structural_rescue_missing_bc_entry".to_string())
                            })
                    } else {
                        Some("creator_rebuy_structural_rescue_not_creator_rebuy".to_string())
                    };
                    let creator_rebuy_structural_rescue_qualifies =
                        creator_rebuy_structural_rescue_rejection_reason.is_none();
                    let creator_rebuy_strict_2x_shadow_rejection_reason = if reason
                        == "creator_rebuy_detected"
                    {
                        bc_entry
                            .as_ref()
                            .map(|entry| {
                                creator_rebuy_strict_2x_shadow_rejection_reason(
                                    creator_rebuy_structural_rescue_rejection_reason.as_deref(),
                                    entry,
                                    &cfg.strategy.filters,
                                )
                            })
                            .unwrap_or_else(|| {
                                Some("creator_rebuy_strict_2x_shadow_missing_bc_entry".to_string())
                            })
                    } else {
                        Some("creator_rebuy_strict_2x_shadow_not_creator_rebuy".to_string())
                    };
                    let creator_rebuy_strict_2x_shadow_profile_qualifies =
                        creator_rebuy_strict_2x_shadow_rejection_reason.is_none();

                    if creator_rebuy_shadow_qualifies
                        || creator_rebuy_live_test_profile_qualifies
                        || creator_rebuy_moonbag_canary_profile_qualifies
                        || creator_rebuy_structural_rescue_qualifies
                        || creator_rebuy_strict_2x_shadow_profile_qualifies
                    {
                        let bc_entry = bc_entry
                            .clone()
                            .expect("creator-rebuy experiment qualification requires BC entry");
                        warn!(
                            mint = %mint_str,
                            bc_score = format!("{:.1}", bc_entry.score),
                            shadow_threshold = cfg.strategy.filters.creator_rebuy_shadow_min_score,
                            live_threshold = cfg.strategy.filters.creator_rebuy_live_test_min_score,
                            "🧪 CREATOR-REBUY EXPERIMENT — running Fast-Track safety checks"
                        );

                        let shadow_start = std::time::Instant::now();
                        let detection_to_sniper_ms = {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            now_ms - detected_at
                        };
                        token.pipeline_timing.detection_to_sniper_ms = Some(detection_to_sniper_ms);

                        let shadow_enrichment =
                            enrichment::enrich_token_fast(&rpc, &mint_str).await;
                        token.pipeline_timing.enrichment_total_ms =
                            Some(shadow_enrichment.enrichment_duration_ms);
                        token.pipeline_timing.enrichment_per_source =
                            shadow_enrichment.per_source_ms.clone();

                        let shadow_filter = filters::apply_fast_track_filters(&shadow_enrichment);
                        let shadow_elapsed = shadow_start.elapsed();
                        let live_test_rejection_reason = if shadow_filter.passed {
                            creator_rebuy_live_test_profile_rejection_reason.clone()
                        } else {
                            Some(
                                shadow_filter
                                    .rejection_reason
                                    .clone()
                                    .unwrap_or_else(|| "fast_track_safety_unknown".to_string()),
                            )
                        };
                        let live_test_qualifies =
                            shadow_filter.passed && live_test_rejection_reason.is_none();
                        let moonbag_canary_rejection_reason = if shadow_filter.passed {
                            creator_rebuy_moonbag_canary_profile_rejection_reason.clone()
                        } else {
                            Some(
                                shadow_filter
                                    .rejection_reason
                                    .clone()
                                    .unwrap_or_else(|| "fast_track_safety_unknown".to_string()),
                            )
                        };
                        let moonbag_canary_qualifies =
                            shadow_filter.passed && moonbag_canary_rejection_reason.is_none();
                        let structural_rescue_rejection_reason = if shadow_filter.passed {
                            creator_rebuy_structural_rescue_rejection_reason.clone()
                        } else {
                            Some(
                                shadow_filter
                                    .rejection_reason
                                    .clone()
                                    .unwrap_or_else(|| "fast_track_safety_unknown".to_string()),
                            )
                        };
                        let structural_rescue_qualifies =
                            shadow_filter.passed && structural_rescue_rejection_reason.is_none();
                        let strict_2x_shadow_rejection_reason = if shadow_filter.passed {
                            creator_rebuy_strict_2x_shadow_rejection_reason.clone()
                        } else {
                            Some(
                                shadow_filter
                                    .rejection_reason
                                    .clone()
                                    .unwrap_or_else(|| "fast_track_safety_unknown".to_string()),
                            )
                        };
                        let strict_2x_shadow_qualifies =
                            shadow_filter.passed && strict_2x_shadow_rejection_reason.is_none();
                        let live_forwarded = live_test_qualifies
                            || moonbag_canary_qualifies
                            || structural_rescue_qualifies;

                        let shadow_action = if live_test_qualifies {
                            "creator_rebuy_live_test_passed"
                        } else if moonbag_canary_qualifies {
                            "creator_rebuy_moonbag_canary_passed"
                        } else if structural_rescue_qualifies {
                            "creator_rebuy_structural_rescue_passed"
                        } else if creator_rebuy_shadow_qualifies && shadow_filter.passed {
                            "creator_rebuy_shadow_passed"
                        } else if creator_rebuy_shadow_qualifies {
                            "creator_rebuy_shadow_rejected"
                        } else {
                            "rejected"
                        };
                        let shadow_rejection_reason = if live_forwarded {
                            None
                        } else if creator_rebuy_shadow_qualifies && shadow_filter.passed {
                            Some("creator_rebuy_detected_live_block")
                        } else if creator_rebuy_shadow_qualifies {
                            shadow_filter.rejection_reason.as_deref()
                        } else if shadow_filter.passed {
                            live_test_rejection_reason
                                .as_deref()
                                .or(Some("creator_rebuy_detected_live_block"))
                        } else {
                            shadow_filter.rejection_reason.as_deref()
                        };
                        let shadow_filter_name = if live_test_qualifies {
                            Some("creator_rebuy_live_test")
                        } else if moonbag_canary_qualifies {
                            Some("creator_rebuy_moonbag_canary")
                        } else if structural_rescue_qualifies {
                            Some("creator_rebuy_structural_rescue")
                        } else if creator_rebuy_shadow_qualifies && shadow_filter.passed {
                            Some("creator_rebuy_shadow")
                        } else if !creator_rebuy_shadow_qualifies {
                            Some("creator_rebuy_live_test")
                        } else {
                            shadow_filter.filter_name.as_deref()
                        };
                        let entry_tier = if live_test_qualifies {
                            CREATOR_REBUY_LIVE_TEST_ENTRY_TIER
                        } else if moonbag_canary_qualifies {
                            CREATOR_REBUY_MOONBAG_CANARY_ENTRY_TIER
                        } else if structural_rescue_qualifies {
                            CREATOR_REBUY_STRUCTURAL_RESCUE_ENTRY_TIER
                        } else if creator_rebuy_shadow_qualifies {
                            CREATOR_REBUY_SHADOW_ENTRY_TIER
                        } else {
                            "creator_rebuy_live_test_rejected"
                        };

                        let mut shadow_features = serde_json::json!({
                            "entry_tier": entry_tier,
                            "creator_rebuy_shadow_enabled": cfg.strategy.filters.creator_rebuy_shadow_enabled,
                            "shadow_mode": !live_test_qualifies && creator_rebuy_shadow_qualifies,
                            "live_forwarded": live_forwarded,
                            "blocked_live_reason": reason,
                            "shadow_min_score": cfg.strategy.filters.creator_rebuy_shadow_min_score,
                            "creator_rebuy_live_test_enabled": cfg.strategy.filters.creator_rebuy_live_test_enabled,
                            "creator_rebuy_live_test_profile_passed": live_test_qualifies,
                            "creator_rebuy_live_test_rejection_reason": live_test_rejection_reason.as_deref(),
                            "creator_rebuy_moonbag_canary_enabled": cfg.strategy.filters.creator_rebuy_moonbag_canary_enabled,
                            "creator_rebuy_moonbag_canary_profile_passed": moonbag_canary_qualifies,
                            "creator_rebuy_moonbag_canary_rejection_reason": moonbag_canary_rejection_reason.as_deref(),
                            "creator_rebuy_moonbag_canary_min_buy_pressure_pct": cfg.strategy.filters.creator_rebuy_moonbag_canary_min_buy_pressure_pct,
                            "creator_rebuy_moonbag_canary_min_buy_sell_ratio": cfg.strategy.filters.creator_rebuy_moonbag_canary_min_buy_sell_ratio,
                            "creator_rebuy_moonbag_canary_min_unique_buyers": cfg.strategy.filters.creator_rebuy_moonbag_canary_min_unique_buyers,
                            "creator_rebuy_moonbag_canary_max_sell_count": cfg.strategy.filters.creator_rebuy_moonbag_canary_max_sell_count,
                            "creator_rebuy_moonbag_canary_min_total_volume_sol": cfg.strategy.filters.creator_rebuy_moonbag_canary_min_total_volume_sol,
                            "creator_rebuy_moonbag_canary_max_bc_progress_pct": cfg.strategy.filters.creator_rebuy_moonbag_canary_max_bc_progress_pct,
                            "creator_rebuy_moonbag_canary_min_initial_liquidity_sol": cfg.strategy.filters.creator_rebuy_moonbag_canary_min_initial_liquidity_sol,
                            "creator_rebuy_moonbag_canary_max_initial_liquidity_sol": cfg.strategy.filters.creator_rebuy_moonbag_canary_max_initial_liquidity_sol,
                            "creator_rebuy_moonbag_canary_max_creator_sell_count": cfg.strategy.filters.creator_rebuy_moonbag_canary_max_creator_sell_count,
                            "creator_rebuy_moonbag_canary_min_creator_net_sol": cfg.strategy.filters.creator_rebuy_moonbag_canary_min_creator_net_sol,
                            "creator_rebuy_moonbag_canary_support_min_buy_sell_ratio": cfg.strategy.filters.creator_rebuy_moonbag_canary_support_min_buy_sell_ratio,
                            "creator_rebuy_moonbag_canary_support_min_creator_net_sol": cfg.strategy.filters.creator_rebuy_moonbag_canary_support_min_creator_net_sol,
                            "creator_rebuy_moonbag_canary_max_daily_trades": cfg.strategy.filters.creator_rebuy_moonbag_canary_max_daily_trades,
                            "creator_rebuy_structural_rescue_enabled": cfg.strategy.filters.creator_rebuy_structural_rescue_enabled,
                            "creator_rebuy_structural_rescue_passed": structural_rescue_qualifies,
                            "creator_rebuy_structural_rescue_rejection_reason": structural_rescue_rejection_reason.as_deref(),
                            "creator_rebuy_structural_rescue_min_unique_buyers": cfg.strategy.filters.creator_rebuy_structural_rescue_min_unique_buyers,
                            "creator_rebuy_structural_rescue_max_total_volume_sol": cfg.strategy.filters.creator_rebuy_structural_rescue_max_total_volume_sol,
                            "creator_rebuy_structural_rescue_min_initial_liquidity_sol": cfg.strategy.filters.creator_rebuy_structural_rescue_min_initial_liquidity_sol,
                            "creator_rebuy_structural_rescue_max_initial_liquidity_sol": cfg.strategy.filters.creator_rebuy_structural_rescue_max_initial_liquidity_sol,
                            "creator_rebuy_structural_rescue_min_token_age_secs": cfg.strategy.filters.creator_rebuy_structural_rescue_min_token_age_secs,
                            "creator_rebuy_live_test_min_score": cfg.strategy.filters.creator_rebuy_live_test_min_score,
                            "creator_rebuy_live_test_min_buy_pressure_pct": cfg.strategy.filters.creator_rebuy_live_test_min_buy_pressure_pct,
                            "creator_rebuy_live_test_min_buy_sell_ratio": cfg.strategy.filters.creator_rebuy_live_test_min_buy_sell_ratio,
                            "creator_rebuy_live_test_min_unique_buyers": cfg.strategy.filters.creator_rebuy_live_test_min_unique_buyers,
                            "creator_rebuy_live_test_max_sell_count": cfg.strategy.filters.creator_rebuy_live_test_max_sell_count,
                            "creator_rebuy_live_test_min_initial_liquidity_sol": cfg.strategy.filters.creator_rebuy_live_test_min_initial_liquidity_sol,
                            "creator_rebuy_live_test_max_initial_liquidity_sol": cfg.strategy.filters.creator_rebuy_live_test_max_initial_liquidity_sol,
                            "creator_rebuy_live_test_require_valid_identity": cfg.strategy.filters.creator_rebuy_live_test_require_valid_identity,
                            "creator_rebuy_live_test_max_bc_progress_pct": cfg.strategy.filters.creator_rebuy_live_test_max_bc_progress_pct,
                            "creator_rebuy_live_test_min_total_volume_sol": cfg.strategy.filters.creator_rebuy_live_test_min_total_volume_sol,
                            "creator_rebuy_live_test_min_whale_buy_sol": cfg.strategy.filters.creator_rebuy_live_test_min_whale_buy_sol,
                            "creator_rebuy_live_test_max_creator_buy_share_pct": cfg.strategy.filters.creator_rebuy_live_test_max_creator_buy_share_pct,
                            "creator_rebuy_live_test_zero_sell_enabled": cfg.strategy.filters.creator_rebuy_live_test_zero_sell_enabled,
                            "creator_rebuy_live_test_zero_sell_min_score": cfg.strategy.filters.creator_rebuy_live_test_zero_sell_min_score,
                            "creator_rebuy_live_test_zero_sell_min_buy_pressure_pct": cfg.strategy.filters.creator_rebuy_live_test_zero_sell_min_buy_pressure_pct,
                            "creator_rebuy_live_test_zero_sell_min_buy_sell_ratio": cfg.strategy.filters.creator_rebuy_live_test_zero_sell_min_buy_sell_ratio,
                            "creator_rebuy_live_test_strong_flow_min_bc_progress_pct": cfg.strategy.filters.creator_rebuy_live_test_strong_flow_min_bc_progress_pct,
                            "creator_rebuy_live_test_buy_amount_sol": cfg.strategy.execution.creator_rebuy_live_test_buy_amount_sol,
                            "bc_score": bc_entry.score,
                            "bc_unique_buyers": bc_entry.unique_buyers,
                            "bc_buy_sell_ratio": bc_entry.buy_sell_ratio,
                            "bc_creator_rebuy": bc_entry.creator_rebuy,
                            "bc_whale_buy": bc_entry.whale_buy,
                            "bc_max_single_buy_sol": bc_entry.max_single_buy_sol,
                            "bc_progress_pct_at_score": bc_entry.bc_progress_pct,
                            "bc_buy_count": bc_entry.buy_count,
                            "bc_sell_count": bc_entry.sell_count,
                            "bc_total_volume_sol": bc_entry.total_volume_sol,
                            "bc_creator_buy_count": bc_entry.creator_buy_count_bc,
                            "bc_creator_buy_sol_total": bc_entry.creator_buy_sol_total_bc,
                            "bc_creator_buy_max_sol": bc_entry.creator_buy_max_sol_bc,
                            "bc_creator_first_buy_after_secs": bc_entry.creator_first_buy_after_secs,
                            "bc_creator_first_buy_progress_pct": bc_entry.creator_first_buy_progress_pct,
                            "bc_creator_last_buy_after_secs": bc_entry.creator_last_buy_after_secs,
                            "bc_creator_last_buy_progress_pct": bc_entry.creator_last_buy_progress_pct,
                            "bc_creator_sell_count": bc_entry.creator_sell_count_bc,
                            "bc_creator_sell_sol_total": bc_entry.creator_sell_sol_total_bc,
                            "bc_creator_net_sol": bc_entry.creator_net_sol_bc,
                            "bc_creator_buy_share_pct": bc_entry.creator_buy_share_pct,
                            "bc_score_recorded_at": bc_entry.recorded_at,
                            "fast_track_safety_passed": shadow_filter.passed,
                            "fast_track_rejection_reason": shadow_filter.rejection_reason.as_deref(),
                            "fast_track_filter_name": shadow_filter.filter_name.as_deref(),
                            "fast_track_enrichment_ms": shadow_enrichment.enrichment_duration_ms,
                            "mint_authority_revoked": shadow_enrichment.on_chain_mint.as_ref().map(|m| m.mint_authority_revoked),
                            "freeze_authority_revoked": shadow_enrichment.on_chain_mint.as_ref().map(|m| m.freeze_authority_revoked),
                            "goplus_honeypot": shadow_enrichment.goplus.as_ref().and_then(|g| g.is_honeypot.clone()),
                            "initial_liquidity_sol": initial_liquidity_sol,
                        });

                        if let Some(features) = shadow_features.as_object_mut() {
                            features.insert(
                                "creator_rebuy_strict_2x_shadow_enabled".to_string(),
                                serde_json::json!(
                                    cfg.strategy.filters.creator_rebuy_strict_2x_shadow_enabled
                                ),
                            );
                            features.insert(
                                "creator_rebuy_strict_2x_shadow_passed".to_string(),
                                serde_json::json!(strict_2x_shadow_qualifies),
                            );
                            features.insert(
                                "creator_rebuy_strict_2x_shadow_rejection_reason".to_string(),
                                serde_json::json!(strict_2x_shadow_rejection_reason.as_deref()),
                            );
                            features.insert(
                                "creator_rebuy_strict_2x_shadow_min_total_volume_sol".to_string(),
                                serde_json::json!(
                                    cfg.strategy
                                        .filters
                                        .creator_rebuy_strict_2x_shadow_min_total_volume_sol
                                ),
                            );
                            features.insert(
                                "creator_rebuy_strict_2x_shadow_target_multiplier".to_string(),
                                serde_json::json!(
                                    cfg.strategy
                                        .filters
                                        .creator_rebuy_strict_2x_shadow_target_multiplier
                                ),
                            );
                            features.insert(
                                "creator_rebuy_strict_2x_shadow_sell_pct".to_string(),
                                serde_json::json!(100.0),
                            );
                            features.insert(
                                "creator_rebuy_strict_2x_shadow_sim_buy_sol".to_string(),
                                serde_json::json!(
                                    cfg.strategy
                                        .execution
                                        .creator_rebuy_live_test_buy_amount_sol
                                ),
                            );
                        }

                        let candidate_id = log_sniper_candidate(
                            &supabase,
                            &mint_str,
                            &token.name,
                            &token.symbol,
                            token
                                .pool_address
                                .as_ref()
                                .map(|p| p.to_string())
                                .as_deref(),
                            &creator_str,
                            initial_liquidity_sol,
                            shadow_action,
                            shadow_rejection_reason,
                            shadow_filter_name,
                            bc_entry.score,
                            &shadow_features,
                        )
                        .await;

                        if strict_2x_shadow_qualifies {
                            info!(
                                mint = %mint_str,
                                total_volume_sol = format!("{:.1}", bc_entry.total_volume_sol),
                                target_multiplier = format!("{:.2}", cfg.strategy.filters.creator_rebuy_strict_2x_shadow_target_multiplier),
                                "🧪 CREATOR-REBUY STRICT 2X SHADOW PASS — tracking full exit at 100% gain, no live exit change"
                            );

                            let strict_supabase = Arc::clone(&supabase);
                            let strict_cfg = Arc::clone(&cfg);
                            let strict_bc_entry = bc_entry.clone();
                            let strict_mint = mint_str.clone();
                            let strict_name = token.name.clone();
                            let strict_symbol = token.symbol.clone();
                            let strict_pool = token.pool_address.as_ref().map(|p| p.to_string());
                            let strict_creator = creator_str.clone();
                            let strict_liquidity_sol = initial_liquidity_sol;
                            tokio::spawn(async move {
                                log_creator_rebuy_strict_2x_shadow_candidate(
                                    &strict_supabase,
                                    &strict_mint,
                                    &strict_name,
                                    &strict_symbol,
                                    strict_pool.as_deref(),
                                    &strict_creator,
                                    strict_liquidity_sol,
                                    &strict_bc_entry,
                                    &strict_cfg.strategy.filters,
                                    strict_cfg
                                        .strategy
                                        .execution
                                        .creator_rebuy_live_test_buy_amount_sol,
                                )
                                .await;
                            });
                        }

                        if live_forwarded {
                            info!(
                                mint = %mint_str,
                                bc_score = format!("{:.1}", bc_entry.score),
                                buy_pressure = format!("{:.1}", token.buy_pressure_pct),
                                buy_sell_ratio = format!("{:.2}", bc_entry.buy_sell_ratio),
                                unique_buyers = bc_entry.unique_buyers,
                                total_volume_sol = format!("{:.1}", bc_entry.total_volume_sol),
                                token_age_secs = format!("{:.1}", token.time_to_graduate_seconds),
                                entry_tier = entry_tier,
                                elapsed_ms = shadow_elapsed.as_millis() as u64,
                                "🚦 CREATOR-REBUY CANARY PASS — forwarding capped creator-rebuy profile to filter engine"
                            );

                            token.candidate_id = candidate_id;
                            token.sniper_features = Some(shadow_features);
                            token.sniper_score = Some(bc_entry.score);

                            let deferred_supabase = Arc::clone(&supabase);
                            let deferred_cfg = Arc::clone(&cfg);
                            let deferred_mint = mint_str.clone();
                            let deferred_creator = creator_str.clone();
                            let deferred_liq = initial_liquidity_sol;
                            let deferred_detected_at = detected_at;
                            let deferred_candidate_id = candidate_id;
                            tokio::spawn(async move {
                                run_deferred_verification(
                                    deferred_cfg,
                                    deferred_supabase,
                                    deferred_mint,
                                    deferred_creator,
                                    deferred_liq,
                                    deferred_detected_at,
                                    deferred_candidate_id,
                                )
                                .await;
                            });

                            if tx.send(token).await.is_err() {
                                warn!("Sniper → filter channel closed");
                                break;
                            }
                            continue;
                        } else if creator_rebuy_shadow_qualifies && shadow_filter.passed {
                            info!(
                                mint = %mint_str,
                                bc_score = format!("{:.1}", bc_entry.score),
                                live_test_rejection = live_test_rejection_reason.as_deref().unwrap_or("unknown"),
                                elapsed_ms = shadow_elapsed.as_millis() as u64,
                                "🧪 CREATOR-REBUY SHADOW PASS — counterfactual tracked, live execution still blocked"
                            );
                            token.pipeline_timing.outcome =
                                Some("shadow_creator_rebuy_fast_track_passed".to_string());
                            token.pipeline_timing.rejection_stage =
                                Some("creator_rebuy_shadow".to_string());
                            token.pipeline_timing.rejection_reason =
                                Some("creator_rebuy_detected_live_block".to_string());
                        } else if creator_rebuy_shadow_qualifies {
                            warn!(
                                mint = %mint_str,
                                reason = shadow_filter.rejection_reason.as_deref().unwrap_or("unknown"),
                                bc_score = format!("{:.1}", bc_entry.score),
                                "🧪 CREATOR-REBUY SHADOW REJECT — Fast-Track safety filter blocked"
                            );
                            token.pipeline_timing.outcome =
                                Some("shadow_creator_rebuy_fast_track_rejected".to_string());
                            token.pipeline_timing.rejection_stage =
                                Some("creator_rebuy_shadow_fast_track_filter".to_string());
                            token.pipeline_timing.rejection_reason = shadow_filter
                                .rejection_reason
                                .clone()
                                .or_else(|| Some("fast_track_safety_unknown".to_string()));
                        } else {
                            warn!(
                                mint = %mint_str,
                                live_test_rejection = live_test_rejection_reason.as_deref().unwrap_or("unknown"),
                                bc_score = format!("{:.1}", bc_entry.score),
                                "⛔ CREATOR-REBUY LIVE TEST REJECT — standalone shadow disabled"
                            );
                            token.pipeline_timing.outcome =
                                Some("rejected_creator_rebuy_live_test".to_string());
                            token.pipeline_timing.rejection_stage =
                                Some("creator_rebuy_live_test".to_string());
                            token.pipeline_timing.rejection_reason = live_test_rejection_reason
                                .clone()
                                .or_else(|| shadow_filter.rejection_reason.clone())
                                .or_else(|| Some("creator_rebuy_detected".to_string()));
                        }

                        if !live_forwarded {
                            let timing_payload = token.pipeline_timing.to_json(&mint_str);
                            let supabase_bg = Arc::clone(&supabase);
                            tokio::spawn(async move {
                                log_pipeline_latency(&supabase_bg, &timing_payload).await;
                            });

                            if creator_rebuy_shadow_qualifies {
                                if let Some(cid) = candidate_id {
                                    tracker::spawn_rejected_tracker(
                                        Arc::clone(&supabase),
                                        cid,
                                        mint_str.clone(),
                                    );
                                }
                            }
                        }

                        continue;
                    }

                    warn!(
                        mint = %mint_str,
                        reason = %reason,
                        buy_sell_ratio = format!("{:.2}", token.buy_sell_ratio),
                        sell_count = token.sell_count,
                        creator_rebuy = token.creator_rebuy,
                        "❌ BC PATTERN REJECT — skipping enrichment"
                    );

                    // Log rejection to pipeline_latency
                    token.pipeline_timing.outcome = Some("rejected_bc_pattern".to_string());
                    token.pipeline_timing.rejection_stage = Some("bc_pattern".to_string());
                    token.pipeline_timing.rejection_reason = Some(reason.clone());

                    // Log to sniper_candidates as rejected
                    log_sniper_candidate(
                        &supabase,
                        &mint_str,
                        &token.name,
                        &token.symbol,
                        token
                            .pool_address
                            .as_ref()
                            .map(|p| p.to_string())
                            .as_deref(),
                        &creator_str,
                        initial_liquidity_sol,
                        "rejected",
                        Some(&reason),
                        Some("bc_pattern"),
                        0.0,
                        &serde_json::json!({}),
                    )
                    .await;

                    // Log pipeline_latency
                    let supabase_clone = Arc::clone(&supabase);
                    let timing_json = token.pipeline_timing.to_json(&mint_str);
                    tokio::spawn(async move {
                        let url = format!("{}/pipeline_latency", supabase_clone.base_url);
                        let _ = supabase_clone
                            .client
                            .post(&url)
                            .json(&timing_json)
                            .send()
                            .await;
                    });
                    continue;
                }

                if cfg.strategy.filters.allow_fast_track_buy_sell_ratio_bypass
                    && token.buy_sell_ratio > 0.0
                    && token.buy_sell_ratio < cfg.strategy.filters.min_buy_sell_ratio
                    && bc_score
                        .map(|score| score >= cfg.strategy.filters.bc_fast_track_min_score)
                        .unwrap_or(false)
                {
                    info!(
                        mint = %mint_str,
                        buy_sell_ratio = format!("{:.2}", token.buy_sell_ratio),
                        bc_score = format!("{:.1}", bc_score.unwrap_or_default()),
                        threshold = cfg.strategy.filters.bc_fast_track_min_score,
                        "⚠️ BC PATTERN BYPASS — low buy/sell ratio allowed because BC fast-track score qualifies"
                    );
                }
            }

            // ── Narrative-cluster live canary ──
            // Uses the exact shadow arm-time snapshot that produced the May 7-9
            // strict profile. It bypasses the Standard-lane kill switch, but only
            // after Fast-Track safety passes. v18.9 allows creator-rebuy only
            // inside this strict narrative profile; broad creator-rebuy remains blocked.
            if token.source == crate::detection::types::DetectionSource::PumpFun {
                if let Some(narrative_context) = token.narrative_cluster.clone() {
                    let phase2_rejection = narrative_cluster_phase2_shadow_rejection_reason(
                        &token,
                        &narrative_context,
                        &cfg.strategy.filters,
                        initial_liquidity_sol,
                    );

                    if phase2_rejection.is_none() {
                        info!(
                            mint = %mint_str,
                            label = %narrative_context.normalized_label,
                            narrative_score = format!("{:.1}", narrative_context.narrative_score),
                            "🧪 NARRATIVE-CLUSTER PHASE2 SHADOW PASS — marking wider profile, no live execution"
                        );
                        let phase2_supabase = Arc::clone(&supabase);
                        let phase2_mint = mint_str.clone();
                        tokio::spawn(async move {
                            mark_narrative_cluster_phase2_shadow(&phase2_supabase, &phase2_mint)
                                .await;
                        });
                    }

                    let narrative_rejection = narrative_cluster_live_canary_rejection_reason(
                        &token,
                        &narrative_context,
                        &cfg.strategy.filters,
                        initial_liquidity_sol,
                    );

                    if phase2_rejection.is_none() && narrative_rejection.is_some() {
                        log_narrative_cluster_phase2_shadow_candidate(
                            &supabase,
                            &token,
                            &mint_str,
                            &creator_str,
                            initial_liquidity_sol,
                            &narrative_context,
                            narrative_rejection.as_deref(),
                            None,
                            &cfg.strategy.filters,
                        )
                        .await;
                    }

                    if narrative_rejection.is_none() {
                        info!(
                            mint = %mint_str,
                            label = %narrative_context.normalized_label,
                            narrative_score = format!("{:.1}", narrative_context.narrative_score),
                            buy_pressure = format!("{:.1}", narrative_context.entry_buy_pressure_pct),
                            buy_sell_ratio = format!("{:.2}", narrative_context.entry_buy_sell_ratio),
                            initial_liquidity_sol = format!("{:.1}", initial_liquidity_sol),
                            "🚦 NARRATIVE-CLUSTER LIVE CANARY — strict profile matched, running Fast-Track safety"
                        );

                        let canary_start = std::time::Instant::now();
                        let detection_to_sniper_ms = {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            now_ms - detected_at
                        };
                        token.pipeline_timing.detection_to_sniper_ms = Some(detection_to_sniper_ms);

                        let canary_enrichment =
                            enrichment::enrich_token_fast(&rpc, &mint_str).await;
                        token.pipeline_timing.enrichment_total_ms =
                            Some(canary_enrichment.enrichment_duration_ms);
                        token.pipeline_timing.enrichment_per_source =
                            canary_enrichment.per_source_ms.clone();

                        let canary_filter = filters::apply_fast_track_filters(&canary_enrichment);
                        let canary_elapsed = canary_start.elapsed();
                        let bc_entry_for_features = bc_entry.clone();

                        let canary_features = serde_json::json!({
                            "entry_tier": NARRATIVE_CLUSTER_LIVE_CANARY_ENTRY_TIER,
                            "narrative_cluster_live_canary_enabled": cfg.strategy.filters.narrative_cluster_live_canary_enabled,
                            "narrative_cluster_live_forwarded": canary_filter.passed,
                            "narrative_cluster_live_min_score": cfg.strategy.filters.narrative_cluster_live_canary_min_score,
                            "narrative_cluster_live_min_buy_pressure_pct": cfg.strategy.filters.narrative_cluster_live_canary_min_buy_pressure_pct,
                            "narrative_cluster_live_min_buy_sell_ratio": cfg.strategy.filters.narrative_cluster_live_canary_min_buy_sell_ratio,
                            "narrative_cluster_live_max_sell_count": cfg.strategy.filters.narrative_cluster_live_canary_max_sell_count,
                            "narrative_cluster_live_max_label_gap_seconds": cfg.strategy.filters.narrative_cluster_live_canary_max_label_gap_seconds,
                            "narrative_cluster_live_min_initial_liquidity_sol": cfg.strategy.filters.narrative_cluster_live_canary_min_initial_liquidity_sol,
                            "narrative_cluster_live_max_initial_liquidity_sol": cfg.strategy.filters.narrative_cluster_live_canary_max_initial_liquidity_sol,
                            "narrative_cluster_live_require_valid_identity": cfg.strategy.filters.narrative_cluster_live_canary_require_valid_identity,
                            "narrative_cluster_live_require_no_creator_sold": cfg.strategy.filters.narrative_cluster_live_canary_require_no_creator_sold,
                            "narrative_cluster_live_allow_creator_rebuy": cfg.strategy.filters.narrative_cluster_live_canary_allow_creator_rebuy,
                            "narrative_cluster_live_buy_amount_sol": cfg.strategy.execution.narrative_cluster_live_canary_buy_amount_sol,
                            "narrative_cluster_phase2_shadow_enabled": cfg.strategy.filters.narrative_cluster_phase2_shadow_enabled,
                            "narrative_cluster_phase2_shadow_passed": phase2_rejection.is_none(),
                            "narrative_cluster_phase2_shadow_rejection_reason": phase2_rejection.as_deref(),
                            "narrative_cluster_phase2_shadow_min_score": cfg.strategy.filters.narrative_cluster_phase2_shadow_min_score,
                            "narrative_cluster_phase2_shadow_max_label_gap_seconds": cfg.strategy.filters.narrative_cluster_phase2_shadow_max_label_gap_seconds,
                            "narrative_cluster_phase2_shadow_max_initial_liquidity_sol": cfg.strategy.filters.narrative_cluster_phase2_shadow_max_initial_liquidity_sol,
                            "narrative_score": narrative_context.narrative_score,
                            "normalized_label": narrative_context.normalized_label,
                            "cluster_rank": narrative_context.cluster_rank,
                            "prior_same_label_mints_6h": narrative_context.prior_same_label_mints_6h,
                            "prior_same_label_creators_6h": narrative_context.prior_same_label_creators_6h,
                            "seconds_since_label_seen": narrative_context.seconds_since_label_seen,
                            "score_reasons": narrative_context.score_reasons,
                            "score_penalties": narrative_context.score_penalties,
                            "score_breakdown": narrative_context.score_breakdown,
                            "entry_volume_sol": narrative_context.entry_volume_sol,
                            "entry_buy_count": narrative_context.entry_buy_count,
                            "entry_sell_count": narrative_context.entry_sell_count,
                            "entry_unique_buyers": narrative_context.entry_unique_buyers,
                            "entry_buy_sell_ratio": narrative_context.entry_buy_sell_ratio,
                            "entry_buy_pressure_pct": narrative_context.entry_buy_pressure_pct,
                            "creator_rebuy_bypassed": narrative_context.creator_rebuy_bypassed,
                            "creator_sold_during_bc": narrative_context.creator_sold_during_bc,
                            "creator_buy_count_bc": narrative_context.creator_buy_count_bc,
                            "creator_buy_sol_total_bc": narrative_context.creator_buy_sol_total_bc,
                            "creator_buy_max_sol_bc": narrative_context.creator_buy_max_sol_bc,
                            "creator_first_buy_after_secs": narrative_context.creator_first_buy_after_secs,
                            "creator_first_buy_progress_pct": narrative_context.creator_first_buy_progress_pct,
                            "creator_last_buy_after_secs": narrative_context.creator_last_buy_after_secs,
                            "creator_last_buy_progress_pct": narrative_context.creator_last_buy_progress_pct,
                            "creator_sell_count_bc": narrative_context.creator_sell_count_bc,
                            "creator_sell_sol_total_bc": narrative_context.creator_sell_sol_total_bc,
                            "creator_net_sol_bc": narrative_context.creator_net_sol_bc,
                            "creator_buy_share_pct": narrative_context.creator_buy_share_pct,
                            "whale_buy": narrative_context.whale_buy,
                            "whale_buy_max_sol": narrative_context.whale_buy_max_sol,
                            "bc_progress_pct_at_narrative_arm": narrative_context.bc_progress_pct,
                            "bc_score": bc_entry_for_features.as_ref().map(|entry| entry.score),
                            "bc_unique_buyers": bc_entry_for_features.as_ref().map(|entry| entry.unique_buyers),
                            "bc_buy_sell_ratio": bc_entry_for_features.as_ref().map(|entry| entry.buy_sell_ratio),
                            "bc_creator_rebuy": bc_entry_for_features.as_ref().map(|entry| entry.creator_rebuy),
                            "bc_whale_buy": bc_entry_for_features.as_ref().map(|entry| entry.whale_buy),
                            "bc_max_single_buy_sol": bc_entry_for_features.as_ref().map(|entry| entry.max_single_buy_sol),
                            "bc_progress_pct_at_score": bc_entry_for_features.as_ref().map(|entry| entry.bc_progress_pct),
                            "bc_buy_count": bc_entry_for_features.as_ref().map(|entry| entry.buy_count),
                            "bc_sell_count": bc_entry_for_features.as_ref().map(|entry| entry.sell_count),
                            "bc_total_volume_sol": bc_entry_for_features.as_ref().map(|entry| entry.total_volume_sol),
                            "bc_creator_buy_count": bc_entry_for_features.as_ref().map(|entry| entry.creator_buy_count_bc),
                            "bc_creator_buy_sol_total": bc_entry_for_features.as_ref().map(|entry| entry.creator_buy_sol_total_bc),
                            "bc_creator_buy_max_sol": bc_entry_for_features.as_ref().map(|entry| entry.creator_buy_max_sol_bc),
                            "bc_creator_first_buy_after_secs": bc_entry_for_features.as_ref().and_then(|entry| entry.creator_first_buy_after_secs),
                            "bc_creator_first_buy_progress_pct": bc_entry_for_features.as_ref().and_then(|entry| entry.creator_first_buy_progress_pct),
                            "bc_creator_last_buy_after_secs": bc_entry_for_features.as_ref().and_then(|entry| entry.creator_last_buy_after_secs),
                            "bc_creator_last_buy_progress_pct": bc_entry_for_features.as_ref().and_then(|entry| entry.creator_last_buy_progress_pct),
                            "bc_creator_sell_count": bc_entry_for_features.as_ref().map(|entry| entry.creator_sell_count_bc),
                            "bc_creator_sell_sol_total": bc_entry_for_features.as_ref().map(|entry| entry.creator_sell_sol_total_bc),
                            "bc_creator_net_sol": bc_entry_for_features.as_ref().map(|entry| entry.creator_net_sol_bc),
                            "bc_creator_buy_share_pct": bc_entry_for_features.as_ref().and_then(|entry| entry.creator_buy_share_pct),
                            "fast_track_safety_passed": canary_filter.passed,
                            "fast_track_rejection_reason": canary_filter.rejection_reason.as_deref(),
                            "fast_track_filter_name": canary_filter.filter_name.as_deref(),
                            "fast_track_enrichment_ms": canary_enrichment.enrichment_duration_ms,
                            "mint_authority_revoked": canary_enrichment.on_chain_mint.as_ref().map(|m| m.mint_authority_revoked),
                            "freeze_authority_revoked": canary_enrichment.on_chain_mint.as_ref().map(|m| m.freeze_authority_revoked),
                            "goplus_honeypot": canary_enrichment.goplus.as_ref().and_then(|g| g.is_honeypot.clone()),
                            "initial_liquidity_sol": initial_liquidity_sol,
                        });

                        let candidate_id = log_sniper_candidate(
                            &supabase,
                            &mint_str,
                            &token.name,
                            &token.symbol,
                            token
                                .pool_address
                                .as_ref()
                                .map(|p| p.to_string())
                                .as_deref(),
                            &creator_str,
                            initial_liquidity_sol,
                            if canary_filter.passed {
                                "narrative_cluster_live_canary_passed"
                            } else {
                                "rejected"
                            },
                            canary_filter.rejection_reason.as_deref(),
                            if canary_filter.passed {
                                Some("narrative_cluster_live_canary")
                            } else {
                                canary_filter.filter_name.as_deref()
                            },
                            narrative_context.narrative_score,
                            &canary_features,
                        )
                        .await;

                        if canary_filter.passed {
                            info!(
                                mint = %mint_str,
                                narrative_score = format!("{:.1}", narrative_context.narrative_score),
                                elapsed_ms = canary_elapsed.as_millis() as u64,
                                "🚦 NARRATIVE-CLUSTER LIVE CANARY PASS — forwarding to filter engine"
                            );

                            token.candidate_id = candidate_id;
                            token.sniper_features = Some(canary_features);
                            token.sniper_score = Some(narrative_context.narrative_score);

                            let live_supabase = Arc::clone(&supabase);
                            let live_mint = mint_str.clone();
                            tokio::spawn(async move {
                                mark_narrative_cluster_would_trade_live(&live_supabase, &live_mint)
                                    .await;
                            });

                            let deferred_supabase = Arc::clone(&supabase);
                            let deferred_cfg = Arc::clone(&cfg);
                            let deferred_mint = mint_str.clone();
                            let deferred_creator = creator_str.clone();
                            let deferred_liq = initial_liquidity_sol;
                            let deferred_detected_at = detected_at;
                            let deferred_candidate_id = candidate_id;
                            tokio::spawn(async move {
                                run_deferred_verification(
                                    deferred_cfg,
                                    deferred_supabase,
                                    deferred_mint,
                                    deferred_creator,
                                    deferred_liq,
                                    deferred_detected_at,
                                    deferred_candidate_id,
                                )
                                .await;
                            });

                            if tx.send(token).await.is_err() {
                                warn!("Sniper → filter channel closed");
                                break;
                            }
                        } else {
                            warn!(
                                mint = %mint_str,
                                reason = canary_filter.rejection_reason.as_deref().unwrap_or("unknown"),
                                narrative_score = format!("{:.1}", narrative_context.narrative_score),
                                "❌ NARRATIVE-CLUSTER LIVE CANARY REJECT — Fast-Track safety blocked"
                            );
                            token.pipeline_timing.outcome =
                                Some("rejected_narrative_cluster_live_canary".to_string());
                            token.pipeline_timing.rejection_stage =
                                Some("narrative_cluster_live_canary".to_string());
                            token.pipeline_timing.rejection_reason = canary_filter
                                .rejection_reason
                                .clone()
                                .or_else(|| Some("fast_track_safety_unknown".to_string()));
                            let timing_payload = token.pipeline_timing.to_json(&mint_str);
                            let supabase_bg = Arc::clone(&supabase);
                            tokio::spawn(async move {
                                log_pipeline_latency(&supabase_bg, &timing_payload).await;
                            });
                            if let Some(cid) = candidate_id {
                                tracker::spawn_rejected_tracker(
                                    Arc::clone(&supabase),
                                    cid,
                                    mint_str.clone(),
                                );
                            }
                        }
                        continue;
                    }
                }
            }

            // ── BC Fast-Track: check cache for pre-computed BC score ──
            if cfg.strategy.filters.bc_fast_track_enabled
                && token.source == crate::detection::types::DetectionSource::PumpFun
            {
                if let Some(bc_entry) = bc_entry.clone() {
                    if bc_entry.score >= cfg.strategy.filters.bc_fast_track_min_score {
                        info!(
                            mint = %mint_str,
                            bc_score = format!("{:.1}", bc_entry.score),
                            threshold = cfg.strategy.filters.bc_fast_track_min_score,
                            "⚡ BC FAST-TRACK — score qualifies, running minimal enrichment"
                        );

                        let ft_start = std::time::Instant::now();

                        // Record detection → sniper latency
                        let detection_to_sniper_ms = {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            now_ms - detected_at
                        };
                        token.pipeline_timing.detection_to_sniper_ms = Some(detection_to_sniper_ms);

                        // Fast enrichment: only mint + GoPlus (~250ms)
                        let ft_enrichment = enrichment::enrich_token_fast(&rpc, &mint_str).await;
                        token.pipeline_timing.enrichment_total_ms =
                            Some(ft_enrichment.enrichment_duration_ms);
                        token.pipeline_timing.enrichment_per_source =
                            ft_enrichment.per_source_ms.clone();

                        // Fast-track filters: mint_auth, freeze_auth, honeypot, GoPlus critical
                        let ft_filter = filters::apply_fast_track_filters(&ft_enrichment);
                        let ft_elapsed = ft_start.elapsed();

                        // Build minimal sniper_features for logging
                        let ft_features = serde_json::json!({
                            "entry_tier": "fast_track",
                            "bc_score": bc_entry.score,
                            "bc_unique_buyers": bc_entry.unique_buyers,
                            "bc_buy_sell_ratio": bc_entry.buy_sell_ratio,
                            "bc_creator_rebuy": bc_entry.creator_rebuy,
                            "bc_whale_buy": bc_entry.whale_buy,
                            "bc_max_single_buy_sol": bc_entry.max_single_buy_sol,
                            "bc_progress_pct_at_score": bc_entry.bc_progress_pct,
                            "bc_buy_count": bc_entry.buy_count,
                            "bc_sell_count": bc_entry.sell_count,
                            "bc_total_volume_sol": bc_entry.total_volume_sol,
                            "fast_track_enrichment_ms": ft_enrichment.enrichment_duration_ms,
                            "mint_authority_revoked": ft_enrichment.on_chain_mint.as_ref().map(|m| m.mint_authority_revoked),
                            "freeze_authority_revoked": ft_enrichment.on_chain_mint.as_ref().map(|m| m.freeze_authority_revoked),
                            "goplus_honeypot": ft_enrichment.goplus.as_ref().and_then(|g| g.is_honeypot.clone()),
                            "initial_liquidity_sol": initial_liquidity_sol,
                        });

                        let candidate_id = log_sniper_candidate(
                            &supabase,
                            &mint_str,
                            &token.name,
                            &token.symbol,
                            token
                                .pool_address
                                .as_ref()
                                .map(|p| p.to_string())
                                .as_deref(),
                            &creator_str,
                            initial_liquidity_sol,
                            if ft_filter.passed {
                                "fast_track_passed"
                            } else {
                                "rejected"
                            },
                            ft_filter.rejection_reason.as_deref(),
                            ft_filter.filter_name.as_deref(),
                            bc_entry.score,
                            &ft_features,
                        )
                        .await;

                        if ft_filter.passed {
                            info!(
                                mint = %mint_str,
                                bc_score = format!("{:.1}", bc_entry.score),
                                elapsed_ms = ft_elapsed.as_millis() as u64,
                                "⚡ FAST-TRACK PASS — forwarding to filter engine (deferred verification will run post-buy)"
                            );

                            token.candidate_id = candidate_id;
                            token.sniper_features = Some(ft_features);
                            token.sniper_score = Some(bc_entry.score);

                            // Spawn deferred verification (runs full enrichment post-buy)
                            let deferred_supabase = Arc::clone(&supabase);
                            let deferred_cfg = Arc::clone(&cfg);
                            let deferred_mint = mint_str.clone();
                            let deferred_creator = creator_str.clone();
                            let deferred_liq = initial_liquidity_sol;
                            let deferred_detected_at = detected_at;
                            let deferred_candidate_id = candidate_id;
                            tokio::spawn(async move {
                                run_deferred_verification(
                                    deferred_cfg,
                                    deferred_supabase,
                                    deferred_mint,
                                    deferred_creator,
                                    deferred_liq,
                                    deferred_detected_at,
                                    deferred_candidate_id,
                                )
                                .await;
                            });

                            if tx.send(token).await.is_err() {
                                warn!("Sniper → filter channel closed");
                                break;
                            }
                        } else {
                            warn!(
                                mint = %mint_str,
                                reason = ft_filter.rejection_reason.as_deref().unwrap_or("unknown"),
                                bc_score = format!("{:.1}", bc_entry.score),
                                "❌ FAST-TRACK REJECT — safety filter blocked"
                            );

                            token.pipeline_timing.outcome =
                                Some("rejected_fast_track_filter".to_string());
                            token.pipeline_timing.rejection_stage =
                                Some("fast_track_filter".to_string());
                            token.pipeline_timing.rejection_reason = ft_filter.rejection_reason;
                            let timing_payload = token.pipeline_timing.to_json(&mint_str);
                            let supabase_bg = Arc::clone(&supabase);
                            tokio::spawn(async move {
                                log_pipeline_latency(&supabase_bg, &timing_payload).await;
                            });
                        }
                        continue; // Skip normal pipeline — fast-track handled it
                    }
                }
            }

            // ── v14.1 Standard-lane kill switch ──
            // Data showed Standard lane is barely break-even (+0.014 SOL across
            // 16 closed trades, every position peaked < 2x). When disabled,
            // a token that didn't qualify for Fast-Track is rejected here
            // BEFORE the 2s enrichment runs — saves API budget too.
            if !cfg.strategy.filters.standard_lane_enabled {
                warn!(
                    mint = %mint_str,
                    "❌ STANDARD LANE DISABLED — token did not qualify for Fast-Track, rejecting"
                );
                token.pipeline_timing.outcome = Some("rejected_standard_disabled".to_string());
                token.pipeline_timing.rejection_stage = Some("standard_disabled".to_string());
                token.pipeline_timing.rejection_reason =
                    Some("standard lane disabled in config".to_string());
                let timing_payload = token.pipeline_timing.to_json(&mint_str);
                let supabase_bg = Arc::clone(&supabase);
                tokio::spawn(async move {
                    log_pipeline_latency(&supabase_bg, &timing_payload).await;
                });
                continue;
            }

            let enrichment_start = std::time::Instant::now();

            // Record detection → sniper latency
            let detection_to_sniper_ms = {
                let now_ms = chrono::Utc::now().timestamp_millis();
                now_ms - detected_at
            };
            token.pipeline_timing.detection_to_sniper_ms = Some(detection_to_sniper_ms);

            // ── Step 1: Run parallel enrichment (2s budget) ──
            let enrichment = enrichment::enrich_token(
                &cfg,
                &rpc,
                &mint_str,
                &creator_str,
                initial_liquidity_sol,
                detected_at,
            )
            .await;

            // Record enrichment timing
            token.pipeline_timing.enrichment_total_ms = Some(enrichment.enrichment_duration_ms);
            token.pipeline_timing.enrichment_per_source = enrichment.per_source_ms.clone();

            // ── Step 2: Compute soft flags ──
            let soft_flags = features::compute_soft_flags(&enrichment);

            // ── Step 3: Get SOL price for features ──
            let sol_price = jupiter.get_price(SOL_MINT).await.unwrap_or(150.0);
            let initial_liquidity_usd = initial_liquidity_sol * sol_price;

            let detection_latency_ms = {
                let now_ms = chrono::Utc::now().timestamp_millis();
                now_ms - detected_at
            };

            // ── Step 4: Build sniper_features JSONB ──
            let bonding_ctx = features::BondingCurveContext {
                bonding_curve_volume_sol: token.bonding_curve_volume_sol,
                buy_pressure_pct: token.buy_pressure_pct,
                time_to_graduate_seconds: token.time_to_graduate_seconds,
                unique_buyer_count: token.unique_buyer_count,
                buy_count: token.buy_count,
                sell_count: token.sell_count,
            };
            let sniper_features = features::build_sniper_features(
                &enrichment,
                initial_liquidity_sol,
                initial_liquidity_usd,
                detected_at,
                detection_latency_ms,
                sol_price,
                &soft_flags,
                &bonding_ctx,
            );

            // ── Step 5: Compute score (Phase 3 placeholder) ──
            let score = scoring::compute_sniper_score(&sniper_features);

            // ── Step 5b: Minimum sniper score gate (v9, data-driven) ──
            // Data: score >= 65 → +0.056 SOL, score < 65 → -0.500 SOL.
            // Using 60 as threshold to be conservative while still cutting worst trades.
            let min_score = cfg.strategy.filters.min_sniper_score;
            if score.score < min_score {
                warn!(
                    mint = %mint_str,
                    score = format!("{:.1}", score.score),
                    threshold = min_score,
                    "❌ SNIPER REJECT — sniper_score {:.1} < {:.0}",
                    score.score, min_score
                );
                let rejection_reason =
                    format!("sniper_score={:.1} < {:.0}", score.score, min_score);
                let filter_name = "sniper_score";
                let enrichment_elapsed = enrichment_start.elapsed();
                let _candidate_id = log_sniper_candidate(
                    &supabase,
                    &mint_str,
                    &token.name,
                    &token.symbol,
                    token
                        .pool_address
                        .as_ref()
                        .map(|p| p.to_string())
                        .as_deref(),
                    &creator_str,
                    initial_liquidity_sol,
                    "rejected",
                    Some(rejection_reason.as_str()),
                    Some(filter_name),
                    score.score,
                    &sniper_features,
                )
                .await;

                // Write pipeline_latency for rejected token
                token.pipeline_timing.outcome = Some("rejected_sniper_score".to_string());
                token.pipeline_timing.rejection_stage = Some("sniper_score".to_string());
                token.pipeline_timing.rejection_reason = Some(rejection_reason);
                let timing_payload = token.pipeline_timing.to_json(&mint_str);
                let supabase_bg = Arc::clone(&supabase);
                tokio::spawn(async move {
                    log_pipeline_latency(&supabase_bg, &timing_payload).await;
                });
                continue;
            }

            // ── Step 6: Apply hard filters ──
            let hard_filter_start = std::time::Instant::now();
            let filter_result = filters::apply_hard_filters(&enrichment, initial_liquidity_sol);
            let hard_filter_ms = hard_filter_start.elapsed().as_millis() as u64;
            token.pipeline_timing.hard_filter_total_ms = Some(hard_filter_ms);

            let enrichment_elapsed = enrichment_start.elapsed();

            // ── Step 7: Log to sniper_candidates ──
            // NOTE: "sniper_passed" means it passed hard filters. JOIN with positions
            // table to confirm it was actually bought (downstream filters/execution may reject).
            let action = if filter_result.passed {
                "sniper_passed"
            } else {
                "rejected"
            };
            let candidate_id = log_sniper_candidate(
                &supabase,
                &mint_str,
                &token.name,
                &token.symbol,
                token
                    .pool_address
                    .as_ref()
                    .map(|p| p.to_string())
                    .as_deref(),
                &creator_str,
                initial_liquidity_sol,
                action,
                filter_result.rejection_reason.as_deref(),
                filter_result.filter_name.as_deref(),
                score.score,
                &sniper_features,
            )
            .await;

            if filter_result.passed {
                info!(
                    mint = %mint_str,
                    preliminary_score = format!("{:.1}", score.score),
                    enrichment_ms = enrichment_elapsed.as_millis() as u64,
                    sources = enrichment.sources_completed.len(),
                    "✅ SNIPER PASS — forwarding to filter engine"
                );

                // Attach candidate ID so filter-engine rejects can be tracked
                token.candidate_id = candidate_id;

                // Attach features/score so downstream (positions insert) can
                // persist them without re-joining sniper_candidates.
                token.sniper_features = Some(sniper_features.clone());
                token.sniper_score = Some(score.score);

                // Forward to existing filter engine (pipeline_timing carried in token)
                if tx.send(token).await.is_err() {
                    warn!("Sniper → filter channel closed");
                    break;
                }
            } else {
                warn!(
                    mint = %mint_str,
                    reason = filter_result.rejection_reason.as_deref().unwrap_or("unknown"),
                    preliminary_score = format!("{:.1}", score.score),
                    "❌ SNIPER REJECT — hard filter blocked"
                );

                // Write pipeline_latency for rejected token
                token.pipeline_timing.outcome = Some("rejected_hard_filter".to_string());
                token.pipeline_timing.rejection_stage = Some("hard_filter".to_string());
                token.pipeline_timing.rejection_reason = filter_result.rejection_reason.clone();
                let timing_payload = token.pipeline_timing.to_json(&mint_str);
                let supabase_bg = Arc::clone(&supabase);
                tokio::spawn(async move {
                    log_pipeline_latency(&supabase_bg, &timing_payload).await;
                });

                // Spawn rejected token price tracker (counterfactual data)
                if let Some(cid) = candidate_id {
                    tracker::spawn_rejected_tracker(Arc::clone(&supabase), cid, mint_str.clone());
                }
            }
        }

        info!("Sniper enrichment pipeline shutting down");
    });

    rx
}

/// Log a candidate (bought or rejected) to the sniper_candidates table.
/// Returns the row ID if insertion succeeds.
async fn log_sniper_candidate(
    supabase: &SupabaseClient,
    mint: &str,
    name: &str,
    symbol: &str,
    pool_address: Option<&str>,
    creator_wallet: &str,
    initial_liquidity_sol: f64,
    action: &str,
    rejection_reason: Option<&str>,
    filter_name: Option<&str>,
    sniper_score: f64,
    sniper_features: &serde_json::Value,
) -> Option<i64> {
    let payload = serde_json::json!({
        "mint": mint,
        "symbol": symbol,
        "name": name,
        "pool_address": pool_address,
        "creator_wallet": creator_wallet,
        "initial_liquidity_sol": initial_liquidity_sol,
        "action": action,
        "rejection_reason": rejection_reason,
        "filter_name": filter_name,
        "sniper_score": sniper_score,
        "sniper_features": sniper_features,
    });

    let url = format!("{}/sniper_candidates", supabase.base_url);

    // Use Prefer: return=representation to get the inserted row back
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
            let id = rows
                .first()
                .and_then(|r| r.get("id"))
                .and_then(|v| v.as_i64());
            if id.is_some() {
                info!(mint = %mint, action = action, "Sniper candidate logged");
            }
            id
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "Failed to log sniper candidate: {}", &body[..body.len().min(200)]);
            None
        }
        Err(e) => {
            error!(mint = %mint, "Sniper candidate log error: {}", e);
            None
        }
    }
}

async fn log_creator_rebuy_strict_2x_shadow_candidate(
    supabase: &SupabaseClient,
    mint: &str,
    name: &str,
    symbol: &str,
    pool_address: Option<&str>,
    creator_wallet: &str,
    initial_liquidity_sol: f64,
    bc_entry: &BcScoreEntry,
    filters_cfg: &FiltersConfig,
    sim_buy_sol: f64,
) -> Option<i64> {
    let target_multiplier = filters_cfg.creator_rebuy_strict_2x_shadow_target_multiplier;
    let strict_features = serde_json::json!({
        "entry_tier": CREATOR_REBUY_STRICT_2X_SHADOW_ENTRY_TIER,
        "shadow_mode": true,
        "live_forwarded": false,
        "live_exit_unchanged": true,
        "shadow_exit_multiplier": target_multiplier,
        "shadow_sell_pct": 100.0,
        "shadow_sim_buy_sol": sim_buy_sol,
        "creator_rebuy_strict_2x_shadow_enabled": filters_cfg.creator_rebuy_strict_2x_shadow_enabled,
        "creator_rebuy_strict_2x_shadow_min_total_volume_sol": filters_cfg.creator_rebuy_strict_2x_shadow_min_total_volume_sol,
        "creator_rebuy_strict_2x_shadow_target_multiplier": target_multiplier,
        "creator_rebuy_structural_rescue_min_unique_buyers": filters_cfg.creator_rebuy_structural_rescue_min_unique_buyers,
        "creator_rebuy_structural_rescue_max_total_volume_sol": filters_cfg.creator_rebuy_structural_rescue_max_total_volume_sol,
        "creator_rebuy_structural_rescue_min_initial_liquidity_sol": filters_cfg.creator_rebuy_structural_rescue_min_initial_liquidity_sol,
        "creator_rebuy_structural_rescue_max_initial_liquidity_sol": filters_cfg.creator_rebuy_structural_rescue_max_initial_liquidity_sol,
        "creator_rebuy_structural_rescue_min_token_age_secs": filters_cfg.creator_rebuy_structural_rescue_min_token_age_secs,
        "bc_score": bc_entry.score,
        "bc_unique_buyers": bc_entry.unique_buyers,
        "bc_buy_sell_ratio": bc_entry.buy_sell_ratio,
        "bc_buy_pressure_pct": if bc_entry.buy_count + bc_entry.sell_count > 0 {
            (bc_entry.buy_count as f64 / (bc_entry.buy_count + bc_entry.sell_count) as f64) * 100.0
        } else {
            0.0
        },
        "bc_creator_rebuy": bc_entry.creator_rebuy,
        "bc_whale_buy": bc_entry.whale_buy,
        "bc_max_single_buy_sol": bc_entry.max_single_buy_sol,
        "bc_progress_pct_at_score": bc_entry.bc_progress_pct,
        "bc_buy_count": bc_entry.buy_count,
        "bc_sell_count": bc_entry.sell_count,
        "bc_total_volume_sol": bc_entry.total_volume_sol,
        "bc_creator_buy_count": bc_entry.creator_buy_count_bc,
        "bc_creator_buy_sol_total": bc_entry.creator_buy_sol_total_bc,
        "bc_creator_sell_count": bc_entry.creator_sell_count_bc,
        "bc_creator_sell_sol_total": bc_entry.creator_sell_sol_total_bc,
        "bc_creator_net_sol": bc_entry.creator_net_sol_bc,
        "bc_creator_buy_share_pct": bc_entry.creator_buy_share_pct,
        "bc_score_recorded_at": bc_entry.recorded_at,
        "initial_liquidity_sol": initial_liquidity_sol,
    });

    log_sniper_candidate(
        supabase,
        mint,
        name,
        symbol,
        pool_address,
        creator_wallet,
        initial_liquidity_sol,
        "creator_rebuy_strict_2x_shadow_passed",
        Some("shadow_only_live_exit_remains_1p5x"),
        Some("creator_rebuy_strict_2x_shadow"),
        bc_entry.score,
        &strict_features,
    )
    .await
}

async fn log_narrative_cluster_phase2_shadow_candidate(
    supabase: &SupabaseClient,
    token: &GraduatedToken,
    mint: &str,
    creator_wallet: &str,
    initial_liquidity_sol: f64,
    context: &NarrativeClusterContext,
    live_rejection_reason: Option<&str>,
    bc_rejection_reason: Option<&str>,
    filters_cfg: &FiltersConfig,
) -> Option<i64> {
    let phase2_features = serde_json::json!({
        "entry_tier": "narrative_cluster_phase2_shadow",
        "shadow_mode": true,
        "live_forwarded": false,
        "live_rejection_reason": live_rejection_reason,
        "bc_rejection_reason": bc_rejection_reason,
        "narrative_cluster_phase2_shadow_enabled": filters_cfg.narrative_cluster_phase2_shadow_enabled,
        "narrative_cluster_phase2_shadow_min_score": filters_cfg.narrative_cluster_phase2_shadow_min_score,
        "narrative_cluster_phase2_shadow_min_buy_pressure_pct": filters_cfg.narrative_cluster_phase2_shadow_min_buy_pressure_pct,
        "narrative_cluster_phase2_shadow_min_buy_sell_ratio": filters_cfg.narrative_cluster_phase2_shadow_min_buy_sell_ratio,
        "narrative_cluster_phase2_shadow_max_sell_count": filters_cfg.narrative_cluster_phase2_shadow_max_sell_count,
        "narrative_cluster_phase2_shadow_max_label_gap_seconds": filters_cfg.narrative_cluster_phase2_shadow_max_label_gap_seconds,
        "narrative_cluster_phase2_shadow_min_initial_liquidity_sol": filters_cfg.narrative_cluster_phase2_shadow_min_initial_liquidity_sol,
        "narrative_cluster_phase2_shadow_max_initial_liquidity_sol": filters_cfg.narrative_cluster_phase2_shadow_max_initial_liquidity_sol,
        "narrative_cluster_phase2_shadow_require_valid_identity": filters_cfg.narrative_cluster_phase2_shadow_require_valid_identity,
        "narrative_cluster_phase2_shadow_require_no_creator_sold": filters_cfg.narrative_cluster_phase2_shadow_require_no_creator_sold,
        "narrative_score": context.narrative_score,
        "normalized_label": context.normalized_label,
        "cluster_rank": context.cluster_rank,
        "prior_same_label_mints_6h": context.prior_same_label_mints_6h,
        "prior_same_label_creators_6h": context.prior_same_label_creators_6h,
        "seconds_since_label_seen": context.seconds_since_label_seen,
        "entry_volume_sol": context.entry_volume_sol,
        "entry_buy_count": context.entry_buy_count,
        "entry_sell_count": context.entry_sell_count,
        "entry_unique_buyers": context.entry_unique_buyers,
        "entry_buy_sell_ratio": context.entry_buy_sell_ratio,
        "entry_buy_pressure_pct": context.entry_buy_pressure_pct,
        "creator_rebuy_bypassed": context.creator_rebuy_bypassed,
        "creator_sold_during_bc": context.creator_sold_during_bc,
        "creator_buy_count_bc": context.creator_buy_count_bc,
        "creator_buy_sol_total_bc": context.creator_buy_sol_total_bc,
        "creator_buy_max_sol_bc": context.creator_buy_max_sol_bc,
        "creator_first_buy_after_secs": context.creator_first_buy_after_secs,
        "creator_first_buy_progress_pct": context.creator_first_buy_progress_pct,
        "creator_last_buy_after_secs": context.creator_last_buy_after_secs,
        "creator_last_buy_progress_pct": context.creator_last_buy_progress_pct,
        "creator_sell_count_bc": context.creator_sell_count_bc,
        "creator_sell_sol_total_bc": context.creator_sell_sol_total_bc,
        "creator_net_sol_bc": context.creator_net_sol_bc,
        "creator_buy_share_pct": context.creator_buy_share_pct,
        "whale_buy": context.whale_buy,
        "whale_buy_max_sol": context.whale_buy_max_sol,
        "initial_liquidity_sol": initial_liquidity_sol,
    });

    log_sniper_candidate(
        supabase,
        mint,
        &token.name,
        &token.symbol,
        token
            .pool_address
            .as_ref()
            .map(|p| p.to_string())
            .as_deref(),
        creator_wallet,
        initial_liquidity_sol,
        "narrative_cluster_phase2_shadow_passed",
        live_rejection_reason
            .or(bc_rejection_reason)
            .or(Some("shadow_only_not_live")),
        Some("narrative_cluster_phase2_shadow"),
        context.narrative_score,
        &phase2_features,
    )
    .await
}

async fn mark_narrative_cluster_would_trade_live(supabase: &SupabaseClient, mint: &str) {
    let url = format!(
        "{}/narrative_cluster_shadow?mint=eq.{}",
        supabase.base_url, mint
    );
    let payload = serde_json::json!({
        "would_trade_live": true,
    });

    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "Failed to mark narrative_cluster_shadow live candidate: {}", body);
        }
        Err(e) => {
            warn!(mint = %mint, "narrative_cluster_shadow live marker error: {}", e);
        }
    }
}

async fn mark_narrative_cluster_phase2_shadow(supabase: &SupabaseClient, mint: &str) {
    let url = format!(
        "{}/narrative_cluster_shadow?mint=eq.{}",
        supabase.base_url, mint
    );
    let payload = serde_json::json!({
        "phase2_shadow_passed": true,
        "phase2_shadow_profile": "score70_pressure80_bsr4_sells3_gap300_liq30_85_no_creator_sold_allow_rebuy",
        "phase2_shadow_checked_at": chrono::Utc::now().to_rfc3339(),
    });

    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "Failed to mark narrative_cluster_shadow Phase 2 candidate: {}", body);
        }
        Err(e) => {
            warn!(mint = %mint, "narrative_cluster_shadow Phase 2 marker error: {}", e);
        }
    }
}

/// Write a pipeline_latency row to Supabase (fire-and-forget).
pub async fn log_pipeline_latency(supabase: &SupabaseClient, payload: &serde_json::Value) {
    let url = format!("{}/pipeline_latency", supabase.base_url);
    match supabase.client.post(&url).json(payload).send().await {
        Ok(resp) if resp.status().is_success() => { /* ok */ }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(
                "Failed to write pipeline_latency: {}",
                &body[..body.len().min(200)]
            );
        }
        Err(e) => {
            warn!("pipeline_latency write error: {}", e);
        }
    }
}

/// Deferred verification for fast-track tokens.
/// Runs 3 seconds after the buy to let on-chain state settle, then performs
/// full SolanaTracker enrichment and checks the filters that were skipped
/// during fast-track entry (bundlers, top10, dev holding, holders).
/// If any deferred filter fails, updates sniper_candidates with the failure
/// reason. The monitoring system should check for `deferred_rejected` status
/// and trigger an emergency exit.
async fn run_deferred_verification(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    mint: String,
    creator_wallet: String,
    initial_liquidity_sol: f64,
    detected_at: i64,
    candidate_id: Option<i64>,
) {
    // Wait 3 seconds for on-chain state to settle
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    info!(
        mint = %mint,
        "🔍 Deferred verification starting for fast-track token"
    );

    let rpc = RpcClient::new(cfg.env.solana_rpc_url.clone());

    // Run full enrichment (same as normal pipeline)
    let enrichment = enrichment::enrich_token(
        &cfg,
        &rpc,
        &mint,
        &creator_wallet,
        initial_liquidity_sol,
        detected_at,
    )
    .await;

    // Apply deferred filters
    let filter_result = filters::apply_deferred_filters(&enrichment, initial_liquidity_sol);

    if filter_result.passed {
        info!(
            mint = %mint,
            sources = enrichment.sources_completed.len(),
            "✅ Deferred verification PASSED — fast-track position validated"
        );

        // Update sniper_candidates with deferred result
        if let Some(cid) = candidate_id {
            let url = format!("{}/sniper_candidates?id=eq.{}", supabase.base_url, cid);
            let payload = serde_json::json!({
                "sniper_features": {
                    "deferred_verification": "passed",
                    "deferred_sources_completed": enrichment.sources_completed,
                    "deferred_enrichment_ms": enrichment.enrichment_duration_ms,
                }
            });
            // Merge deferred features into existing sniper_features
            let _ = supabase.client.patch(&url).json(&payload).send().await;
        }
    } else {
        let reason = filter_result
            .rejection_reason
            .as_deref()
            .unwrap_or("unknown");
        warn!(
            mint = %mint,
            reason = reason,
            "🚨 Deferred verification FAILED — fast-track position should be exited"
        );

        // Update sniper_candidates to signal deferred rejection
        if let Some(cid) = candidate_id {
            let url = format!("{}/sniper_candidates?id=eq.{}", supabase.base_url, cid);
            let payload = serde_json::json!({
                "action": "deferred_rejected",
                "rejection_reason": format!("deferred: {}", reason),
                "filter_name": filter_result.filter_name,
            });
            let _ = supabase.client.patch(&url).json(&payload).send().await;
        }
    }
}
