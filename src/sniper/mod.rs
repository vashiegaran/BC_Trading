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
use tracing::{info, warn, error};

use crate::config::AppConfig;
use crate::detection::types::{GraduatedToken, BcScoreCache};
use crate::execution::jupiter::{JupiterClient, SOL_MINT};
use crate::logger::SupabaseClient;

const SNIPER_CHANNEL_CAPACITY: usize = 100;

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
                "11111111111111111111111111111111",             // System Program
                "So11111111111111111111111111111111111111112",   // Wrapped SOL
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  // SPL Token Program
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",   // Token-2022
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

                if let Some(reason) = filters::check_bc_pattern(&token, &cfg.strategy.filters, bc_score) {
                    let creator_rebuy_shadow_qualifies = reason == "creator_rebuy_detected"
                        && cfg.strategy.filters.creator_rebuy_shadow_enabled
                        && bc_entry
                            .as_ref()
                            .map(|entry| entry.score >= cfg.strategy.filters.creator_rebuy_shadow_min_score)
                            .unwrap_or(false);

                    if creator_rebuy_shadow_qualifies {
                        let bc_entry = bc_entry.clone().expect("creator-rebuy shadow qualification requires BC entry");
                        warn!(
                            mint = %mint_str,
                            bc_score = format!("{:.1}", bc_entry.score),
                            threshold = cfg.strategy.filters.creator_rebuy_shadow_min_score,
                            "🧪 CREATOR-REBUY SHADOW — live blocked, running Fast-Track safety checks only"
                        );

                        let shadow_start = std::time::Instant::now();
                        let detection_to_sniper_ms = {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            now_ms - detected_at
                        };
                        token.pipeline_timing.detection_to_sniper_ms = Some(detection_to_sniper_ms);

                        let shadow_enrichment = enrichment::enrich_token_fast(&rpc, &mint_str).await;
                        token.pipeline_timing.enrichment_total_ms = Some(shadow_enrichment.enrichment_duration_ms);
                        token.pipeline_timing.enrichment_per_source = shadow_enrichment.per_source_ms.clone();

                        let shadow_filter = filters::apply_fast_track_filters(&shadow_enrichment);
                        let shadow_elapsed = shadow_start.elapsed();
                        let shadow_action = if shadow_filter.passed {
                            "creator_rebuy_shadow_passed"
                        } else {
                            "creator_rebuy_shadow_rejected"
                        };
                        let shadow_rejection_reason = if shadow_filter.passed {
                            Some("creator_rebuy_detected_live_block")
                        } else {
                            shadow_filter.rejection_reason.as_deref()
                        };
                        let shadow_filter_name = if shadow_filter.passed {
                            Some("creator_rebuy_shadow")
                        } else {
                            shadow_filter.filter_name.as_deref()
                        };

                        let shadow_features = serde_json::json!({
                            "entry_tier": "creator_rebuy_shadow_fast_track",
                            "shadow_mode": true,
                            "live_forwarded": false,
                            "blocked_live_reason": reason,
                            "shadow_min_score": cfg.strategy.filters.creator_rebuy_shadow_min_score,
                            "bc_score": bc_entry.score,
                            "bc_unique_buyers": bc_entry.unique_buyers,
                            "bc_buy_sell_ratio": bc_entry.buy_sell_ratio,
                            "bc_creator_rebuy": bc_entry.creator_rebuy,
                            "bc_whale_buy": bc_entry.whale_buy,
                            "bc_buy_count": bc_entry.buy_count,
                            "bc_sell_count": bc_entry.sell_count,
                            "bc_total_volume_sol": bc_entry.total_volume_sol,
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

                        let candidate_id = log_sniper_candidate(
                            &supabase,
                            &mint_str,
                            &token.name,
                            &token.symbol,
                            token.pool_address.as_ref().map(|p| p.to_string()).as_deref(),
                            &creator_str,
                            initial_liquidity_sol,
                            shadow_action,
                            shadow_rejection_reason,
                            shadow_filter_name,
                            bc_entry.score,
                            &shadow_features,
                        ).await;

                        if shadow_filter.passed {
                            info!(
                                mint = %mint_str,
                                bc_score = format!("{:.1}", bc_entry.score),
                                elapsed_ms = shadow_elapsed.as_millis() as u64,
                                "🧪 CREATOR-REBUY SHADOW PASS — counterfactual tracked, live execution still blocked"
                            );
                            token.pipeline_timing.outcome = Some("shadow_creator_rebuy_fast_track_passed".to_string());
                            token.pipeline_timing.rejection_stage = Some("creator_rebuy_shadow".to_string());
                            token.pipeline_timing.rejection_reason = Some("creator_rebuy_detected_live_block".to_string());
                        } else {
                            warn!(
                                mint = %mint_str,
                                reason = shadow_filter.rejection_reason.as_deref().unwrap_or("unknown"),
                                bc_score = format!("{:.1}", bc_entry.score),
                                "🧪 CREATOR-REBUY SHADOW REJECT — Fast-Track safety filter blocked"
                            );
                            token.pipeline_timing.outcome = Some("shadow_creator_rebuy_fast_track_rejected".to_string());
                            token.pipeline_timing.rejection_stage = Some("creator_rebuy_shadow_fast_track_filter".to_string());
                            token.pipeline_timing.rejection_reason = shadow_filter
                                .rejection_reason
                                .clone()
                                .or_else(|| Some("fast_track_safety_unknown".to_string()));
                        }

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
                        token.pool_address.as_ref().map(|p| p.to_string()).as_deref(),
                        &creator_str,
                        initial_liquidity_sol,
                        "rejected",
                        Some(&reason),
                        Some("bc_pattern"),
                        0.0,
                        &serde_json::json!({}),
                    ).await;

                    // Log pipeline_latency
                    let supabase_clone = Arc::clone(&supabase);
                    let timing_json = token.pipeline_timing.to_json(&mint_str);
                    tokio::spawn(async move {
                        let url = format!("{}/pipeline_latency", supabase_clone.base_url);
                        let _ = supabase_clone.client.post(&url).json(&timing_json).send().await;
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
                        token.pipeline_timing.enrichment_total_ms = Some(ft_enrichment.enrichment_duration_ms);
                        token.pipeline_timing.enrichment_per_source = ft_enrichment.per_source_ms.clone();

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
                            token.pool_address.as_ref().map(|p| p.to_string()).as_deref(),
                            &creator_str,
                            initial_liquidity_sol,
                            if ft_filter.passed { "fast_track_passed" } else { "rejected" },
                            ft_filter.rejection_reason.as_deref(),
                            ft_filter.filter_name.as_deref(),
                            bc_entry.score,
                            &ft_features,
                        ).await;

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
                                ).await;
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

                            token.pipeline_timing.outcome = Some("rejected_fast_track_filter".to_string());
                            token.pipeline_timing.rejection_stage = Some("fast_track_filter".to_string());
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
                let rejection_reason = format!("sniper_score={:.1} < {:.0}", score.score, min_score);
                let filter_name = "sniper_score";
                let enrichment_elapsed = enrichment_start.elapsed();
                let _candidate_id = log_sniper_candidate(
                    &supabase,
                    &mint_str,
                    &token.name,
                    &token.symbol,
                    token.pool_address.as_ref().map(|p| p.to_string()).as_deref(),
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
            let action = if filter_result.passed { "sniper_passed" } else { "rejected" };
            let candidate_id = log_sniper_candidate(
                &supabase,
                &mint_str,
                &token.name,
                &token.symbol,
                token.pool_address.as_ref().map(|p| p.to_string()).as_deref(),
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
                    tracker::spawn_rejected_tracker(
                        Arc::clone(&supabase),
                        cid,
                        mint_str.clone(),
                    );
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
            let id = rows.first().and_then(|r| r.get("id")).and_then(|v| v.as_i64());
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

/// Write a pipeline_latency row to Supabase (fire-and-forget).
pub async fn log_pipeline_latency(supabase: &SupabaseClient, payload: &serde_json::Value) {
    let url = format!("{}/pipeline_latency", supabase.base_url);
    match supabase.client.post(&url).json(payload).send().await {
        Ok(resp) if resp.status().is_success() => { /* ok */ }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to write pipeline_latency: {}", &body[..body.len().min(200)]);
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
    ).await;

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
            let url = format!(
                "{}/sniper_candidates?id=eq.{}",
                supabase.base_url, cid
            );
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
        let reason = filter_result.rejection_reason.as_deref().unwrap_or("unknown");
        warn!(
            mint = %mint,
            reason = reason,
            "🚨 Deferred verification FAILED — fast-track position should be exited"
        );

        // Update sniper_candidates to signal deferred rejection
        if let Some(cid) = candidate_id {
            let url = format!(
                "{}/sniper_candidates?id=eq.{}",
                supabase.base_url, cid
            );
            let payload = serde_json::json!({
                "action": "deferred_rejected",
                "rejection_reason": format!("deferred: {}", reason),
                "filter_name": filter_result.filter_name,
            });
            let _ = supabase.client.patch(&url).json(&payload).send().await;
        }
    }
}
