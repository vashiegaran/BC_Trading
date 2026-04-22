pub mod age;
pub mod buy_pressure;
pub mod goplus;
pub mod helius_das;
pub mod holders;
pub mod honeypot;
pub mod liquidity;
pub mod market_cap;
pub mod post_buy;
pub mod price_impact;
pub mod rpc_fallback;
pub mod rugcheck;
pub mod sanity;
pub mod smart_wallet;
pub mod token_safety;
pub mod types;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tracing::{info, warn, error};

use crate::config::AppConfig;
use crate::detection::types::{DetectionSource, GraduatedToken};
use crate::execution::wallet::BotWallet;
use crate::sniper::log_pipeline_latency;
use crate::logger::SupabaseClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use types::{FilterSummary, FilteredToken};

use age::AgeFilter;
use buy_pressure::BuyPressureFilter;
use liquidity::LiquidityFilter;
use price_impact::PriceImpactFilter;
use sanity::SanityFilter;

const FILTER_CHANNEL_CAPACITY: usize = 50;

pub fn start(
    cfg: Arc<AppConfig>,
    mut detection_rx: mpsc::Receiver<GraduatedToken>,
    supabase: Arc<SupabaseClient>,
    wallet: Arc<BotWallet>,
) -> mpsc::Receiver<FilteredToken> {
    let (tx, rx) = mpsc::channel::<FilteredToken>(FILTER_CHANNEL_CAPACITY);

    let pumpfun_count = Arc::new(AtomicU64::new(0));
    let raydium_count = Arc::new(AtomicU64::new(0));
    let total_age_sum = Arc::new(AtomicU64::new(0));
    let total_tokens = Arc::new(AtomicU64::new(0));

    let pf = Arc::clone(&pumpfun_count);
    let ry = Arc::clone(&raydium_count);
    let age_sum = Arc::clone(&total_age_sum);
    let tot = Arc::clone(&total_tokens);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(600));
        loop {
            interval.tick().await;
            let pf_val = pf.swap(0, Ordering::Relaxed);
            let ry_val = ry.swap(0, Ordering::Relaxed);
            let age_val = age_sum.swap(0, Ordering::Relaxed);
            let tot_val = tot.swap(0, Ordering::Relaxed);
            tracing::info!(
                "📊 10min summary — pumpfun_tokens={} raydium_tokens={} avg_age_seconds={}",
                pf_val,
                ry_val,
                if tot_val > 0 { age_val / tot_val } else { 0 }
            );
        }
    });

    tokio::spawn(async move {
        info!("Filter engine started — waiting for graduated tokens");

        let liquidity = LiquidityFilter::new();
        let age = AgeFilter::new();
        let price_impact = PriceImpactFilter::new();
        let buy_pressure = BuyPressureFilter::new();
        let sanity = SanityFilter::new();

        // Shared RPC clients — created once, reused for every token
        let rpc = RpcClient::new_with_timeout(
            cfg.env.solana_rpc_url.clone(),
            std::time::Duration::from_secs(5),
        );
        let backup_rpc = RpcClient::new_with_timeout(
            cfg.env.solana_rpc_backup_url.clone(),
            std::time::Duration::from_secs(5),
        );

        while let Some(mut token) = detection_rx.recv().await {
            let mint_str = token.mint.to_string();

            let age_seconds = {
                let now_ms = chrono::Utc::now().timestamp_millis();
                (now_ms - token.detected_at) / 1000
            };

            let source_str = match token.source {
                DetectionSource::PumpFun => "pump.fun",
                DetectionSource::Poll => "raydium_logs",
                _ => "unknown",
            };

            tracing::info!(
                mint = %token.mint,
                source = source_str,
                age_seconds = age_seconds,
                buy_pressure = token.buy_pressure_pct,
                unique_buyers = token.unique_buyer_count,
                "🔍 Token entered filter engine"
            );

            match token.source {
                DetectionSource::PumpFun => { pumpfun_count.fetch_add(1, Ordering::Relaxed); }
                DetectionSource::Poll => { raydium_count.fetch_add(1, Ordering::Relaxed); }
                _ => {}
            }

            total_age_sum.fetch_add(age_seconds.max(0) as u64, Ordering::Relaxed);
            total_tokens.fetch_add(1, Ordering::Relaxed);

            info!(
                mint = %token.mint,
                pool = ?token.pool_address,
                buy_pct = token.buy_pressure_pct,
                vol = token.bonding_curve_volume_sol,
                "🔍 Filtering graduated token"
            );

            if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
                warn!(mint = %mint_str, "⚠️ Empty name or symbol — proceeding anyway");
            }

            // ── Sanity pre-check: reject structurally broken tokens early ──
            let sanity_start = std::time::Instant::now();
            let sanity_result = sanity.check(&token, &cfg);
            let sanity_ms = sanity_start.elapsed().as_millis() as u64;
            if !sanity_result.passed {
                warn!(
                    mint = %mint_str,
                    reason = sanity_result.fail_reason.as_deref().unwrap_or("unknown"),
                    "🚫 FAIL — sanity pre-check rejected token"
                );
                // Write pipeline_latency for sanity rejection
                let mut timing = token.pipeline_timing.clone();
                timing.filter_per_check.insert("sanity".to_string(), sanity_ms);
                timing.filter_engine_total_ms = Some(sanity_ms);
                timing.outcome = Some("rejected_filter".to_string());
                timing.rejection_stage = Some("filter_engine".to_string());
                timing.rejection_reason = sanity_result.fail_reason.clone();
                let timing_payload = timing.to_json(&mint_str);
                let supabase_bg = Arc::clone(&supabase);
                tokio::spawn(async move {
                    log_pipeline_latency(&supabase_bg, &timing_payload).await;
                });

                // Track rejected token prices (counterfactual data)
                if let Some(cid) = token.candidate_id {
                    crate::sniper::tracker::spawn_rejected_tracker(
                        Arc::clone(&supabase),
                        cid,
                        mint_str.clone(),
                    );
                }
                continue;
            }

            let filter_start = std::time::Instant::now();

            let age_start = std::time::Instant::now();
            let age_result = age.check(&token, &cfg);
            let age_ms = age_start.elapsed().as_millis() as u64;

            let bp_start = std::time::Instant::now();
            let bp_result = buy_pressure.check(&token, &cfg);
            let bp_ms = bp_start.elapsed().as_millis() as u64;

            // ── FAST GATE: liquidity is async, price impact is instant math ──
            // Note: token_safety (RPC mint/freeze authority check) removed — redundant
            // with sniper enrichment's cross-source mint/freeze validation.
            let liq_start = std::time::Instant::now();
            let (liquidity_result, liquidity_usd) =
                liquidity.check(token.pool_address.as_ref(), &token.mint, token.initial_liquidity_sol, &cfg, &rpc, &backup_rpc).await;
            let liq_ms = liq_start.elapsed().as_millis() as u64;

            // Price impact: estimated from AMM math (zero API calls, instant)
            let impact_start = std::time::Instant::now();
            let impact_result = price_impact.check_from_liquidity(token.initial_liquidity_sol, &cfg);
            let impact_ms = impact_start.elapsed().as_millis() as u64;

            let filter_elapsed = filter_start.elapsed();

            // Populate per-check timing
            token.pipeline_timing.filter_per_check.insert("sanity".to_string(), sanity_ms);
            token.pipeline_timing.filter_per_check.insert("age".to_string(), age_ms);
            token.pipeline_timing.filter_per_check.insert("buy_pressure".to_string(), bp_ms);
            token.pipeline_timing.filter_per_check.insert("liquidity".to_string(), liq_ms);
            token.pipeline_timing.filter_per_check.insert("price_impact".to_string(), impact_ms);
            token.pipeline_timing.filter_engine_total_ms = Some(filter_elapsed.as_millis() as u64);

            tracing::info!(
                mint = %token.mint,
                total_filter_ms = filter_elapsed.as_millis() as u64,
                sanity_ms,
                age_ms,
                bp_ms,
                liq_ms,
                impact_ms,
                "⚡ Fast gate completed"
            );

            let results = vec![
                age_result,
                bp_result,
                liquidity_result,
                impact_result,
            ];

            let summary = FilterSummary::from_results(results);

            // ── Log fast-gate results to Supabase (fire-and-forget) ──
            let supabase_bg = Arc::clone(&supabase);
            let token_bg = token.clone();
            let summary_bg = summary.clone();
            let estimated_impact = if token.initial_liquidity_sol > 0.0 {
                Some((cfg.strategy.execution.buy_amount_sol / token.initial_liquidity_sol) * 100.0)
            } else {
                None
            };
            tokio::spawn(async move {
                log_filter_result(
                    &supabase_bg,
                    &token_bg,
                    &summary_bg,
                    None, // rugcheck_score — runs in post-buy
                    liquidity_usd,
                    None, // market_cap_usd — runs in post-buy
                    estimated_impact,
                    None, // top_10_holder_pct — runs in post-buy,
                ).await;
            });

            if summary.overall_passed {
                info!(
                    mint = %token.mint,
                    liq = liquidity_usd.unwrap_or(0.0),
                    buy_pct = token.buy_pressure_pct,
                    filter_ms = filter_elapsed.as_millis() as u64,
                    "⚡ FAST PASS — sending to execution (slow checks run post-buy)"
                );

                let timing = token.pipeline_timing.clone();
                let filtered = FilteredToken {
                    event: token,
                    filter_summary: summary,
                    market_cap_usd: None,
                    liquidity_usd,
                    rugcheck_score: None,
                    filter_price_usd: None,
                    pipeline_timing: timing,
                };

                if tx.send(filtered).await.is_err() {
                    warn!("Filter → execution channel closed");
                    break;
                }
            } else {
                let failed: Vec<String> = summary
                    .failed_checks()
                    .iter()
                    .map(|r| {
                        format!(
                            "{}:{}",
                            r.check_name,
                            r.fail_reason.as_deref().unwrap_or("unknown")
                        )
                    })
                    .collect();

                warn!(
                    mint = %token.mint,
                    failed = %failed.join(", "),
                    "❌ FAIL — token dropped"
                );

                // Write pipeline_latency for filter rejection
                token.pipeline_timing.outcome = Some("rejected_filter".to_string());
                token.pipeline_timing.rejection_stage = Some("filter_engine".to_string());
                token.pipeline_timing.rejection_reason = Some(failed.join(", "));
                let timing_payload = token.pipeline_timing.to_json(&mint_str);
                let supabase_bg = Arc::clone(&supabase);
                tokio::spawn(async move {
                    log_pipeline_latency(&supabase_bg, &timing_payload).await;
                });

                // Track rejected token prices (counterfactual data)
                if let Some(cid) = token.candidate_id {
                    crate::sniper::tracker::spawn_rejected_tracker(
                        Arc::clone(&supabase),
                        cid,
                        mint_str.clone(),
                    );
                }
            }
        }

        info!("Filter engine shutting down (detection channel closed)");
    });

    rx
}

// ─── Supabase logging helper ─────────────────────────────────

async fn log_filter_result(
    supabase: &SupabaseClient,
    token: &GraduatedToken,
    summary: &FilterSummary,
    rugcheck_score: Option<f64>,
    liquidity_usd: Option<f64>,
    market_cap_usd: Option<f64>,
    estimated_price_impact_pct: Option<f64>,
    top_10_holder_pct: Option<f64>,
) {
    let fail_reasons: Vec<&str> = summary
        .results
        .iter()
        .filter_map(|r| r.fail_reason.as_deref())
        .collect();

    let mint_authority = fail_reasons.iter().any(|r| r.contains("mint_authority_not_revoked"));
    let freeze_authority = fail_reasons.iter().any(|r| r.contains("freeze_authority_not_revoked"));
    let bundled = fail_reasons.iter().any(|r| r.contains("token_is_bundled"));

    let price_impact_pct: Option<f64> = estimated_price_impact_pct;

    let token_age_seconds = token.time_to_graduate_seconds as i64;

    let check_details: Vec<serde_json::Value> = summary
        .results
        .iter()
        .map(|r| {
            serde_json::json!({
                "check_name": r.check_name,
                "passed": r.passed,
                "fail_reason": r.fail_reason,
            })
        })
        .collect();

    let payload = serde_json::json!({
        "mint": token.mint.to_string(),
        "passed": summary.overall_passed,
        "fail_reason": summary
            .failed_checks()
            .iter()
            .filter_map(|r| r.fail_reason.as_deref())
            .collect::<Vec<_>>()
            .join(" | "),
        "rugcheck_score": rugcheck_score,
        "mint_authority": mint_authority,
        "freeze_authority": freeze_authority,
        "bundled": bundled,
        "top_10_holder_pct": top_10_holder_pct,  // ← FIXED: real value now
        "liquidity_usd": liquidity_usd,
        "market_cap_usd": market_cap_usd,
        "price_impact_pct": price_impact_pct,
        "token_age_seconds": token_age_seconds,
        "checked_at": chrono::Utc::now().to_rfc3339(),
        "check_details": serde_json::Value::Array(check_details),
    });

    let url = format!("{}/filter_results", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => { /* ok */ }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            error!(
                mint = %token.mint,
                "Failed to write filter_results: HTTP {} — {}",
                status,
                body
            );
        }
        Err(e) => {
            tracing::error!("Failed to write filter_results: {}", e);
        }
    }
}
