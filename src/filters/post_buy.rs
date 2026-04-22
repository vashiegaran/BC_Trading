//! Post-buy verification: runs slow safety checks AFTER execution.
//!
//! The fast gate lets tokens through with only critical fast checks
//! (sanity, age, buy_pressure, token_safety, price_impact, liquidity).
//! This module runs the expensive checks in the background and sends
//! an emergency exit alert if a critical danger is detected.

use std::sync::Arc;

use solana_client::nonblocking::rpc_client::RpcClient as SolRpcClient;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::logger::SupabaseClient;

use super::goplus::GoPlusFilter;
use super::holders::HoldersFilter;
use super::market_cap::MarketCapFilter;
use super::rpc_fallback;
use super::rugcheck::RugCheckFilter;

/// Alert sent when post-buy verification detects a critical danger.
#[derive(Debug, Clone)]
pub struct PostBuyAlert {
    pub mint: String,
    pub position_id: i64,
    pub reason: String,
    /// Entry price — needed to build an ExitSignal in monitoring.
    pub entry_price_usd: f64,
    pub sol_spent: f64,
    pub token_amount: f64,
    pub is_paper_trade: bool,
}

/// Run all slow verification checks for a token that was already bought.
///
/// This function is spawned as a background task by the execution engine.
/// It runs rugcheck, goplus, smart_wallet, holders, and market_cap checks.
/// If any **critical** check fails, it sends a `PostBuyAlert` through the
/// provided channel so monitoring can trigger an emergency exit.
///
/// Non-critical failures (e.g. high rugcheck score, low holder quality)
/// are logged to Supabase but do NOT trigger an exit — they're informational.
pub async fn verify(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    alert_tx: tokio::sync::mpsc::Sender<PostBuyAlert>,
    mint: String,
    position_id: i64,
    entry_price_usd: f64,
    sol_spent: f64,
    token_amount: f64,
    is_paper_trade: bool,
) {
    let verify_start = std::time::Instant::now();
    info!(mint = %mint, "🔍 Post-buy verification started (background)");

    // Create filter instances (cheap — just HTTP client + empty cache)
    let rugcheck = RugCheckFilter::new();
    let goplus = GoPlusFilter::new();
    let holders = HoldersFilter::new();
    let market_cap = MarketCapFilter::new();

    // Pre-fetch token supply (shared by holders + market_cap)
    let prefetched_supply: Option<f64> = {
        let rpc = SolRpcClient::new_with_timeout(
            cfg.env.solana_rpc_url.clone(),
            std::time::Duration::from_secs(5),
        );
        let mint_pubkey = match solana_sdk::pubkey::Pubkey::from_str(&mint) {
            Ok(pk) => pk,
            Err(_) => {
                warn!(mint = %mint, "post_buy_verify: invalid mint pubkey");
                return;
            }
        };
        let res = rpc.get_token_supply(&mint_pubkey).await;
        let res = if matches!(&res, Err(e) if rpc_fallback::is_rate_limited(e)) {
            let fb = SolRpcClient::new_with_timeout(
                cfg.env.solana_rpc_backup_url.clone(),
                std::time::Duration::from_secs(5),
            );
            fb.get_token_supply(&mint_pubkey).await
        } else {
            res
        };
        res.ok().map(|s| {
            s.ui_amount.unwrap_or_else(|| {
                let raw: f64 = s.amount.parse().unwrap_or(0.0);
                raw / 10_f64.powi(s.decimals as i32)
            })
        }).filter(|&v| v > 0.0)
    };

    let mint_pubkey = match solana_sdk::pubkey::Pubkey::from_str(&mint) {
        Ok(pk) => pk,
        Err(_) => return,
    };

    // Run all slow checks concurrently (smart_wallet disabled — always bails on
    // Token-2022 accounts used by pump.fun tokens, wasting RPC calls for no protection)
    let (
        (rugcheck_result, rugcheck_score, rugcheck_ms),
        (goplus_result, goplus_ms),
        (holders_result, top_10_holder_pct, holders_ms),
        (mcap_result, market_cap_usd, _filter_price, mcap_ms),
    ) = tokio::join!(
        async {
            let t = std::time::Instant::now();
            let r = rugcheck.check(&mint, &cfg).await;
            (r.0, r.1, t.elapsed().as_millis() as u64)
        },
        async {
            let t = std::time::Instant::now();
            let r = goplus.check(&mint, &cfg).await;
            (r, t.elapsed().as_millis() as u64)
        },
        async {
            let t = std::time::Instant::now();
            let r = holders.check(&mint_pubkey, &cfg, prefetched_supply).await;
            (r.0, r.1, t.elapsed().as_millis() as u64)
        },
        async {
            let t = std::time::Instant::now();
            let r = market_cap.check(&mint_pubkey, &cfg, prefetched_supply).await;
            (r.0, r.1, r.2, t.elapsed().as_millis() as u64)
        },
    );

    let elapsed = verify_start.elapsed();
    info!(
        mint = %mint,
        elapsed_ms = elapsed.as_millis() as u64,
        rugcheck_ms,
        goplus_ms,
        holders_ms,
        mcap_ms,
        rugcheck_score = rugcheck_score,
        goplus_passed = goplus_result.passed,
        holders_passed = holders_result.passed,
        top_10 = top_10_holder_pct,
        mcap = market_cap_usd,
        "📋 Post-buy verification complete"
    );

    // Update pipeline_latency with post-buy timing (background PATCH)
    {
        let post_buy_ms = elapsed.as_millis() as u64;
        let post_buy_per_check = serde_json::json!({
            "rugcheck": rugcheck_ms,
            "goplus": goplus_ms,
            "holders": holders_ms,
            "market_cap": mcap_ms,
        });
        let supabase_bg = Arc::clone(&supabase);
        let mint_bg = mint.clone();
        tokio::spawn(async move {
            let patch_url = format!(
                "{}/pipeline_latency?mint=eq.{}&outcome=eq.bought&order=created_at.desc&limit=1",
                supabase_bg.base_url, mint_bg
            );
            let patch_payload = serde_json::json!({
                "post_buy_total_ms": post_buy_ms,
                "post_buy_per_check": post_buy_per_check,
            });
            let _ = supabase_bg.client.patch(&patch_url).json(&patch_payload).send().await;
        });
    }

    // ── Log all results to Supabase (fire-and-forget) ──
    // Clone data needed by both the logging closure AND the post-spawn checks
    let goplus_passed = goplus_result.passed;
    let goplus_fail = goplus_result.fail_reason.clone();
    let holders_passed = holders_result.passed;
    let holders_fail = holders_result.fail_reason.clone();
    let rugcheck_passed = rugcheck_result.passed;
    let rugcheck_fail = rugcheck_result.fail_reason.clone();
    {
        let supabase_bg = Arc::clone(&supabase);
        let mint_bg = mint.clone();
        let goplus_fail_bg = goplus_fail.clone();
        let holders_fail_bg = holders_fail.clone();
        tokio::spawn(async move {
            // Log post-buy verification summary to system_events
            let summary_payload = serde_json::json!({
                "event_type": "post_buy_verification",
                "message": format!(
                    "Mint: {} | rugcheck={} goplus={} holders={} mcap={} | elapsed={}ms",
                    mint_bg,
                    rugcheck_score.map_or("null".to_string(), |s| format!("{:.0}", s)),
                    if goplus_passed { "pass" } else { goplus_fail_bg.as_deref().unwrap_or("fail") },
                    if holders_passed { "pass" } else { holders_fail_bg.as_deref().unwrap_or("fail") },
                    market_cap_usd.map_or("null".to_string(), |m| format!("${:.0}", m)),
                    elapsed.as_millis(),
                ),
            });
            let url = format!("{}/system_events", supabase_bg.base_url);
            let _ = supabase_bg.client.post(&url).json(&summary_payload).send().await;
        });
    }

    // ── PATCH filter_results with slow-check data (fire-and-forget) ──
    // The fast-gate row was inserted pre-buy with None for these columns;
    // fill them in now that rugcheck/holders/market_cap have resolved.
    {
        let supabase_bg = Arc::clone(&supabase);
        let mint_bg = mint.clone();
        tokio::spawn(async move {
            let patch_url = format!(
                "{}/filter_results?mint=eq.{}&order=checked_at.desc&limit=1",
                supabase_bg.base_url, mint_bg
            );
            let patch_payload = serde_json::json!({
                "rugcheck_score": rugcheck_score,
                "top_10_holder_pct": top_10_holder_pct,
                "market_cap_usd": market_cap_usd,
            });
            let _ = supabase_bg.client.patch(&patch_url).json(&patch_payload).send().await;
        });
    }

    // ── Check for CRITICAL failures that warrant emergency exit ──
    // Only truly dangerous signals trigger an exit. Informational
    // failures (high score, suspicious holders) are logged but tolerated.

    // GoPlus critical: honeypot, mintable, transfer_pausable, blacklist, reclaim ownership
    // NOTE: honeypot IS checked here as a safety net. The sniper hard-filters it pre-buy
    // via GoPlus, but if GoPlus timed out during the 2s enrichment window, the pre-buy
    // check was skipped. Post-buy runs with no time pressure so GoPlus almost always succeeds.
    if !goplus_passed {
        if let Some(ref reason) = goplus_fail {
            let is_critical = reason.contains("honeypot")
                || reason.contains("mintable")
                || reason.contains("transfer_pausable")
                || reason.contains("blacklist")
                || reason.contains("reclaim_ownership");

            if is_critical {
                warn!(
                    mint = %mint,
                    reason = %reason,
                    "🚨 POST-BUY CRITICAL: GoPlus detected danger — triggering emergency exit"
                );
                let _ = alert_tx.send(PostBuyAlert {
                    mint: mint.clone(),
                    position_id,
                    reason: format!("goplus_critical: {}", reason),
                    entry_price_usd,
                    sol_spent,
                    token_amount,
                    is_paper_trade,
                }).await;
                return;
            }
        }
    }

    // RugCheck critical: mint authority still active, OR score > 10,000 (extremely dangerous)
    if !rugcheck_passed {
        if let Some(ref reason) = rugcheck_fail {
            let is_authority_critical = reason.contains("mint_authority_not_revoked")
                || reason.contains("freeze_authority_not_revoked");

            if is_authority_critical {
                warn!(
                    mint = %mint,
                    reason = %reason,
                    "🚨 POST-BUY CRITICAL: RugCheck detected authority not revoked — triggering emergency exit"
                );
                let _ = alert_tx.send(PostBuyAlert {
                    mint: mint.clone(),
                    position_id,
                    reason: format!("rugcheck_critical: {}", reason),
                    entry_price_usd,
                    sol_spent,
                    token_amount,
                    is_paper_trade,
                }).await;
                return;
            }
        }
    }

    // RugCheck score > 15,000 = extremely dangerous token
    // Raised from 10K→15K: fresh PumpFun tokens get inflated scores before
    // RugCheck fully indexes them.  NiPjkeGPo1 had score=14,395 but hit $1M mcap.
    if let Some(score) = rugcheck_score {
        if score > 15_000.0 {
            warn!(
                mint = %mint,
                rugcheck_score = score,
                "🚨 POST-BUY CRITICAL: RugCheck score > 15,000 — triggering emergency exit"
            );
            let _ = alert_tx.send(PostBuyAlert {
                mint: mint.clone(),
                position_id,
                reason: format!("rugcheck_critical: score={:.0} > 15000", score),
                entry_price_usd,
                sol_spent,
                token_amount,
                is_paper_trade,
            }).await;
            return;
        }
    }

    // Non-critical rugcheck failures are logged but do NOT trigger exit
    if !rugcheck_passed {
        warn!(
            mint = %mint,
            reason = rugcheck_fail.as_deref().unwrap_or("unknown"),
            rugcheck_score = rugcheck_score,
            "⚠️ Post-buy: RugCheck failed (non-critical — NOT exiting)"
        );
    }

    // Holders: split by failure type based on Phase 3 data analysis
    //   - top10 > 65%: CRITICAL (5/5 confirmed dangerous: 95.7%, 98.2%, 100%, 68.4%, 65.6%)
    //   - single_holder > 25%: WARNING only (over by only 0.1-3.4%, 36% false positive rate)
    //   - min_holder_count: WARNING only
    if !holders_passed {
        let fail_reason = holders_fail.as_deref().unwrap_or("unknown");
        let is_top10_critical = fail_reason.contains("top10_");

        if is_top10_critical {
            warn!(
                mint = %mint,
                reason = fail_reason,
                top_10 = top_10_holder_pct,
                "🚨 POST-BUY CRITICAL: Top-10 holder concentration too high — triggering emergency exit"
            );
            let _ = alert_tx.send(PostBuyAlert {
                mint: mint.clone(),
                position_id,
                reason: format!("holders_critical: {}", fail_reason),
                entry_price_usd,
                sol_spent,
                token_amount,
                is_paper_trade,
            }).await;
            return;
        } else {
            // single_holder or min_holder_count — log as warning, do NOT exit
            warn!(
                mint = %mint,
                reason = fail_reason,
                top_10 = top_10_holder_pct,
                "⚠️ Post-buy: Holders check failed (non-critical — NOT exiting)"
            );
        }
    }

    info!(mint = %mint, "✅ Post-buy verification complete — no critical issues");
}

use std::str::FromStr;
