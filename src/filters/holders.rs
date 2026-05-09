use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, warn};

use super::rpc_fallback::is_rate_limited;
use super::types::FilterResult;
use crate::config::AppConfig;

const CHECK_NAME: &str = "holders";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub struct HoldersFilter;

impl HoldersFilter {
    pub fn new() -> Self {
        Self
    }

    pub async fn check(
        &self,
        mint: &Pubkey,
        cfg: &AppConfig,
        prefetched_supply: Option<f64>,
    ) -> (FilterResult, Option<f64>) {
        let rpc = RpcClient::new_with_timeout(cfg.env.solana_rpc_url.clone(), REQUEST_TIMEOUT);

        // ── Resolve total supply (use pre-fetched value when available) ──
        let total_ui = if let Some(supply) = prefetched_supply {
            supply
        } else {
            let supply_result = rpc.get_token_supply(mint).await;
            let supply_result = if matches!(&supply_result, Err(e) if is_rate_limited(e)) {
                warn!(mint = %mint, "QuickNode rate limited, using fallback RPC");
                let fallback = RpcClient::new_with_timeout(
                    cfg.env.solana_rpc_backup_url.clone(),
                    REQUEST_TIMEOUT,
                );
                fallback.get_token_supply(mint).await
            } else {
                supply_result
            };

            let total_supply = match supply_result {
                Ok(supply) => supply,
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("-32602") || err_str.contains("could not find account") {
                        warn!(mint = %mint, "supply fetch failed: account not found, skipping top_holders filter");
                        return (FilterResult::pass(CHECK_NAME), None);
                    }
                    warn!(mint = %mint, "holders: supply fetch failed: {}", e);
                    return (
                        FilterResult::fail(CHECK_NAME, &format!("supply_fetch_failed: {}", e)),
                        None,
                    );
                }
            };

            match total_supply.ui_amount {
                Some(amt) if amt > 0.0 => amt,
                _ => {
                    let raw: f64 = total_supply.amount.parse().unwrap_or(0.0);
                    let d = total_supply.decimals as i32;
                    if d > 0 {
                        raw / 10_f64.powi(d)
                    } else {
                        raw
                    }
                }
            }
        };

        if total_ui <= 0.0 {
            return (FilterResult::fail(CHECK_NAME, "zero_total_supply"), None);
        }

        // ── Fetch largest holders ──
        let largest_result = rpc.get_token_largest_accounts(mint).await;
        let largest_result = if matches!(&largest_result, Err(e) if is_rate_limited(e)) {
            warn!(mint = %mint, "QuickNode rate limited, using fallback RPC");
            let fallback =
                RpcClient::new_with_timeout(cfg.env.solana_rpc_backup_url.clone(), REQUEST_TIMEOUT);
            fallback.get_token_largest_accounts(mint).await
        } else {
            largest_result
        };

        let largest = match largest_result {
            Ok(accounts) => accounts,
            Err(e) => {
                warn!(mint = %mint, "holders: largest accounts fetch failed: {}", e);
                return (
                    FilterResult::fail(
                        CHECK_NAME,
                        &format!("largest_accounts_fetch_failed: {}", e),
                    ),
                    None,
                );
            }
        };

        let mut holder_pcts: Vec<f64> = Vec::with_capacity(largest.len());

        for acct in &largest {
            let holder_ui = acct.amount.ui_amount.unwrap_or_else(|| {
                let raw: f64 = acct.amount.amount.parse().unwrap_or(0.0);
                let d = acct.amount.decimals as i32;
                if d > 0 {
                    raw / 10_f64.powi(d)
                } else {
                    raw
                }
            });
            let pct = (holder_ui / total_ui) * 100.0;
            holder_pcts.push(pct);
        }

        let top_10_sum: f64 = holder_pcts.iter().take(10).sum();
        let max_top = cfg.strategy.filters.max_top_holder_pct;

        debug!(
            mint = %mint,
            top_10_sum,
            max_top,
            "holders: concentration check"
        );

        if top_10_sum > max_top {
            return (
                FilterResult::fail(
                    CHECK_NAME,
                    &format!("top10_{:.1}pct_exceeds_max_{:.1}pct", top_10_sum, max_top),
                ),
                Some(top_10_sum), // ← still return the value even on fail
            );
        }

        // ── Holder count floor: reject if too few distinct holders ──
        let total_holders = largest.len();
        let min_holders = cfg.strategy.filters.min_holder_count;
        if total_holders < min_holders {
            info!(
                mint = %mint,
                holders = total_holders,
                min = min_holders,
                "🚫 holders: only {} holders (need {})",
                total_holders,
                min_holders
            );
            return (
                FilterResult::fail(
                    CHECK_NAME,
                    &format!("too_few_holders: {} < {}", total_holders, min_holders),
                ),
                Some(top_10_sum),
            );
        }

        // ── Single-wallet dominance: reject if any one wallet holds too much ──
        let max_single = cfg.strategy.filters.max_single_holder_pct;
        if max_single > 0.0 {
            if let Some((idx, &biggest_pct)) = holder_pcts
                .iter()
                .enumerate()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            {
                if biggest_pct > max_single {
                    info!(
                        mint = %mint,
                        biggest_holder_pct = format!("{:.1}", biggest_pct),
                        max_single = format!("{:.1}", max_single),
                        holder_rank = idx + 1,
                        "🚫 holders: single wallet holds {:.1}% (max {:.1}%)",
                        biggest_pct,
                        max_single
                    );
                    return (
                        FilterResult::fail(
                            CHECK_NAME,
                            &format!(
                                "single_holder_{:.1}pct_exceeds_max_{:.1}pct",
                                biggest_pct, max_single
                            ),
                        ),
                        Some(top_10_sum),
                    );
                }
            }
        }

        (FilterResult::pass(CHECK_NAME), Some(top_10_sum))
    }
}
