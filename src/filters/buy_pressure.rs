use std::collections::{HashMap, HashSet};

use tracing::{debug, warn};

use crate::config::AppConfig;
use crate::detection::types::GraduatedToken;
use super::types::FilterResult;

const CHECK_NAME: &str = "buy_pressure";

pub struct BuyPressureFilter;

impl BuyPressureFilter {
    pub fn new() -> Self {
        Self
    }

    /// Evaluate bonding-curve metrics from the graduated token.
    ///
    /// This is a **synchronous** check — all data is already in the
    /// `GraduatedToken` struct from the detection engine.
    ///
    /// Checks:
    /// 1. Buy pressure percentage above minimum
    /// 2. Bonding curve volume above minimum
    /// 3. Sufficient unique buyer wallets (wash-trade detection)
    /// 4. No coordinated buy timing patterns (wash-trade detection)
    pub fn check(&self, token: &GraduatedToken, cfg: &AppConfig) -> FilterResult {
        let filters = &cfg.strategy.filters;
        let min_buy_pressure_pct = filters.min_buy_pressure_pct;
        let min_bonding_volume_sol = filters.min_bonding_volume_sol;
        let min_unique_buyers = filters.min_unique_buyers;
        let coordinated_window_ms = filters.coordinated_window_ms;
        let coordinated_buy_threshold = filters.coordinated_buy_threshold;

        let total_trades = token.buy_count + token.sell_count;

        // ── Timing log: how long since token was first seen ──
        let watchlist_age_ms = {
            let now_ms = chrono::Utc::now().timestamp_millis();
            (now_ms - token.detected_at).max(0) as u128
        };

        tracing::info!(
            mint = %token.mint,
            watchlist_age_ms = watchlist_age_ms as u64,
            buy_count = token.buy_count,
            sell_count = token.sell_count,
            "Buy pressure filter running"
        );

        debug!(
            mint = %token.mint,
            buy_count = token.buy_count,
            sell_count = token.sell_count,
            total_trades = total_trades,
            "Buy pressure raw data"
        );

        // If no trades recorded yet, skip this filter.
        // Token arrived before watchlist collected trade data.
        if total_trades == 0 {
            warn!(
                mint = %token.mint,
                "Buy pressure skipped — no trade data yet \
                 (token arrived before watchlist populated)"
            );
            return FilterResult::pass(CHECK_NAME);
        }

        let min_trades = 5u64; // minimum meaningful sample
        if total_trades < min_trades {
            tracing::warn!(
                mint = %token.mint,
                total_trades,
                "Buy pressure: insufficient trades for meaningful signal, skipping"
            );
            return FilterResult::pass(CHECK_NAME);
        }

        debug!(
            mint = %token.mint,
            buy_pressure_pct = token.buy_pressure_pct,
            bonding_curve_volume_sol = token.bonding_curve_volume_sol,
            unique_buyers = token.unique_buyer_count,
            "buy_pressure check"
        );

        if token.buy_pressure_pct < min_buy_pressure_pct {
            return FilterResult::fail(
                CHECK_NAME,
                &format!(
                    "buy_pressure_{:.1}pct_below_min_{:.1}pct",
                    token.buy_pressure_pct, min_buy_pressure_pct
                ),
            );
        }

        if token.bonding_curve_volume_sol < min_bonding_volume_sol {
            return FilterResult::fail(
                CHECK_NAME,
                &format!(
                    "volume_{:.2}sol_below_min_{:.2}sol",
                    token.bonding_curve_volume_sol, min_bonding_volume_sol
                ),
            );
        }

        // ── Wash-trade detection: unique buyers ──────────
        if token.unique_buyer_count < min_unique_buyers {
            return FilterResult::fail(
                CHECK_NAME,
                &format!(
                    "insufficient_unique_buyers: {} < {}",
                    token.unique_buyer_count, min_unique_buyers
                ),
            );
        }

        // ── Wash-trade detection: coordinated buy timing ─
        if let Some(reason) = detect_coordinated_buys(
            &token.trade_timestamps,
            coordinated_window_ms,
            coordinated_buy_threshold,
        ) {
            return FilterResult::fail(CHECK_NAME, &reason);
        }

        FilterResult::pass(CHECK_NAME)
    }
}

/// Detect coordinated buy patterns by grouping trades into 2-second windows.
///
/// If any single window contains > 5 buys AND all those buyers are new
/// wallets (only appear once in the entire trade history), flag as
/// coordinated launch.
fn detect_coordinated_buys(
    trade_timestamps: &[(i64, solana_sdk::pubkey::Pubkey)],
    coordinated_window_ms: i64,
    coordinated_buy_threshold: usize,
) -> Option<String> {
    if trade_timestamps.is_empty() {
        return None;
    }

    // Count how many times each wallet appears across ALL trades
    let mut wallet_trade_count: HashMap<solana_sdk::pubkey::Pubkey, usize> = HashMap::new();
    for (_, wallet) in trade_timestamps {
        *wallet_trade_count.entry(*wallet).or_insert(0) += 1;
    }

    // Sort timestamps for windowing
    let mut sorted: Vec<(i64, solana_sdk::pubkey::Pubkey)> = trade_timestamps.to_vec();
    sorted.sort_by_key(|(ts, _)| *ts);

    // Sliding window: group trades into 2-second buckets
    let mut window_start = 0;
    for window_end in 0..sorted.len() {
        // Shrink window from left while it exceeds 2 seconds
        while sorted[window_end].0 - sorted[window_start].0 > coordinated_window_ms {
            window_start += 1;
        }

        let window_size = window_end - window_start + 1;
        if window_size > coordinated_buy_threshold {
            // Check if all wallets in this window are "new" (appear only once overall)
            let window_wallets: Vec<&solana_sdk::pubkey::Pubkey> = sorted[window_start..=window_end]
                .iter()
                .map(|(_, w)| w)
                .collect();

            let unique_in_window: HashSet<&solana_sdk::pubkey::Pubkey> =
                window_wallets.iter().copied().collect();

            let all_new = unique_in_window
                .iter()
                .all(|w| wallet_trade_count.get(w).copied().unwrap_or(0) <= 1);

            if all_new {
                warn!(
                    buys_in_window = window_size,
                    window_ms = coordinated_window_ms,
                    "Coordinated buy pattern detected: {} new wallets in {}ms window",
                    window_size
                    ,
                    coordinated_window_ms
                );
                return Some(format!(
                    "coordinated_buy_detected: {} new wallets in {}ms window",
                    window_size,
                    coordinated_window_ms
                ));
            }
        }
    }

    None
}
