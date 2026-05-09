//! Solana Tracker /trades pattern monitor.
//!
//! Runs as a parallel watcher alongside the main monitoring loop.
//! Polls /trades/{mint} every 15 seconds and checks 6 patterns in
//! **observe-only** mode — patterns are detected and logged (+ written
//! as snapshots to Supabase `st_trade_snapshots`) but do NOT trigger exits.
//!
//! 1. **Whale Dump**: single sell > 5 SOL
//! 2. **Sell Pressure**: >70% sell volume in 5-minute window
//! 3. **Buyers Fading**: avg buy size drops >70% from peak window
//! 4. **Stealth Dump**: same wallet sells 3+ times in 2 minutes
//! 5. **Dead Interest**: <2 unique buyers for 3 consecutive polls
//! 6. **Volume Cliff**: total volume drops >80% from peak
//!
//! Budget: ~30k requests/month (~2 active positions × 4 polls/min × 30 days).
//! Combined with search poller (~130k), total ST usage ~166k of 200k/month.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use crate::logger::SupabaseClient;
use crate::sniper::solana_tracker::SolanaTrackerClient;

const POLL_INTERVAL_SECS: u64 = 15;

// ── Pattern thresholds ──
const WHALE_DUMP_SOL: f64 = 5.0;
const SELL_PRESSURE_THRESHOLD: f64 = 0.70;
const MIN_VOLUME_FOR_PRESSURE: f64 = 2.0;
/// Avg buy size must drop this much from peak to trigger BuyersFading
const BUYERS_FADING_DROP_PCT: f64 = 0.70;
/// Same wallet selling this many times in 2min = stealth dump
const STEALTH_DUMP_COUNT: usize = 3;
const STEALTH_DUMP_WINDOW_MS: i64 = 120_000;
/// Below this many unique buyers per poll = dead interest
const DEAD_INTEREST_MIN_BUYERS: usize = 2;
/// This many consecutive low-buyer polls triggers exit
const DEAD_INTEREST_CONSECUTIVE: u32 = 3;
/// Volume must drop this much from peak to trigger VolumeCliff
const VOLUME_CLIFF_DROP_PCT: f64 = 0.80;
/// Minimum polls before pattern detection kicks in (avoid false signals at start)
const MIN_POLLS_BEFORE_PATTERNS: u32 = 4;

/// Monitor trades for a position — **observe-only**.
///
/// Patterns are detected and logged but do NOT fire exit signals.
/// Each poll writes a snapshot to the `st_trade_snapshots` Supabase table.
pub async fn watch_trades(
    api_key: Option<String>,
    mint: String,
    position_id: i64,
    supabase: Arc<SupabaseClient>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let client = SolanaTrackerClient::new(api_key);

    // ── Tracking state across polls ──
    let mut peak_avg_buy_sol: f64 = 0.0;
    let mut peak_volume_sol: f64 = 0.0;
    let mut consecutive_low_buyer_polls: u32 = 0;
    let mut poll_count: u32 = 0;

    debug!(mint = %mint, "ST trades watcher started (6 patterns, observe-only)");

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)) => {}
            _ = shutdown_rx.changed() => {
                debug!(mint = %mint, "ST trades watcher shutting down");
                return;
            }
        }

        let trades = match client.fetch_trades(&mint).await {
            Some(t) => t,
            None => continue,
        };

        if trades.is_empty() {
            continue;
        }

        poll_count += 1;

        // Only look at trades from the last 5 minutes
        let cutoff_ms = chrono::Utc::now().timestamp_millis() - 300_000;
        let recent: Vec<_> = trades.iter().filter(|t| t.time_ms >= cutoff_ms).collect();

        if recent.is_empty() {
            consecutive_low_buyer_polls += 1;
            if poll_count >= MIN_POLLS_BEFORE_PATTERNS
                && consecutive_low_buyer_polls >= DEAD_INTEREST_CONSECUTIVE
            {
                warn!(
                    mint = %mint,
                    consecutive_polls = consecutive_low_buyer_polls,
                    "💀 [OBSERVE] DEAD INTEREST — no trades for {}+ polls",
                    DEAD_INTEREST_CONSECUTIVE,
                );
            }
            // Write empty snapshot
            write_snapshot(
                &supabase,
                position_id,
                &mint,
                poll_count,
                0,
                0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0,
                0,
                &if consecutive_low_buyer_polls >= DEAD_INTEREST_CONSECUTIVE {
                    vec!["DeadInterest".to_string()]
                } else {
                    vec![]
                },
            )
            .await;
            continue;
        }

        let buys: Vec<_> = recent.iter().filter(|t| t.trade_type == "buy").collect();
        let sells: Vec<_> = recent.iter().filter(|t| t.trade_type == "sell").collect();
        let total_buy_vol: f64 = buys.iter().map(|t| t.volume_sol).sum();
        let total_sell_vol: f64 = sells.iter().map(|t| t.volume_sol).sum();
        let total_vol = total_buy_vol + total_sell_vol;

        // Update peak tracking
        let avg_buy = if buys.is_empty() {
            0.0
        } else {
            total_buy_vol / buys.len() as f64
        };
        if avg_buy > peak_avg_buy_sol {
            peak_avg_buy_sol = avg_buy;
        }
        if total_vol > peak_volume_sol {
            peak_volume_sol = total_vol;
        }

        // Count unique buyers this poll
        let unique_buyers: std::collections::HashSet<&str> =
            buys.iter().map(|t| t.wallet.as_str()).collect();

        debug!(
            mint = %mint,
            poll = poll_count,
            recent_trades = recent.len(),
            buys = buys.len(),
            sells = sells.len(),
            avg_buy = format!("{:.3}", avg_buy),
            peak_avg_buy = format!("{:.3}", peak_avg_buy_sol),
            vol = format!("{:.2}", total_vol),
            peak_vol = format!("{:.2}", peak_volume_sol),
            unique_buyers = unique_buyers.len(),
            "ST trades pattern check"
        );

        // ── Accumulate detected patterns this poll ──
        let mut patterns_detected: Vec<String> = Vec::new();

        // ── Pattern 1: Whale Dump (single large sell) ──
        if let Some(whale) = sells.iter().find(|t| t.volume_sol >= WHALE_DUMP_SOL) {
            warn!(
                mint = %mint,
                wallet = %whale.wallet,
                volume_sol = format!("{:.2}", whale.volume_sol),
                "🐋 [OBSERVE] WHALE DUMP — single sell > {} SOL", WHALE_DUMP_SOL
            );
            patterns_detected.push(format!(
                "WhaleDump({:.2}sol,{})",
                whale.volume_sol,
                &whale.wallet[..8.min(whale.wallet.len())]
            ));
        }

        // ── Pattern 2: Sell Pressure (>70% sell volume) ──
        if total_vol >= MIN_VOLUME_FOR_PRESSURE {
            let sell_ratio = total_sell_vol / total_vol;
            if sell_ratio >= SELL_PRESSURE_THRESHOLD {
                warn!(
                    mint = %mint,
                    sell_ratio = format!("{:.2}", sell_ratio),
                    sell_vol = format!("{:.2}", total_sell_vol),
                    "📉 [OBSERVE] SELL PRESSURE — {:.0}% sell volume", sell_ratio * 100.0
                );
                patterns_detected.push(format!("SellPressure({:.0}%)", sell_ratio * 100.0));
            }
        }

        // Patterns 3-6 require enough data
        if poll_count >= MIN_POLLS_BEFORE_PATTERNS {
            // ── Pattern 3: Buyers Fading (avg buy size dropping) ──
            if peak_avg_buy_sol > 0.01 && avg_buy > 0.0 {
                let drop_pct = 1.0 - (avg_buy / peak_avg_buy_sol);
                if drop_pct >= BUYERS_FADING_DROP_PCT {
                    warn!(
                        mint = %mint,
                        avg_buy = format!("{:.4}", avg_buy),
                        peak_avg = format!("{:.4}", peak_avg_buy_sol),
                        drop = format!("{:.0}%", drop_pct * 100.0),
                        "📉 [OBSERVE] BUYERS FADING — avg buy size dropped {:.0}% from peak", drop_pct * 100.0
                    );
                    patterns_detected.push(format!("BuyersFading({:.0}%)", drop_pct * 100.0));
                }
            }

            // ── Pattern 4: Stealth Dump (same wallet, 3+ sells in 2 min) ──
            {
                let two_min_ago = chrono::Utc::now().timestamp_millis() - STEALTH_DUMP_WINDOW_MS;
                let recent_sells: Vec<_> =
                    sells.iter().filter(|t| t.time_ms >= two_min_ago).collect();

                let mut wallet_sell_counts: HashMap<&str, usize> = HashMap::new();
                for s in &recent_sells {
                    *wallet_sell_counts.entry(s.wallet.as_str()).or_default() += 1;
                }

                if let Some((wallet, count)) = wallet_sell_counts
                    .iter()
                    .find(|(_, &c)| c >= STEALTH_DUMP_COUNT)
                {
                    let stealth_vol: f64 = recent_sells
                        .iter()
                        .filter(|t| t.wallet.as_str() == *wallet)
                        .map(|t| t.volume_sol)
                        .sum();
                    warn!(
                        mint = %mint,
                        wallet = %wallet,
                        sell_count = count,
                        total_vol = format!("{:.2}", stealth_vol),
                        "🥷 [OBSERVE] STEALTH DUMP — {} sells in 2min from same wallet", count
                    );
                    patterns_detected
                        .push(format!("StealthDump({}sells,{:.2}sol)", count, stealth_vol));
                }
            }

            // ── Pattern 5: Dead Interest (<2 unique buyers, 3 consecutive polls) ──
            if unique_buyers.len() < DEAD_INTEREST_MIN_BUYERS {
                consecutive_low_buyer_polls += 1;
                if consecutive_low_buyer_polls >= DEAD_INTEREST_CONSECUTIVE {
                    warn!(
                        mint = %mint,
                        unique_buyers = unique_buyers.len(),
                        consecutive_polls = consecutive_low_buyer_polls,
                        "💀 [OBSERVE] DEAD INTEREST — <{} unique buyers for {} polls",
                        DEAD_INTEREST_MIN_BUYERS, DEAD_INTEREST_CONSECUTIVE,
                    );
                    patterns_detected.push(format!(
                        "DeadInterest({}polls)",
                        consecutive_low_buyer_polls
                    ));
                }
            } else {
                consecutive_low_buyer_polls = 0;
            }

            // ── Pattern 6: Volume Cliff (volume drops >80% from peak) ──
            if peak_volume_sol > MIN_VOLUME_FOR_PRESSURE && total_vol > 0.0 {
                let vol_drop_pct = 1.0 - (total_vol / peak_volume_sol);
                if vol_drop_pct >= VOLUME_CLIFF_DROP_PCT {
                    warn!(
                        mint = %mint,
                        current_vol = format!("{:.2}", total_vol),
                        peak_vol = format!("{:.2}", peak_volume_sol),
                        drop = format!("{:.0}%", vol_drop_pct * 100.0),
                        "📉 [OBSERVE] VOLUME CLIFF — volume dropped {:.0}% from peak", vol_drop_pct * 100.0
                    );
                    patterns_detected.push(format!("VolumeCliff({:.0}%)", vol_drop_pct * 100.0));
                }
            }
        } // end MIN_POLLS_BEFORE_PATTERNS gate

        // ── Compute max single values for snapshot ──
        let max_buy_sol = buys.iter().map(|t| t.volume_sol).fold(0.0_f64, f64::max);
        let max_sell_sol = sells.iter().map(|t| t.volume_sol).fold(0.0_f64, f64::max);
        let avg_sell = if sells.is_empty() {
            0.0
        } else {
            total_sell_vol / sells.len() as f64
        };
        let unique_sellers: std::collections::HashSet<&str> =
            sells.iter().map(|t| t.wallet.as_str()).collect();

        // ── Write snapshot to Supabase ──
        write_snapshot(
            &supabase,
            position_id,
            &mint,
            poll_count,
            buys.len(),
            sells.len(),
            total_buy_vol,
            total_sell_vol,
            avg_buy,
            max_buy_sol,
            avg_sell,
            max_sell_sol,
            unique_buyers.len(),
            unique_sellers.len(),
            &patterns_detected,
        )
        .await;
    }
}

/// Write a trade snapshot to the `st_trade_snapshots` Supabase table.
async fn write_snapshot(
    supabase: &SupabaseClient,
    position_id: i64,
    mint: &str,
    poll_number: u32,
    buy_count: usize,
    sell_count: usize,
    total_buy_sol: f64,
    total_sell_sol: f64,
    avg_buy_sol: f64,
    max_buy_sol: f64,
    avg_sell_sol: f64,
    max_sell_sol: f64,
    unique_buyers: usize,
    unique_sellers: usize,
    patterns_detected: &[String],
) {
    let payload = serde_json::json!({
        "position_id": position_id,
        "mint": mint,
        "poll_number": poll_number,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "total_buy_sol": total_buy_sol,
        "total_sell_sol": total_sell_sol,
        "avg_buy_sol": avg_buy_sol,
        "max_buy_sol": max_buy_sol,
        "avg_sell_sol": avg_sell_sol,
        "max_sell_sol": max_sell_sol,
        "unique_buyers": unique_buyers,
        "unique_sellers": unique_sellers,
        "patterns_detected": patterns_detected,
    });

    let url = format!("{}/st_trade_snapshots", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            debug!(mint = %mint, poll = poll_number, "ST trade snapshot written");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "ST trade snapshot write failed: {}", body);
        }
        Err(e) => {
            warn!(mint = %mint, "ST trade snapshot write error: {}", e);
        }
    }
}
