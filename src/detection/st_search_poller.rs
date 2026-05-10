//! Solana Tracker /search polling — backup detection source.
//!
//! Runs every 20 seconds, queries server-side filtered recently-graduated tokens,
//! and injects any not-yet-seen mints into the detection pipeline.
//! Budget: ~130k requests/month (3 req/min × 1440 min/day × 30 days).

use std::collections::HashSet;
use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::types::{DetectionSource, GraduatedToken, PipelineTiming};
use crate::sniper::solana_tracker::SolanaTrackerClient;

/// Polling interval — 20s catches new tokens fast while staying within
/// the 200k/month Advanced plan budget (~166k total with trade monitoring).
const POLL_INTERVAL_SECS: u64 = 20;

/// Run the search poller loop. Sends discovered tokens to the detection channel.
pub async fn run(tx: mpsc::Sender<GraduatedToken>, api_key: Option<String>) {
    let client = SolanaTrackerClient::new(api_key);
    let mut seen: HashSet<String> = HashSet::new();

    info!(
        "ST search poller started (interval: {}s)",
        POLL_INTERVAL_SECS
    );

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS)).await;

        let results = match client.search_graduated(1500.0, 65.0, 30.0).await {
            Some(r) => r,
            None => {
                debug!("ST search returned no results");
                continue;
            }
        };

        let mut new_count = 0;
        for result in results {
            if seen.contains(&result.mint) {
                continue;
            }
            seen.insert(result.mint.clone());

            // Parse mint pubkey
            let mint = match Pubkey::from_str(&result.mint) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let creator = result
                .deployer
                .as_deref()
                .and_then(|d| Pubkey::from_str(d).ok())
                .unwrap_or_default();

            let pool = result
                .pool_address
                .as_deref()
                .and_then(|p| Pubkey::from_str(p).ok());

            let now_ms = chrono::Utc::now().timestamp_millis();

            let token = GraduatedToken {
                mint,
                pool_address: pool,
                creator_wallet: creator,
                bonding_curve_volume_sol: 0.0,
                buy_pressure_pct: 0.0,
                time_to_graduate_seconds: 0.0,
                detected_at: now_ms,
                source: DetectionSource::SolanaTrackerSearch,
                unique_buyer_count: 0,
                buy_count: result.buys,
                sell_count: result.sells,
                trade_timestamps: Vec::new(),
                name: result.name,
                symbol: result.symbol,
                initial_liquidity_sol: result.liquidity_usd / 150.0, // rough estimate
                creator_rebuy: false,
                buy_sell_ratio: if result.sells > 0 {
                    result.buys as f64 / result.sells as f64
                } else {
                    result.buys as f64
                },
                narrative_cluster: None,
                candidate_id: None,
                sniper_features: None,
                sniper_score: None,
                pipeline_timing: PipelineTiming::new(now_ms),
            };

            if tx.send(token).await.is_err() {
                warn!("ST search poller: detection channel closed");
                return;
            }
            new_count += 1;
        }

        if new_count > 0 {
            info!(
                new = new_count,
                seen = seen.len(),
                "ST search discovered new tokens"
            );
        }

        // Cap seen set to prevent unbounded growth (keep last 5000)
        if seen.len() > 5000 {
            let drain_count = seen.len() - 2500;
            let to_remove: Vec<String> = seen.iter().take(drain_count).cloned().collect();
            for key in to_remove {
                seen.remove(&key);
            }
        }
    }
}
