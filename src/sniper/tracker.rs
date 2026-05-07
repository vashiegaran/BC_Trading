//! Rejected token price tracker — counterfactual data for scoring model.
//!
//! For every token we DON'T buy (hard-filtered or position-full),
//! track what happened anyway by checking price at 1min, 5min, 15min, 1hr.
//!
//! On bot startup, `recover_pending_trackers()` re-spawns trackers for any
//! `sniper_candidates` rows less than 1 hour old that still have NULL price
//! columns — so restarts no longer lose in-flight tracking tasks.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::logger::SupabaseClient;

/// Schedule async price checks for a rejected/skipped token.
/// Spawns as a fire-and-forget background task.
pub fn spawn_rejected_tracker(
    supabase: Arc<SupabaseClient>,
    candidate_id: i64,
    mint: String,
) {
    tokio::spawn(async move {
        track_rejected_prices(&supabase, candidate_id, &mint, 0).await;
    });
}

/// Recover trackers for candidates that were interrupted by a restart.
/// Called once at startup. Finds rows < 1hr old with NULL price_1h and
/// re-spawns trackers with adjusted delays based on elapsed time.
pub async fn recover_pending_trackers(supabase: Arc<SupabaseClient>) {
    let url = format!("{}/sniper_candidates", supabase.base_url);
    let cutoff = chrono::Utc::now()
        .checked_sub_signed(chrono::Duration::hours(1))
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();
    let created_at_filter = format!("gte.{}", cutoff);

    let resp = match supabase.client
        .get(&url)
        .query(&[
            ("price_1h", "is.null"),
            ("created_at", created_at_filter.as_str()),
            ("select", "id,mint,created_at"),
            ("limit", "100"),
        ])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("Tracker recovery: failed to query sniper_candidates: {}", e);
            return;
        }
    };

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        warn!(status = %status, body = %body, "Tracker recovery: sniper_candidates query failed");
        return;
    }

    let rows: Vec<serde_json::Value> = match resp.json().await {
        Ok(r) => r,
        Err(e) => {
            warn!("Tracker recovery: failed to parse response: {}", e);
            return;
        }
    };

    if rows.is_empty() {
        info!("Tracker recovery: no pending candidates to recover");
        return;
    }

    let now = chrono::Utc::now();
    let mut recovered = 0u32;

    for row in &rows {
        let id = match row.get("id").and_then(|v| v.as_i64()) {
            Some(id) => id,
            None => continue,
        };
        let mint = match row.get("mint").and_then(|v| v.as_str()) {
            Some(m) => m.to_string(),
            None => continue,
        };
        let created_str = match row.get("created_at").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let created = match chrono::DateTime::parse_from_rfc3339(created_str) {
            Ok(dt) => dt.with_timezone(&chrono::Utc),
            Err(_) => continue,
        };
        let elapsed_secs = (now - created).num_seconds().max(0) as u64;

        let sb = Arc::clone(&supabase);
        tokio::spawn(async move {
            track_rejected_prices(&sb, id, &mint, elapsed_secs).await;
        });
        recovered += 1;
    }

    info!(
        recovered,
        total_pending = rows.len(),
        "Tracker recovery: re-spawned trackers for pending candidates"
    );
}

/// All check intervals (cumulative seconds from detection time).
const INTERVALS: [(u64, &str); 4] = [
    (60, "price_1m"),
    (300, "price_5m"),
    (900, "price_15m"),
    (3600, "price_1h"),
];

/// Check prices at 1min, 5min, 15min, 1hr after detection.
/// `elapsed_secs` is how much time has already passed (0 for fresh, >0 for recovery).
/// Updates the sniper_candidates row with counterfactual price data.
async fn track_rejected_prices(
    supabase: &SupabaseClient,
    candidate_id: i64,
    mint: &str,
    elapsed_secs: u64,
) {
    let mut peak_price: f64 = 0.0;

    // Fetch entry price with retry — freshly graduated tokens often aren't on
    // DexScreener immediately. Try up to 3 times with 30s gaps.
    let mut entry_price = 0.0_f64;
    for attempt in 0..3 {
        if let Some(p) = fetch_dexscreener_price(mint).await {
            if p > 0.0 {
                entry_price = p;
                break;
            }
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    for (target_secs, column) in &INTERVALS {
        if elapsed_secs >= *target_secs {
            // This interval already passed (recovery case) — skip
            continue;
        }

        let remaining = target_secs - elapsed_secs;
        tokio::time::sleep(Duration::from_secs(remaining)).await;

        let price = match fetch_dexscreener_price(mint).await {
            Some(p) if p > 0.0 => p,
            _ => {
                debug!(mint = %mint, column = column, "Price fetch failed for rejected token");
                continue;
            }
        };

        if price > peak_price {
            peak_price = price;
        }

        // Update price column in sniper_candidates
        let url = format!("{}/sniper_candidates?id=eq.{}", supabase.base_url, candidate_id);
        let mut payload = serde_json::Map::new();
        payload.insert(column.to_string(), serde_json::json!(price));
        let _ = supabase.client.patch(&url).json(&serde_json::Value::Object(payload)).send().await;

        debug!(
            mint = %mint,
            column = column,
            price = price,
            "Rejected token price recorded"
        );
    }

    // Update peak price and multiplier
    if peak_price > 0.0 && entry_price > 0.0 {
        let peak_multiplier = peak_price / entry_price;
        let url = format!("{}/sniper_candidates?id=eq.{}", supabase.base_url, candidate_id);
        let payload = serde_json::json!({
            "peak_multiplier": peak_multiplier,
        });
        let _ = supabase.client.patch(&url).json(&payload).send().await;
    }
}

/// Simple DexScreener price fetch for counterfactual tracking.
async fn fetch_dexscreener_price(mint: &str) -> Option<f64> {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", mint);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;

    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let json: serde_json::Value = resp.json().await.ok()?;
    json.get("pairs")
        .and_then(|p| p.as_array())
        .and_then(|arr| arr.first())
        .and_then(|pair| pair.get("priceUsd"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
}
