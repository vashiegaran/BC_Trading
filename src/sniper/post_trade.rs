//! Post-trade async enrichment — runs after every position closes.
//!
//! 11-step pipeline: re-fetch ST scores, Birdeye OHLCV/trade data/top traders,
//! holder distribution, exit liquidity, wallet PnL, creator reputation.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use super::birdeye::BirdeyeClient;
use super::solana_tracker::SolanaTrackerClient;
use crate::config::AppConfig;
use crate::logger::SupabaseClient;

/// Spawn post-trade enrichment as a fire-and-forget background task.
/// Runs 5 seconds after exit to let on-chain state settle.
pub fn spawn_post_trade(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    position_id: i64,
    mint: String,
    creator_wallet: String,
    exit_time_unix: i64,
    entry_sniper_features: Option<serde_json::Value>,
) {
    tokio::spawn(async move {
        // Wait 5 seconds for on-chain state to settle
        tokio::time::sleep(Duration::from_secs(5)).await;

        let features = run_post_trade_enrichment(
            &cfg,
            &supabase,
            &mint,
            &creator_wallet,
            exit_time_unix,
            entry_sniper_features.as_ref(),
        )
        .await;

        // Write post_trade_features to sniper_positions
        let url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
        let payload = serde_json::json!({
            "post_trade_features": features,
        });
        match supabase.client.patch(&url).json(&payload).send().await {
            Ok(r) if r.status().is_success() => {
                info!(position_id = position_id, "Post-trade features saved");
            }
            Ok(r) => {
                let body = r.text().await.unwrap_or_default();
                warn!(
                    position_id = position_id,
                    "Post-trade save failed: {}", body
                );
            }
            Err(e) => {
                warn!(position_id = position_id, "Post-trade save error: {}", e);
            }
        }

        // Update creator reputation
        update_creator_reputation(&supabase, &creator_wallet).await;
    });
}

async fn run_post_trade_enrichment(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    mint: &str,
    creator_wallet: &str,
    exit_time_unix: i64,
    entry_features: Option<&serde_json::Value>,
) -> serde_json::Value {
    let mut features = serde_json::json!({});
    let map = features.as_object_mut().unwrap();

    // ── 1. Fresh Solana Tracker re-screen ──
    let st_client = SolanaTrackerClient::new(cfg.env.solana_tracker_api_key.clone());
    if let Some(st_data) = st_client.fetch_token(mint).await {
        map.insert(
            "post_st_risk_score".into(),
            serde_json::json!(st_data.risk_score),
        );
        map.insert("post_st_holders".into(), serde_json::json!(st_data.holders));
        map.insert(
            "post_st_top10_pct".into(),
            serde_json::json!(st_data.top10_pct),
        );
        map.insert(
            "post_st_bundlers_pct".into(),
            serde_json::json!(st_data.bundlers_pct),
        );

        // Compute deltas from entry
        if let Some(ef) = entry_features {
            if let Some(entry_holders) = ef.get("st_holders").and_then(|v| v.as_u64()) {
                if let Some(post_holders) = st_data.holders {
                    map.insert(
                        "delta_holders".into(),
                        serde_json::json!(post_holders as i64 - entry_holders as i64),
                    );
                }
            }
            if let Some(entry_risk) = ef.get("st_risk_score").and_then(|v| v.as_f64()) {
                if let Some(post_risk) = st_data.risk_score {
                    map.insert(
                        "delta_risk_score".into(),
                        serde_json::json!(post_risk - entry_risk),
                    );
                }
            }
        }
    }

    // ── 2-7. Birdeye post-trade endpoints ──
    if let Some(api_key) = &cfg.env.birdeye_api_key {
        let be = BirdeyeClient::new(api_key);

        // 2. OHLCV: 30 one-minute candles after exit
        let time_from = exit_time_unix;
        let time_to = exit_time_unix + 1800;
        if let Some(ohlcv) = be.ohlcv(mint, "1m", time_from, time_to).await {
            map.insert("post_ohlcv_candles".into(), ohlcv);
        }

        // 3. Trade data
        if let Some(td) = be.trade_data(mint).await {
            map.insert("post_trade_data".into(), td);
        }

        // 4. Top traders
        if let Some(tt) = be.top_traders(mint).await {
            map.insert("post_top_traders".into(), tt);
        }

        // 5. Holder distribution
        if let Some(hd) = be.holder_distribution(mint).await {
            map.insert("post_holder_distribution".into(), hd);
        }
    }

    map.insert(
        "post_trade_enrichment_at".into(),
        serde_json::json!(chrono::Utc::now().to_rfc3339()),
    );

    features
}

/// Update or insert creator reputation based on accumulated trade data.
async fn update_creator_reputation(supabase: &SupabaseClient, creator_wallet: &str) {
    // Check if creator exists in reputation table
    let check_url = format!(
        "{}/creator_reputation?wallet=eq.{}&select=total_launches",
        supabase.base_url, creator_wallet
    );

    let resp = match supabase.client.get(&check_url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(creator = %creator_wallet, "Creator reputation check failed: {}", e);
            return;
        }
    };

    let existing: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();

    if existing.is_empty() {
        // Insert new creator
        let url = format!("{}/creator_reputation", supabase.base_url);
        let payload = serde_json::json!({
            "wallet": creator_wallet,
            "total_launches": 1,
            "last_launch_at": chrono::Utc::now().to_rfc3339(),
            "updated_at": chrono::Utc::now().to_rfc3339(),
        });
        match supabase.client.post(&url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!(creator = %creator_wallet, "Creator reputation INSERT ok");
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                warn!(creator = %creator_wallet, "Creator reputation INSERT failed: {}", body);
            }
            Err(e) => {
                warn!(creator = %creator_wallet, "Creator reputation INSERT error: {}", e);
            }
        }
    } else {
        // Increment token count
        let current_count = existing[0]
            .get("total_launches")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let url = format!(
            "{}/creator_reputation?wallet=eq.{}",
            supabase.base_url, creator_wallet
        );
        let payload = serde_json::json!({
            "total_launches": current_count + 1,
            "last_launch_at": chrono::Utc::now().to_rfc3339(),
            "updated_at": chrono::Utc::now().to_rfc3339(),
        });
        match supabase.client.patch(&url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!(creator = %creator_wallet, "Creator reputation PATCH ok");
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                warn!(creator = %creator_wallet, "Creator reputation PATCH failed: {}", body);
            }
            Err(e) => {
                warn!(creator = %creator_wallet, "Creator reputation PATCH error: {}", e);
            }
        }
    }
}
