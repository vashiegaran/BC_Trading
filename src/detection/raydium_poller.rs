use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use super::types::{DetectionSource, GraduatedToken, PipelineTiming};

/// Raydium AMM v4 program address — all new pools are created by this program.
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

/// Maximum backoff between reconnection attempts.
const MAX_BACKOFF_SECS: u64 = 30;
const INITIAL_BACKOFF_SECS: u64 = 1;

/// Run the Raydium logsSubscribe listener.
///
/// Subscribes to logs for the Raydium AMM v4 program via the Solana
/// WebSocket RPC. When a new pool initialization is detected, emits
/// a GraduatedToken event downstream.
pub async fn run(tx: mpsc::Sender<GraduatedToken>, rpc_ws_url: String) {
    let mut backoff_secs = INITIAL_BACKOFF_SECS;

    loop {
        info!("Connecting to Solana RPC WebSocket for Raydium logsSubscribe...");

        match connect_and_listen(&tx, &rpc_ws_url).await {
            Ok(()) => {
                warn!("Raydium logsSubscribe closed cleanly — reconnecting");
                backoff_secs = INITIAL_BACKOFF_SECS;
            }
            Err(e) => {
                error!(
                    "Raydium logsSubscribe error: {:#}. Reconnecting in {}s...",
                    e, backoff_secs
                );
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
    }
}

async fn connect_and_listen(
    tx: &mpsc::Sender<GraduatedToken>,
    rpc_ws_url: &str,
) -> Result<()> {
    let (ws_stream, _) = connect_async(rpc_ws_url)
        .await
        .context("Failed to connect to Solana RPC WebSocket")?;

    info!("Connected to Solana RPC WebSocket for Raydium monitoring");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to logs mentioning Raydium AMM v4 program
    let subscribe_msg = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            { "mentions": [RAYDIUM_AMM_V4] },
            { "commitment": "confirmed" }
        ]
    });

    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await
        .context("Failed to send logsSubscribe message")?;

    info!(program = RAYDIUM_AMM_V4, "Subscribed to Raydium AMM v4 logs");

    while let Some(msg_result) = read.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => return Err(anyhow::anyhow!("WebSocket read error: {}", e)),
        };

        match msg {
            Message::Text(text) => {
                if let Err(e) = handle_log_message(&text, tx).await {
                    warn!("Failed to process Raydium log message: {:#}", e);
                }
            }
            Message::Ping(payload) => {
                let _ = write.send(Message::Pong(payload)).await;
            }
            Message::Close(_) => {
                info!("Raydium WebSocket sent Close frame");
                return Ok(());
            }
            _ => {}
        }
    }

    Err(anyhow::anyhow!("Raydium WebSocket stream ended unexpectedly"))
}

async fn handle_log_message(
    text: &str,
    tx: &mpsc::Sender<GraduatedToken>,
) -> Result<()> {
    let v: serde_json::Value = serde_json::from_str(text)
        .context("Invalid JSON from Raydium log")?;

    // Skip subscription confirmation messages
    if v.get("result").is_some() && v.get("method").is_none() {
        debug!("Raydium logsSubscribe confirmation received");
        return Ok(());
    }

    // Only process logsNotification messages
    let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("");
    if method != "logsNotification" {
        return Ok(());
    }

    let value = match v
        .get("params")
        .and_then(|p| p.get("result"))
        .and_then(|r| r.get("value"))
    {
        Some(val) => val,
        None => return Ok(()),
    };

    // Skip failed transactions
    if value.get("err").map(|e| !e.is_null()).unwrap_or(false) {
        return Ok(());
    }

    let logs = match value.get("logs").and_then(|l| l.as_array()) {
        Some(l) => l,
        None => return Ok(()),
    };

    // Check if this is a pool initialization transaction
    // Raydium logs "initialize2" when creating a new AMM pool
    let is_pool_init = logs.iter().any(|log| {
        log.as_str()
            .map(|s| s.contains("initialize2") || s.contains("InitializeInstruction2"))
            .unwrap_or(false)
    });

    if !is_pool_init {
        return Ok(());
    }

    let signature = value
        .get("signature")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");

    info!(
        signature = signature,
        "🟣 Raydium new pool detected via logsSubscribe"
    );

    // Extract pool address from logs if available
    // Raydium logs the pool address in the format "Program log: amm_pool: <ADDRESS>"
    let pool_address: Option<Pubkey> = logs
        .iter()
        .find_map(|log| {
            let s = log.as_str()?;
            if s.contains("amm_pool:") {
                let parts: Vec<&str> = s.split("amm_pool:").collect();
                parts.get(1)?.trim().split_whitespace().next()
                    .and_then(|addr| Pubkey::from_str(addr).ok())
            } else {
                None
            }
        });

    let now_ms = chrono::Utc::now().timestamp_millis();

    // Emit GraduatedToken with available data
    // Full enrichment (buy pressure, holders etc) happens in filter engine
    let graduated = GraduatedToken {
        mint: Pubkey::default(), // Will be resolved by filter engine from pool
        pool_address,
        creator_wallet: Pubkey::default(),
        bonding_curve_volume_sol: 0.0,
        buy_pressure_pct: 100.0, // Assume bullish — filters will verify
        time_to_graduate_seconds: 0.0,
        detected_at: now_ms,
        source: DetectionSource::Poll,
        unique_buyer_count: 0,
        buy_count: 0,
        sell_count: 0,
        trade_timestamps: vec![],
        name: String::new(), // Unknown for direct Raydium detection
        symbol: String::new(), // Unknown for direct Raydium detection
        initial_liquidity_sol: 0.0, // Unknown for direct Raydium detection
        creator_rebuy: false,
        buy_sell_ratio: 0.0,
        candidate_id: None,
        sniper_features: None,
        sniper_score: None,
        pipeline_timing: PipelineTiming::new(now_ms),
    };

    if tx.send(graduated).await.is_err() {
        warn!("Detection channel closed — receiver dropped");
    }

    Ok(())
}
