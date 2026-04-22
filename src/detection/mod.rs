pub mod pumpfun_ws;
pub mod raydium_poller;
pub mod st_search_poller;
pub mod types;

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::info;

use crate::config::AppConfig;
use crate::logger::SupabaseClient;
use types::GraduatedToken;

/// Channel capacity for the detection → downstream pipeline.
const CHANNEL_CAPACITY: usize = 100;

/// Starts the detection engine and returns a receiver for [`GraduatedToken`]s.
///
/// Internally this function:
/// 1. Creates an MPSC channel.
/// 2. Spawns the PumpFun WebSocket listener task.
/// 3. If `poll_raydium` is enabled in config, spawns the Raydium polling task.
/// 4. Returns the receiving half of the channel so that `main.rs` (or the
///    filter engine in later phases) can consume events.
pub fn start(cfg: Arc<AppConfig>, supabase: Arc<SupabaseClient>) -> mpsc::Receiver<GraduatedToken> {
    let (tx, rx) = mpsc::channel::<GraduatedToken>(CHANNEL_CAPACITY);

    // ── PumpFun WebSocket (primary, free) ────────────────
    let pumpfun_tx = tx.clone();
    let pumpfun_supabase = Arc::clone(&supabase);
    let rpc_url = cfg.env.solana_rpc_url.clone();
    let pumpfun_cfg = Arc::clone(&cfg);
    tokio::spawn(async move {
        pumpfun_ws::run(pumpfun_tx, pumpfun_supabase, rpc_url, pumpfun_cfg).await;
    });
    info!("Detection: PumpFun WebSocket task spawned");

    // ── Raydium logsSubscribe (real-time detection) ───────
    if cfg.strategy.detection.poll_raydium {
        let raydium_tx = tx.clone();
        // Convert HTTP RPC URL to WebSocket URL
        let rpc_ws_url = cfg.env.solana_rpc_url
            .replace("https://", "wss://")
            .replace("http://", "ws://");

        tokio::spawn(async move {
            raydium_poller::run(raydium_tx, rpc_ws_url).await;
        });
        info!("Detection: Raydium logsSubscribe task spawned");
    } else {
        info!("Detection: Raydium poller disabled (poll_raydium = false)");
    }

    // ── Solana Tracker /search polling (backup detection) ────
    if cfg.env.solana_tracker_api_key.is_some() {
        let search_tx = tx.clone();
        let st_api_key = cfg.env.solana_tracker_api_key.clone();
        tokio::spawn(async move {
            st_search_poller::run(search_tx, st_api_key).await;
        });
        info!("Detection: SolanaTracker search poller spawned (2-min interval)");
    } else {
        info!("Detection: ST search poller disabled (no API key)");
    }

    // `tx` is dropped here — only the cloned senders inside spawned tasks
    // keep the channel open. When all senders are gone the receiver will
    // return `None`, signalling the consumer loop to stop.
    drop(tx);

    rx
}
