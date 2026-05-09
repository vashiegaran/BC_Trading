pub mod pumpfun_ws;
pub mod raydium_poller;
pub mod st_search_poller;
pub mod types;
pub mod yellowstone_pumpfun;

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::logger::SupabaseClient;
use types::{new_bc_score_cache, BcScoreCache, GraduatedToken};

/// Channel capacity for the detection → downstream pipeline.
const CHANNEL_CAPACITY: usize = 100;

/// Starts the detection engine and returns a receiver for [`GraduatedToken`]s
/// plus the shared BC score cache (for the sniper fast-track pipeline).
pub fn start(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
) -> (mpsc::Receiver<GraduatedToken>, BcScoreCache) {
    let (tx, rx) = mpsc::channel::<GraduatedToken>(CHANNEL_CAPACITY);
    let bc_cache = new_bc_score_cache();

    // ── Detection source: Yellowstone gRPC (preferred) or PumpPortal WS ──
    let pumpfun_tx = tx.clone();
    let pumpfun_supabase = Arc::clone(&supabase);
    let rpc_url = cfg.env.solana_rpc_url.clone();
    let pumpfun_cfg = Arc::clone(&cfg);
    let pumpfun_cache = bc_cache.clone();

    if cfg.env.use_grpc_pumpfun_detection {
        match cfg.env.yellowstone_grpc_endpoint.as_deref() {
            Some(endpoint) if !endpoint.is_empty() => {
                let grpc_cfg = yellowstone_pumpfun::YellowstonePumpfunConfig {
                    endpoint: endpoint.to_string(),
                    x_token: cfg.env.yellowstone_grpc_x_token.clone(),
                    username: cfg.env.yellowstone_grpc_username.clone(),
                    password: cfg.env.yellowstone_grpc_password.clone(),
                };
                tokio::spawn(async move {
                    yellowstone_pumpfun::run(
                        grpc_cfg,
                        pumpfun_tx,
                        pumpfun_supabase,
                        rpc_url,
                        pumpfun_cfg,
                        pumpfun_cache,
                    )
                    .await;
                });
                info!(
                    "Detection: Yellowstone gRPC pump.fun source spawned (PumpPortal WS disabled)"
                );
            }
            _ => {
                warn!(
                    "USE_GRPC_PUMPFUN_DETECTION=true but YELLOWSTONE_GRPC_ENDPOINT is empty — \
                     falling back to PumpPortal WebSocket"
                );
                tokio::spawn(async move {
                    pumpfun_ws::run(
                        pumpfun_tx,
                        pumpfun_supabase,
                        rpc_url,
                        pumpfun_cfg,
                        pumpfun_cache,
                    )
                    .await;
                });
                info!("Detection: PumpFun WebSocket task spawned (fallback)");
            }
        }
    } else {
        tokio::spawn(async move {
            pumpfun_ws::run(
                pumpfun_tx,
                pumpfun_supabase,
                rpc_url,
                pumpfun_cfg,
                pumpfun_cache,
            )
            .await;
        });
        info!("Detection: PumpFun WebSocket task spawned");
    }

    // ── Raydium logsSubscribe (real-time detection) ───────
    if cfg.strategy.detection.poll_raydium {
        let raydium_tx = tx.clone();
        // Convert HTTP RPC URL to WebSocket URL
        let rpc_ws_url = cfg
            .env
            .solana_rpc_url
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
    // DISABLED 2026-04-24: contributed 0/141 bought positions over 7 days while
    // burning ~4,320 ST calls/day (~58% of remaining monthly budget). PumpFun WS
    // is the sole detection source in practice. Re-enable by uncommenting below
    // AND restoring the 20s interval only if WS coverage gaps are observed.
    //
    // if cfg.env.solana_tracker_api_key.is_some() {
    //     let search_tx = tx.clone();
    //     let st_api_key = cfg.env.solana_tracker_api_key.clone();
    //     tokio::spawn(async move {
    //         st_search_poller::run(search_tx, st_api_key).await;
    //     });
    //     info!("Detection: SolanaTracker search poller spawned");
    // } else {
    //     info!("Detection: ST search poller disabled (no API key)");
    // }
    info!("Detection: ST search poller disabled (see comment in detection/mod.rs)");
    let _ = &tx; // silence unused-clone lints if any future edit re-enables the block

    // `tx` is dropped here — only the cloned senders inside spawned tasks
    // keep the channel open. When all senders are gone the receiver will
    // return `None`, signalling the consumer loop to stop.
    drop(tx);

    (rx, bc_cache)
}
