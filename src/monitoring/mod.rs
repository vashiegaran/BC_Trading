pub mod api_limiter;
pub mod enrichment_sampler;
pub mod helius_price_ws;
pub mod helius_ws;
pub mod moonbag;
pub mod price;
pub mod st_trades;
pub mod tick_stream;
pub mod triggers;
pub mod types;
pub mod yellowstone_grpc;

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{debug, info, warn};

/// Backend-agnostic price-stream handle. PR-F1 added the `Yellowstone`
/// variant; the `Helius` variant remains for fallback. Construction picks
/// one based on `enable_yellowstone_grpc` / `enable_helius_price_ws`.
#[derive(Clone)]
pub enum PriceStreamBackend {
    Helius(helius_price_ws::PriceStreamHandle),
    Yellowstone(yellowstone_grpc::YellowstonePriceHandle),
}

impl PriceStreamBackend {
    pub fn subscribe(&self, mint: String) {
        match self {
            Self::Helius(h) => h.subscribe(mint),
            Self::Yellowstone(h) => h.subscribe(mint),
        }
    }

    pub fn unsubscribe(&self, mint: String) {
        match self {
            Self::Helius(h) => h.unsubscribe(mint),
            Self::Yellowstone(h) => h.unsubscribe(mint),
        }
    }

    /// Watch a developer's token account for sell-offs (rug detection via gRPC).
    /// Only effective with the Yellowstone backend.
    pub fn watch_dev_wallet(&self, mint: String, dev_token_account: String) {
        if let Self::Yellowstone(h) = self {
            h.watch_dev_wallet(mint, dev_token_account);
        }
    }
}

use crate::config::AppConfig;
use crate::execution::state::TradingState;
use crate::execution::types::PositionOpened;
use crate::filters::post_buy::PostBuyAlert;
use crate::logger::SupabaseClient;
use crate::narrative::{self, NarrativeContext, NarrativeResult, NarrativeState};

use moonbag::{MoonbagCommand, PromotionSource};
use enrichment_sampler::SamplerCtx;
use price::PriceFetcher;
use tick_stream::{DipAction, DipConfig, DipState, TickWindow, evaluate_dip};
use triggers::{check_triggers, PositionState};
use types::{ExitResult, ExitSignal};

/// Channel capacity for monitoring → exit pipeline.
const EXIT_CHANNEL_CAPACITY: usize = 50;

/// Channel capacity for exit → monitoring confirmation pipeline.
const CONFIRM_CHANNEL_CAPACITY: usize = 50;

/// Status returned from a dev wallet balance check.
#[derive(Debug, Clone, PartialEq)]
enum DevWalletStatus {
    /// Dev wallet token balance is within acceptable range.
    Stable,
    /// Dev has sold a significant portion of their tokens (partial dump).
    Dumping { drop_pct: f64 },
    /// Dev sold 100% of tokens — potential Community Takeover (bullish signal).
    CTO,
    /// Could not determine status (RPC error, etc.) — not actionable.
    Unknown,
}

/// Start the monitoring engine.
///
/// Returns `(exit_rx, confirm_tx, position_tx)`:
/// - `exit_rx`: receives exit signals for the exit engine
/// - `confirm_tx`: broadcast sender for exit engine to report sell results
/// - `position_tx`: a sender that can be used to inject positions
///   (e.g. recovered stuck positions on startup)
pub fn start(
    cfg: Arc<AppConfig>,
    mut position_rx: mpsc::Receiver<PositionOpened>,
    supabase: Arc<SupabaseClient>,
    trading_state: Arc<TradingState>,
    mut alert_rx: mpsc::Receiver<PostBuyAlert>,
    moonbag_tx: mpsc::Sender<MoonbagCommand>,
) -> (mpsc::Receiver<ExitSignal>, broadcast::Sender<ExitResult>, mpsc::Sender<PositionOpened>, mpsc::Sender<ExitSignal>) {
    let (exit_tx, exit_rx) = mpsc::channel::<ExitSignal>(EXIT_CHANNEL_CAPACITY);
    let exit_tx_for_moonbag = exit_tx.clone();
    let (confirm_tx, _) = broadcast::channel::<ExitResult>(CONFIRM_CHANNEL_CAPACITY);
    let confirm_tx_clone = confirm_tx.clone();
    let (inject_tx, mut inject_rx) = mpsc::channel::<PositionOpened>(EXIT_CHANNEL_CAPACITY);

    let birdeye_key = cfg.env.birdeye_api_key.clone();
    let price_timeout_secs = cfg.strategy.monitoring.price_timeout_secs;
    let api_request_timeout_secs = cfg.strategy.execution.api_request_timeout_secs;
    let max_retries = cfg.strategy.execution.max_retries;
    let max_sane_price = cfg.strategy.monitoring.max_sane_price_usd;
    let max_price_change_ratio = cfg.strategy.monitoring.max_price_change_ratio;

    tokio::spawn(async move {
        info!("Monitoring engine started — waiting for open positions");

        let mut price_fetcher = PriceFetcher::new(
            birdeye_key,
            price_timeout_secs,
            api_request_timeout_secs,
            max_retries,
            max_sane_price,
            max_price_change_ratio,
        );

        // ── Price-stream backend (PR-F1) ────────────────────────────
        // Priority order:
        //   1. Yellowstone gRPC (Chainstack) — if enabled AND endpoint set.
        //      Replaces Helius WS entirely; empirically Helius Developer
        //      plan silently drops accountNotifications for pump.fun PDAs.
        //   2. Helius WS mux — fallback for when Yellowstone isn't
        //      configured or was intentionally disabled.
        //   3. None — pure-polling v5 behavior.
        let (helius_price_cache, helius_price_handle) = if cfg.env.enable_yellowstone_grpc
            && cfg.env.yellowstone_grpc_endpoint.is_some()
        {
            let cache = Arc::new(helius_price_ws::HeliusPriceCache::new());
            price_fetcher = price_fetcher.with_helius_cache(Arc::clone(&cache));
            let ys_cfg = yellowstone_grpc::YellowstoneConfig {
                endpoint: cfg.env.yellowstone_grpc_endpoint.clone().unwrap(),
                x_token: cfg.env.yellowstone_grpc_x_token.clone(),
                username: cfg.env.yellowstone_grpc_username.clone(),
                password: cfg.env.yellowstone_grpc_password.clone(),
            };
            let handle = yellowstone_grpc::start_mux(
                ys_cfg,
                Arc::clone(&cache),
                Arc::clone(&supabase),
                None, // graduation_tx: wired in main.rs if detection channel exists
                {
                    // Derive bot wallet pubkey from private key for gRPC tx confirmation.
                    use solana_sdk::signer::Signer;
                    let bot_wallet = bs58::decode(&cfg.env.wallet_private_key).into_vec().ok()
                        .and_then(|kb| solana_sdk::signature::Keypair::from_bytes(&kb).ok())
                        .map(|kp| kp.pubkey().to_string());
                    if bot_wallet.is_none() {
                        warn!("Failed to derive bot wallet pubkey — gRPC tx confirmation disabled");
                    }
                    bot_wallet
                },
            );
            // Start the periodic metrics flusher here (previously lived
            // inside helius_price_ws::start_mux). Reuse the same function.
            tokio::spawn(helius_price_ws::flush_metrics_loop(
                Arc::clone(&cache),
                Arc::clone(&supabase),
            ));
            info!("📡 Yellowstone gRPC price stream ENABLED (Chainstack, Jito ShredStream)");
            (Some(cache), Some(PriceStreamBackend::Yellowstone(handle)))
        } else if cfg.strategy.monitoring.enable_helius_price_ws
            && cfg.env.helius_ws_url.is_some()
        {
            let cache = Arc::new(helius_price_ws::HeliusPriceCache::new());
            price_fetcher = price_fetcher.with_helius_cache(Arc::clone(&cache));
            let handle = helius_price_ws::start_mux(
                cfg.env.helius_ws_url.clone().unwrap(),
                Arc::clone(&cache),
                Arc::clone(&supabase),
            );
            info!("📡 Helius WS price stream ENABLED (multiplexed mux, single connection)");
            (Some(cache), Some(PriceStreamBackend::Helius(handle)))
        } else {
            (None, None)
        };

        let price_fetcher = Arc::new(price_fetcher);

        // SOL/USD refresher — keeps the WS subscribers' multiplier current.
        // 30s cadence = ~120 Jupiter calls/hour for SOL price — negligible.
        if let Some(cache) = helius_price_cache.clone() {
            let fetcher_for_sol = Arc::clone(&price_fetcher);
            tokio::spawn(async move {
                use crate::execution::jupiter::SOL_MINT;
                let mut tick = tokio::time::interval(Duration::from_secs(30));
                loop {
                    tick.tick().await;
                    let p = fetcher_for_sol.get_price(SOL_MINT).await;
                    if p > 0.0 {
                        cache.set_sol_usd(p);
                        debug!(sol_usd = p, "helius_price_ws: SOL/USD refreshed");
                    }
                }
            });
        }

        // Enrichment sampler (passive data collection — Tiers 1/2/3).
        // Built once and cloned into each per-position task when enabled.
        let sampler_ctx: Option<SamplerCtx> = if cfg.strategy.monitoring.enrichment_sampler_enabled {
            info!("🧪 Enrichment sampler ENABLED — snapshots scheduled per position");
            Some(SamplerCtx::new(Arc::clone(&cfg), Arc::clone(&supabase)))
        } else {
            None
        };

        // Stale position sweeper: force-close positions stuck in TradingState
        let stale_timeout = cfg.strategy.monitoring.stale_position_timeout_secs;
        let mut stale_sweep_interval = tokio::time::interval(Duration::from_secs(60));
        stale_sweep_interval.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                Some(opened) = position_rx.recv() => {
                    info!(
                        position_id = opened.position_id,
                        mint = %opened.mint,
                        entry_price = opened.entry_price_usd,
                        "👁️ Monitoring position"
                    );

                    let cfg = Arc::clone(&cfg);
                    let supabase = Arc::clone(&supabase);
                    let exit_tx = exit_tx.clone();
                    let fetcher = Arc::clone(&price_fetcher);
                    let ts = Arc::clone(&trading_state);

                    let confirm_rx = confirm_tx_clone.subscribe();
                    let moonbag = moonbag_tx.clone();
                    let sampler = sampler_ctx.clone();
                    if let Some(s) = sampler.clone() {
                        enrichment_sampler::spawn_scheduled_sampler(s, opened.clone());
                    }
                    // Subscribe price stream BEFORE the monitor task starts so
                    // first ticks are already populated by the time triggers run.
                    if let Some(h) = &helius_price_handle {
                        h.subscribe(opened.mint.clone());
                    }
                    let unsubscribe_handle = helius_price_handle.clone();
                    let mint_for_unsubscribe = opened.mint.clone();
                    let fetcher_cleanup = Arc::clone(&fetcher);
                    tokio::spawn(async move {
                        monitor_position(cfg, supabase, fetcher, exit_tx, confirm_rx, opened, ts, moonbag, sampler).await;
                        if let Some(h) = unsubscribe_handle {
                            h.unsubscribe(mint_for_unsubscribe.clone());
                        }
                        // Clean up stale price cache entries for this mint
                        fetcher_cleanup.remove_mint(&mint_for_unsubscribe);
                    });
                }
                Some(opened) = inject_rx.recv() => {
                    info!(
                        position_id = opened.position_id,
                        mint = %opened.mint,
                        entry_price = opened.entry_price_usd,
                        "🔄 Re-monitoring recovered position"
                    );

                    let cfg = Arc::clone(&cfg);
                    let supabase = Arc::clone(&supabase);
                    let exit_tx = exit_tx.clone();
                    let fetcher = Arc::clone(&price_fetcher);
                    let ts = Arc::clone(&trading_state);

                    let confirm_rx = confirm_tx_clone.subscribe();
                    let moonbag = moonbag_tx.clone();
                    let sampler = sampler_ctx.clone();
                    if let Some(s) = sampler.clone() {
                        enrichment_sampler::spawn_scheduled_sampler(s, opened.clone());
                    }
                    if let Some(h) = &helius_price_handle {
                        h.subscribe(opened.mint.clone());
                    }
                    let unsubscribe_handle = helius_price_handle.clone();
                    let mint_for_unsubscribe = opened.mint.clone();
                    let fetcher_cleanup = Arc::clone(&fetcher);
                    tokio::spawn(async move {
                        monitor_position(cfg, supabase, fetcher, exit_tx, confirm_rx, opened, ts, moonbag, sampler).await;
                        if let Some(h) = unsubscribe_handle {
                            h.unsubscribe(mint_for_unsubscribe.clone());
                        }
                        // Clean up stale price cache entries for this mint
                        fetcher_cleanup.remove_mint(&mint_for_unsubscribe);
                    });
                }
                Some(alert) = alert_rx.recv() => {
                    warn!(
                        mint = %alert.mint,
                        reason = %alert.reason,
                        "🚨 POST-BUY ALERT — triggering emergency exit"
                    );

                    let signal = ExitSignal {
                        position_id: alert.position_id,
                        mint: alert.mint.clone(),
                        pct_to_sell: 100,
                        reason: types::ExitReason::PostBuyVerificationFailed,
                        current_price: alert.entry_price_usd,
                        entry_price_usd: alert.entry_price_usd,
                        sol_spent: alert.sol_spent,
                        token_amount: alert.token_amount,
                        is_paper_trade: alert.is_paper_trade,
                        sub_reason: None,
                    };

                    if exit_tx.send(signal).await.is_err() {
                        warn!(mint = %alert.mint, "Post-buy alert → exit channel closed");
                    }
                }
                _ = stale_sweep_interval.tick() => {
                    if stale_timeout > 0 {
                        let stale = trading_state.stale_mints(stale_timeout).await;
                        for mint in stale {
                            warn!(
                                mint = %mint,
                                timeout_secs = stale_timeout,
                                "🧹 Force-closing stale position from TradingState"
                            );
                            // Force-remove from state so the slot is freed for new trades.
                            // The actual on-chain sell (if tokens exist) will be handled
                            // by the per-position monitor's TimeStop, or the DB-level
                            // recovery on next restart.
                            trading_state.force_close_mint(&mint, cfg.strategy.execution.buy_amount_sol).await;

                            // Log to Supabase
                            let payload = serde_json::json!({
                                "event_type": "stale_position_force_closed",
                                "message": format!("Mint: {} — position open > {}s, slot freed from TradingState", mint, stale_timeout),
                            });
                            let url = format!("{}/system_events", supabase.base_url);
                            let _ = supabase.client.post(&url).json(&payload).send().await;
                        }
                    }
                }
                else => break,
            }
        }

        info!("Monitoring engine shutting down (all channels closed)");
    });

    (exit_rx, confirm_tx, inject_tx, exit_tx_for_moonbag)
}

/// Monitoring loop for a single position.
async fn monitor_position(
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    price_fetcher: Arc<PriceFetcher>,
    exit_tx: mpsc::Sender<ExitSignal>,
    mut confirm_rx: broadcast::Receiver<ExitResult>,
    position: PositionOpened,
    trading_state: Arc<TradingState>,
    moonbag_tx: mpsc::Sender<MoonbagCommand>,
    sampler: Option<SamplerCtx>,
) {
    let interval_ms = cfg.strategy.monitoring.monitor_interval_ms;
    let exit_cfg = &cfg.strategy.exit;
    let dev_wallet_check_interval =
        Duration::from_secs(cfg.strategy.monitoring.dev_wallet_check_interval_secs);
    let dev_dump_threshold_pct = cfg.strategy.monitoring.dev_dump_threshold_pct;

    let started_at = Instant::now();
    let mut tp1_triggered = false;
    let mut tp2_triggered = false;
    let mut peak_price = position.entry_price_usd;
    let mut remaining_token_amount = position.token_amount;
    let mut remaining_sol_spent = position.sol_spent;
    let mut entry_price_usd = position.entry_price_usd;
    let mut entry_price_corrected = entry_price_usd >= 0.000001;
    let mut consecutive_exit_failures: u32 = 0;
    const MAX_EXIT_FAILURES: u32 = 5;

    // ── Narrative moonbag state ──────────────────────────
    let has_narrative = cfg.strategy.monitoring.narrative_check_enabled
        && cfg.env.openai_api_key.is_some();
    let narrative_intervals = cfg.strategy.monitoring.narrative_check_intervals_secs.clone();
    let mut narrative_state = NarrativeState::NoSignal;
    let mut narrative_check_idx: usize = 0;
    let mut narrative_in_flight = false;
    let mut last_narrative_result: Option<NarrativeResult> = None;
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(35))
        .build()
        .unwrap_or_default();

    let dev_wallet_pubkey = position.dev_wallet.as_deref().and_then(|s| {
        match Pubkey::from_str(s) {
            Ok(pk) => Some(pk),
            Err(e) => {
                warn!(
                    mint = %position.mint,
                    dev_wallet = s,
                    "Failed to parse dev wallet pubkey — dev dump protection disabled: {}",
                    e
                );
                None
            }
        }
    });
    let dev_initial_balance = position.dev_initial_balance.unwrap_or(0);
    let mint_pubkey = Pubkey::from_str(&position.mint).ok();
    let mut last_dev_check = Instant::now();

    let rpc = Arc::new(RpcClient::new_with_timeout(
        cfg.env.solana_rpc_url.clone(),
        Duration::from_secs(cfg.strategy.monitoring.dev_wallet_rpc_timeout_secs),
    ));

    // ── Helius WebSocket watchers (real-time dev + LP monitoring) ──
    // Spawn as parallel tasks. They send exit signals independently of
    // the polling loop below, which continues as a fallback.
    let (helius_shutdown_tx, _) = tokio::sync::watch::channel(false);

    // Note: bonding-curve price subscription is now handled by the shared
    // multiplexer in `helius_price_ws::start_mux` — subscribed/unsubscribed
    // by the spawn-site wrapper around `monitor_position`. We no longer
    // open a dedicated WS connection per position here.

    // Safety watchers use standard accountSubscribe (not Helius-specific),
    // so route through the primary WS (Chainstack) which actually delivers
    // notifications. Helius Developer plan silently drops them.
    {
        let safety_ws_url = cfg.env.solana_ws_url.clone();
        let watcher = helius_ws::HeliusWatcher::new(safety_ws_url.clone());
        let watch_ctx = helius_ws::WatchContext {
            position_id: position.position_id,
            mint: position.mint.clone(),
            current_price: position.entry_price_usd,
            entry_price_usd: position.entry_price_usd,
            sol_spent: position.sol_spent,
            token_amount: position.token_amount,
            is_paper_trade: position.is_paper_trade,
        };

        // 1. Dev wallet token account watcher
        if let (Some(dev_pub), Some(mint_pub)) = (&dev_wallet_pubkey, &mint_pubkey) {
            if dev_initial_balance > 0 {
                let dev_ata = helius_ws::derive_ata(dev_pub, mint_pub);
                let exit_tx_clone = exit_tx.clone();
                let ctx = watch_ctx.clone();
                let shutdown_rx = helius_shutdown_tx.subscribe();
                let watcher_clone = helius_ws::HeliusWatcher::new(safety_ws_url.clone());
                tokio::spawn(async move {
                    watcher_clone.watch_dev_wallet(
                        dev_ata,
                        dev_initial_balance,
                        dev_dump_threshold_pct,
                        exit_tx_clone,
                        ctx,
                        shutdown_rx,
                    ).await;
                });
            }
        }

        // 2. LP vault watcher
        if let Some(pool_str) = &position.pool_address {
            if let Ok(pool_pubkey) = Pubkey::from_str(pool_str) {
                let exit_tx_clone = exit_tx.clone();
                let ctx = watch_ctx.clone();
                let shutdown_rx = helius_shutdown_tx.subscribe();
                let lp_threshold = cfg.strategy.monitoring.lp_drop_threshold_pct;
                let lp_grace_secs = cfg.strategy.monitoring.lp_grace_period_secs;
                let rpc_clone = Arc::clone(&rpc);
                let mint_str = position.mint.clone();
                let ws_url = safety_ws_url.clone();
                tokio::spawn(async move {
                    // Grace period: fresh graduates shuffle LP during PumpFun→PumpSwap
                    // migration. Wait before activating to avoid false exits.
                    if lp_grace_secs > 0 {
                        info!(
                            mint = %mint_str,
                            grace_secs = lp_grace_secs,
                            "⏳ LP watcher: waiting grace period before activating"
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(lp_grace_secs)).await;
                        if *shutdown_rx.borrow() {
                            return;
                        }
                    }
                    // Fetch initial LP vault balance before starting watcher
                    let initial_lamports = match rpc_clone.get_balance(&pool_pubkey).await {
                        Ok(bal) => {
                            info!(
                                mint = %mint_str,
                                pool = %pool_pubkey,
                                lamports = bal,
                                "📊 Initial LP vault balance fetched"
                            );
                            bal
                        }
                        Err(e) => {
                            warn!(
                                mint = %mint_str,
                                "Failed to fetch initial LP balance: {} — LP watcher disabled",
                                e
                            );
                            return;
                        }
                    };
                    let watcher = helius_ws::HeliusWatcher::new(ws_url);
                    watcher.watch_lp_vault(
                        pool_pubkey,
                        initial_lamports,
                        lp_threshold,
                        exit_tx_clone,
                        ctx,
                        shutdown_rx,
                    ).await;
                });
            }
        }
    }

    // ── Tick stream: pool vault watcher for real-time buy/sell data ──
    let tick_window = Arc::new(Mutex::new(TickWindow::new(
        cfg.strategy.monitoring.tick_window_secs,
        cfg.strategy.monitoring.dip_min_volume_sol,
    )));
    let dip_cfg = DipConfig::from_monitoring_config(&cfg.strategy.monitoring);

    // Tick stream also uses standard accountSubscribe — route through Chainstack WS.
    {
        let tick_ws_url = cfg.env.solana_ws_url.clone();
        if position.pool_address.is_none() {
            warn!(
                target: "tick_monitor",
                mint = %position.mint,
                position_id = position.position_id,
                "⚠️ TICK_MONITOR_SKIP — no pool_address on position, momentum ticks will be zero"
            );
        }
        if let Some(pool_str) = &position.pool_address {
            if let (Ok(pool_pubkey), Some(mint_pub)) = (Pubkey::from_str(pool_str), &mint_pubkey) {
                // Resolve vault addresses — PumpSwap pools store vaults in on-chain
                // data; legacy Raydium pools use ATA derivation.
                let (token_vault, sol_vault, sol_vault_is_token_account, resolve_method) =
                    match helius_ws::resolve_pool_vaults(&rpc, &pool_pubkey, mint_pub).await {
                        Some(v) => (v.token_vault, v.sol_vault, v.sol_vault_is_token_account, "pumpswap_onchain"),
                        None => {
                            // Fallback for non-PumpSwap pools (Raydium etc.)
                            let tv = helius_ws::derive_ata(&pool_pubkey, mint_pub);
                            (tv, pool_pubkey, false, "legacy_ata_fallback")
                        }
                    };

                info!(
                    target: "tick_monitor",
                    mint = %position.mint,
                    position_id = position.position_id,
                    pool = %pool_pubkey,
                    token_vault = %token_vault,
                    sol_vault = %sol_vault,
                    resolve_method,
                    "🔧 TICK_MONITOR_INIT — vault resolution (method='{}')",
                    resolve_method
                );

                // Fetch initial balances for tick delta calculation
                let rpc_clone = Arc::clone(&rpc);
                let tick_window_clone = Arc::clone(&tick_window);
                let shutdown_rx = helius_shutdown_tx.subscribe();
                let mint_str = position.mint.clone();
                let ws_url = tick_ws_url.clone();

                tokio::spawn(async move {
                    // Get initial token vault balance
                    let initial_token = match rpc_clone
                        .get_token_account_balance(&token_vault)
                        .await
                    {
                        Ok(bal) => {
                            let amount = bal.amount.parse::<u64>().unwrap_or(0);
                            info!(
                                mint = %mint_str,
                                token_vault = %token_vault,
                                initial_balance = amount,
                                "📊 Initial pool token vault balance"
                            );
                            amount
                        }
                        Err(e) => {
                            warn!(
                                mint = %mint_str,
                                token_vault = %token_vault,
                                "Failed to fetch pool token vault balance: {} — tick stream disabled",
                                e
                            );
                            return;
                        }
                    };

                    // For PumpSwap: SOL vault is a WSOL token account.
                    // For Raydium: SOL vault is a raw SOL account.
                    let initial_sol = if sol_vault_is_token_account {
                        match rpc_clone.get_token_account_balance(&sol_vault).await {
                            Ok(bal) => {
                                let amount = bal.amount.parse::<u64>().unwrap_or(0);
                                info!(
                                    mint = %mint_str,
                                    sol_vault = %sol_vault,
                                    initial_wsol_lamports = amount,
                                    "📊 Initial pool WSOL vault balance"
                                );
                                amount
                            }
                            Err(e) => {
                                warn!(
                                    mint = %mint_str,
                                    "Failed to fetch pool WSOL vault balance: {} — tick stream disabled",
                                    e
                                );
                                return;
                            }
                        }
                    } else {
                        match rpc_clone.get_balance(&sol_vault).await {
                            Ok(bal) => bal,
                            Err(e) => {
                                warn!(
                                    mint = %mint_str,
                                    "Failed to fetch pool SOL vault balance: {} — tick stream disabled",
                                    e
                                );
                                return;
                            }
                        }
                    };

                    let watcher = helius_ws::HeliusWatcher::new(ws_url);
                    watcher.watch_pool_trades(
                        token_vault,
                        sol_vault,
                        initial_token,
                        initial_sol,
                        tick_window_clone,
                        mint_str,
                        shutdown_rx,
                        sol_vault_is_token_account,
                    ).await;
                });
            }
        }
    }

    let mut dip_state = DipState::Normal;
    let mut consecutive_death_ticks: u32 = 0;

    // ── Round Number Rejection (#5) state ──
    let mut round_touch_price: Option<f64> = None;
    let mut round_touch_time: Option<Instant> = None;

    // ── Lower High (#6) state ──
    let mut lh_first_peak: f64 = 0.0;
    let mut lh_dip_happened = false;
    let mut lh_second_peak: f64 = 0.0;

    // ── ST /trades observe-only watcher ──────────────────────
    if cfg.env.solana_tracker_api_key.is_some() {
        let st_key = cfg.env.solana_tracker_api_key.clone();
        let mint_str = position.mint.clone();
        let shutdown_rx = helius_shutdown_tx.subscribe();
        let pos_id = position.position_id;
        let supa = Arc::clone(&supabase);
        tokio::spawn(async move {
            st_trades::watch_trades(
                st_key, mint_str, pos_id, supa, shutdown_rx,
            ).await;
        });
        debug!(mint = %position.mint, "ST trades observe-only watcher spawned");
    }

    // Tracking for periodic momentum writes (Gap 5) and exit snapshot (Gap 6)
    let mut last_momentum_write = Instant::now();
    let mut last_momentum = tick_stream::MomentumSnapshot::default();
    let mut last_exit_reason: Option<String> = None;
    let mut last_pnl_pct = 0.0_f64;
    let mut last_price = entry_price_usd;

    // CTO (Community Takeover) staged evaluation state
    let cto_stages = cfg.strategy.monitoring.cto_stage_secs.clone();
    let cto_strong_pct = cfg.strategy.monitoring.cto_strong_recovery_pct;
    let cto_moderate_pct = cfg.strategy.monitoring.cto_moderate_recovery_pct;
    let cto_min_dev_pct = cfg.strategy.monitoring.cto_min_dev_hold_pct;
    let cto_collapse_pct = cfg.strategy.monitoring.cto_collapse_pct;
    let cto_early_kill_momentum = cfg.strategy.monitoring.cto_early_kill_momentum;
    let cto_strong_momentum = cfg.strategy.monitoring.cto_strong_momentum;
    let cto_moderate_momentum = cfg.strategy.monitoring.cto_moderate_momentum;
    let mut cto_detected = false;
    let mut cto_detect_time: Option<Instant> = None;
    let mut cto_pre_price: f64 = 0.0;
    let mut cto_stage_idx: usize = 0; // which staged checkpoint we're at
    let mut cto_stage_prices: Vec<f64> = Vec::new(); // price at each checkpoint for higher-lows detection

    // Extract dev hold % from sniper enrichment features (from SolanaTracker or Birdeye)
    let cto_dev_hold_pct: f64 = position.sniper_features.as_ref().and_then(|f| {
        // Try SolanaTracker dev_pct first, fall back to Birdeye owner/creator pct
        f.get("st_dev_pct").and_then(|v| v.as_f64())
            .or_else(|| f.get("be_owner_balance_pct").and_then(|v| v.as_f64()))
            .or_else(|| f.get("be_creator_balance_pct").and_then(|v| v.as_f64()))
    }).unwrap_or(0.0);

    // ── Shadow logging: start from entry, runs in parallel with monitoring ──
    let (shadow_exit_tx, shadow_exit_rx) = tokio::sync::watch::channel::<Option<String>>(None);
    let shadow_duration = cfg.strategy.monitoring.shadow_log_duration_secs;
    if shadow_duration > 0 {
        let price_fetcher = Arc::clone(&price_fetcher);
        let supabase = Arc::clone(&supabase);
        let mint = position.mint.clone();
        let position_id = position.position_id;
        let shadow_entry = position.entry_price_usd;
        tokio::spawn(async move {
            shadow_log_loop(
                price_fetcher,
                supabase,
                mint,
                position_id,
                shadow_entry,
                shadow_duration,
                shadow_exit_rx,
            )
            .await;
        });
    }

    let mut consecutive_zero_prices: u32 = 0;
    /// After this many consecutive zero-price readings, fire emergency exit.
    /// At 500ms interval, 120 = 60 seconds of no price data.
    /// For brand-new tokens, price APIs (DexScreener, Birdeye) can take 30-60s
    /// to index the token. The Jupiter quote fallback in PriceFetcher covers
    /// most cases, but if even that fails for 60 seconds the token is likely dead.
    const MAX_ZERO_PRICES: u32 = 120;

    /// Graduated tokens have no Helius WS bonding-curve feed, so
    /// `get_monitoring_price()` returns a stale `last_known` forever.
    /// Force a fresh Jupiter price fetch every JUPITER_REFRESH_TICKS ticks
    /// (~6s at 2s interval) so the monitoring loop sees real price changes.
    const JUPITER_REFRESH_TICKS: u32 = 3;
    let mut price_poll_counter: u32 = 0;

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        price_poll_counter += 1;

        // ── TimeStop: evaluate BEFORE price fetch — it's purely time-based ──
        // This must never be gated behind price availability. If price is 0,
        // all other triggers are skipped, but TimeStop must still fire.
        let elapsed_seconds = started_at.elapsed().as_secs();
        if elapsed_seconds >= cfg.strategy.exit.max_hold_seconds {
            // Try to get a price for the exit signal; fall back to entry price
            let exit_price = {
                let p = price_fetcher.get_price(&position.mint).await;
                if p > 0.0 { p } else { entry_price_usd }
            };

            info!(
                mint = %position.mint,
                elapsed_s = elapsed_seconds,
                max_hold_s = cfg.strategy.exit.max_hold_seconds,
                exit_price,
                "⏰ TimeStop triggered — forcing exit"
            );

            let signal = ExitSignal {
                position_id: position.position_id,
                mint: position.mint.clone(),
                pct_to_sell: 100,
                reason: types::ExitReason::TimeStop,
                current_price: exit_price,
                entry_price_usd,
                sol_spent: remaining_sol_spent,
                token_amount: remaining_token_amount,
                is_paper_trade: position.is_paper_trade,
                sub_reason: None,
            };

            if exit_tx.send(signal).await.is_err() {
                warn!(mint = %position.mint, "Monitoring → exit channel closed");
                break;
            }

            // Wait for confirmation before deciding whether to break or retry
            info!(mint = %position.mint, "⏳ TimeStop exit signal sent — waiting for confirmation");
            let mut got_success = false;
            let wait_timeout = tokio::time::sleep(std::time::Duration::from_secs(120));
            tokio::pin!(wait_timeout);

            loop {
                tokio::select! {
                    result = confirm_rx.recv() => {
                        match result {
                            Ok(r) if r.mint == position.mint => {
                                if r.success {
                                    info!(mint = %position.mint, "✅ TimeStop exit confirmed");
                                    got_success = true;
                                    break;
                                } else if r.permanent {
                                    warn!(mint = %position.mint, "🛑 TimeStop exit permanently failed (e.g. TOKEN_NOT_TRADABLE) — giving up");
                                    got_success = true; // treat as handled — don't retry
                                    break;
                                } else {
                                    warn!(mint = %position.mint, "❌ TimeStop exit failed — will retry next tick");
                                    break;
                                }
                            }
                            Ok(_) => continue,
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(_) => {
                                warn!(mint = %position.mint, "Confirm channel closed");
                                break;
                            }
                        }
                    }
                    _ = &mut wait_timeout => {
                        warn!(mint = %position.mint, "⏰ TimeStop confirmation timed out — will retry");
                        break;
                    }
                }
            }

            if got_success {
                last_exit_reason = Some("time_stop".to_string());
                break; // Position closed — exit monitoring loop
            }
            // Failed — loop back and retry TimeStop next tick
            continue;
        }

        // Fetch current price.
        // For bonding-curve tokens: Helius WS feeds real-time price into cache.
        // For GRADUATED tokens (PumpSwap/Raydium): WS cache has no entry, so
        // get_monitoring_price() returns stale last_known. We must periodically
        // call Jupiter (get_price) to get fresh quotes.
        let ws_has_fresh = price_fetcher.helius_cache()
            .map_or(false, |c| c.get(&position.mint).is_some());
        let mut current_price = if ws_has_fresh {
            // Bonding-curve token with live WS feed — use cache (fast, free)
            price_fetcher.get_monitoring_price(&position.mint)
        } else if price_poll_counter % JUPITER_REFRESH_TICKS == 0 {
            // Graduated token — force Jupiter quote every ~6s for real price
            price_fetcher.get_price(&position.mint).await
        } else {
            // Between Jupiter calls, use last_known (may be slightly stale
            // but prevents hammering Jupiter on every 2s tick)
            price_fetcher.get_monitoring_price(&position.mint)
        };
        // First-tick fallback: if still zero, try Jupiter regardless
        if current_price <= 0.0 {
            current_price = price_fetcher.get_price(&position.mint).await;
        }
        if current_price <= 0.0 {
            consecutive_zero_prices += 1;
            if consecutive_zero_prices >= MAX_ZERO_PRICES {
                warn!(
                    mint = %position.mint,
                    consecutive_zero_prices,
                    "🚨 Price unavailable for too long — token likely dead, triggering emergency exit"
                );
                let signal = ExitSignal {
                    position_id: position.position_id,
                    mint: position.mint.clone(),
                    pct_to_sell: 100,
                    reason: types::ExitReason::TimeStop,
                    current_price: entry_price_usd,
                    entry_price_usd,
                    sol_spent: position.sol_spent,
                    token_amount: remaining_token_amount,
                    is_paper_trade: position.is_paper_trade,
                    sub_reason: None,
                };
                let _ = exit_tx.send(signal).await;
                last_exit_reason = Some("zero_price_dead".to_string());
                break;
            }
            debug!(mint = %position.mint, consecutive_zero_prices, "Price is 0 — skipping trigger evaluation");
            continue;
        }
        consecutive_zero_prices = 0;

        // ── Post-fill sanity check: if position is >50% underwater within first 10s,
        // the entry was catastrophic (stale quote, token already rugging). Emergency exit.
        if started_at.elapsed().as_secs() <= 10 && entry_price_usd > 0.000001 {
            let first_multiplier = current_price / entry_price_usd;
            if first_multiplier < 0.50 {
                warn!(
                    mint = %position.mint,
                    current_price,
                    entry_price_usd,
                    multiplier = format!("{:.3}x", first_multiplier),
                    "🚨 Post-fill sanity failed: >50% underwater at first tick — emergency exit"
                );

                let signal = ExitSignal {
                    position_id: position.position_id,
                    mint: position.mint.clone(),
                    pct_to_sell: 100,
                    reason: types::ExitReason::PostFillSanity,
                    current_price,
                    entry_price_usd,
                    sol_spent: remaining_sol_spent,
                    token_amount: remaining_token_amount,
                    is_paper_trade: position.is_paper_trade,
                    sub_reason: None,
                };

                if exit_tx.send(signal).await.is_err() {
                    warn!(mint = %position.mint, "Monitoring → exit channel closed");
                }
                last_exit_reason = Some("post_fill_sanity".to_string());
                break;
            }
        }

        // Self-correct broken entry price: if entry_price was near-zero (price fetch
        // failed at buy time) and we now have a valid current_price, use it as entry.
        // This is slightly inaccurate (price may have moved) but far better than
        // having no TP triggers for the entire position lifetime.
        if !entry_price_corrected && current_price > 0.000001 {
            entry_price_usd = current_price;
            peak_price = current_price;
            entry_price_corrected = true;
            warn!(
                mint = %position.mint,
                corrected_entry_price = current_price,
                "🔧 Entry price was near-zero — corrected to first valid price"
            );
            // Update Supabase so exit logic also sees the corrected price
            update_entry_price(&supabase, position.position_id, current_price).await;
        }

        let pnl_pct = if entry_price_usd > 0.0 {
            (current_price - entry_price_usd) / entry_price_usd * 100.0
        } else {
            0.0
        };

        debug!(
            mint = %position.mint,
            current_price,
            pnl_pct = format!("{:.2}", pnl_pct),
            elapsed_s = elapsed_seconds,
            "📊 Price update"
        );

        // Update peak price
        if current_price > peak_price {
            peak_price = current_price;
            let peak_multiplier = if entry_price_usd > 0.0 {
                peak_price / entry_price_usd
            } else {
                0.0
            };
            // Fire-and-forget — peak price is observational, don't block price polling
            let supabase_peak = supabase.clone();
            let pos_id = position.position_id;
            tokio::spawn(async move {
                update_peak_price(&supabase_peak, pos_id, peak_price, peak_multiplier).await;
            });
        }

        // ── Pattern #5: Round Number Rejection ──────────────────
        // If price hits a round multiplier (2x, 3x, 5x, 10x) and then
        // drops >5% from that touch within 60 seconds → exit.
        if entry_price_usd > 0.000001 && elapsed_seconds >= 30 {
            let multiplier = current_price / entry_price_usd;
            let near_round = [2.0, 3.0, 5.0, 10.0].iter().any(|&level| {
                (multiplier - level).abs() / level < 0.05
            });

            if near_round && round_touch_price.is_none() {
                round_touch_price = Some(current_price);
                round_touch_time = Some(Instant::now());
                debug!(
                    mint = %position.mint,
                    multiplier = format!("{:.2}x", multiplier),
                    touch_price = format!("{:.8}", current_price),
                    "🎯 Round multiplier touched — watching for rejection"
                );
            }

            if let (Some(touch_price), Some(touch_time)) = (round_touch_price, round_touch_time) {
                if touch_time.elapsed().as_secs() <= 60 {
                    let drop_from_touch = (touch_price - current_price) / touch_price;
                    if drop_from_touch >= 0.05 {
                        warn!(
                            mint = %position.mint,
                            touch_price = format!("{:.8}", touch_price),
                            current_price = format!("{:.8}", current_price),
                            drop_pct = format!("{:.1}%", drop_from_touch * 100.0),
                            "🔴 [OBSERVE] ROUND NUMBER REJECTION — price rejected from round multiplier"
                        );
                        // Observe-only: log but do NOT exit
                        round_touch_price = None;
                        round_touch_time = None;
                    }
                } else {
                    // 60s window expired without rejection — clear
                    round_touch_price = None;
                    round_touch_time = None;
                }
            }
        }

        // ── Pattern #6: Lower High ──────────────────────────────
        // Track first meaningful peak (>1.1x entry). After a >10% dip,
        // watch for a rally. If the rally's peak is >5% below first peak
        // and starts turning down → bearish structure, exit.
        if entry_price_usd > 0.000001 && elapsed_seconds >= 30 {
            // Phase 1: record first peak (must be meaningful)
            if !lh_dip_happened && peak_price >= entry_price_usd * 1.1 {
                lh_first_peak = peak_price;
            }

            // Phase 2: detect significant dip from first peak
            if lh_first_peak > 0.0 && !lh_dip_happened {
                if current_price < lh_first_peak * 0.90 {
                    lh_dip_happened = true;
                    lh_second_peak = current_price;
                    debug!(
                        mint = %position.mint,
                        first_peak = format!("{:.8}", lh_first_peak),
                        dip_price = format!("{:.8}", current_price),
                        "📉 LowerHigh: significant dip from first peak — watching for lower high"
                    );
                }
            }

            // Phase 3: after dip, track second rally and detect lower high
            if lh_dip_happened {
                if current_price > lh_second_peak {
                    lh_second_peak = current_price;
                }
                // Second peak formed and it's <95% of first peak, and price is turning down
                if lh_second_peak < lh_first_peak * 0.95
                    && lh_second_peak > entry_price_usd
                    && current_price < lh_second_peak * 0.97
                {
                    warn!(
                        mint = %position.mint,
                        first_peak = format!("{:.8}", lh_first_peak),
                        second_peak = format!("{:.8}", lh_second_peak),
                        current = format!("{:.8}", current_price),
                        "📉 [OBSERVE] LOWER HIGH — bearish structure confirmed"
                    );
                    // Observe-only: log but do NOT exit. Reset state so it can re-detect.
                    lh_first_peak = 0.0;
                    lh_dip_happened = false;
                    lh_second_peak = 0.0;
                }
            }
        }

        // ── Dev wallet monitoring ─────────────────────────────
        if let (Some(dev_pub), Some(mint_pub)) = (&dev_wallet_pubkey, &mint_pubkey) {
            if dev_initial_balance > 0 && last_dev_check.elapsed() >= dev_wallet_check_interval {
                last_dev_check = Instant::now();

                let status = check_dev_wallet(
                    rpc.as_ref(),
                    dev_pub,
                    mint_pub,
                    dev_initial_balance,
                    dev_dump_threshold_pct,
                )
                .await;

                match status {
                    DevWalletStatus::CTO if !cto_detected => {
                        // Dev sold 100% — potential Community Takeover.
                        // Gate: only enter CTO path if dev held a meaningful % of supply.
                        if cto_dev_hold_pct < cto_min_dev_pct {
                            info!(
                                mint = %position.mint,
                                dev_hold_pct = format!("{:.1}%", cto_dev_hold_pct),
                                min_required = format!("{:.1}%", cto_min_dev_pct),
                                "Dev sold 100% but held only {:.1}% — too small for CTO, treating as noise",
                                cto_dev_hold_pct
                            );
                            // Don't exit, don't blacklist — just ignore (tiny dev position)
                        } else {
                            cto_detected = true;
                            cto_detect_time = Some(Instant::now());
                            cto_pre_price = current_price;
                            cto_stage_idx = 0;

                            info!(
                                mint = %position.mint,
                                dev_hold_pct = format!("{:.1}%", cto_dev_hold_pct),
                                pre_price = format!("{:.10}", current_price),
                                stages = ?cto_stages,
                                "🔄 CTO detected — dev held {:.1}% and sold 100%. Starting staged evaluation",
                                cto_dev_hold_pct
                            );

                            let payload = serde_json::json!({
                                "event_type": "cto_detected",
                                "message": format!(
                                    "CTO: dev held {:.1}% and sold 100% for mint {}. Stages: {:?}, thresholds: strong {:.0}% / moderate {:.0}%",
                                    cto_dev_hold_pct, position.mint, cto_stages, cto_strong_pct, cto_moderate_pct
                                ),
                            });
                            let url = format!("{}/system_events", supabase.base_url);
                            let _ = supabase.client.post(&url).json(&payload).send().await;
                        }
                    }
                    DevWalletStatus::Dumping { drop_pct } => {
                        // Partial dump (not 100%) — still emergency exit + blacklist
                        warn!(
                            mint = %position.mint,
                            drop_pct = format!("{:.1}%", drop_pct),
                            "🚨 Dev wallet dumping detected — triggering emergency exit"
                        );

                        log_dev_dump_event(&supabase, &position.mint, drop_pct).await;

                        // Blacklist this dev wallet so future tokens from them are skipped
                        if let Some(dev_str) = &position.dev_wallet {
                            trading_state.blacklist_dev(dev_str).await;
                        }

                        let signal = ExitSignal {
                            position_id: position.position_id,
                            mint: position.mint.clone(),
                            pct_to_sell: 100,
                            reason: types::ExitReason::DevWalletDumping,
                            current_price,
                            entry_price_usd,
                            sol_spent: position.sol_spent,
                            token_amount: remaining_token_amount,
                            is_paper_trade: position.is_paper_trade,
                            sub_reason: None,
                        };

                        if exit_tx.send(signal).await.is_err() {
                            warn!(mint = %position.mint, "Monitoring → exit channel closed");
                        }

                        info!(mint = %position.mint, "Monitoring task ending — dev dump exit signal sent");
                        last_exit_reason = Some("dev_wallet_dumping".to_string());
                        break;
                    }
                    _ => {} // Stable, Unknown, or CTO already detected — no action
                }
            }
        }

        // ── CTO staged evaluation (2min / 5min / 10min checkpoints) ──
        if cto_detected {
            if let Some(detect_time) = cto_detect_time {
                let elapsed = detect_time.elapsed().as_secs();

                if cto_pre_price > 0.0 && cto_stage_idx < cto_stages.len() {
                    let recovery_pct = current_price / cto_pre_price * 100.0;
                    let momentum = last_momentum.momentum_ratio;

                    // Hard collapse: if price drops below collapse threshold, instant exit
                    if recovery_pct < cto_collapse_pct {
                        warn!(
                            mint = %position.mint,
                            recovery = format!("{:.1}%", recovery_pct),
                            collapse_threshold = format!("{:.0}%", cto_collapse_pct),
                            "💀 CTO hard collapse — price below {:.0}%. Token is dead.",
                            cto_collapse_pct
                        );

                        let signal = ExitSignal {
                            position_id: position.position_id,
                            mint: position.mint.clone(),
                            pct_to_sell: 100,
                            reason: types::ExitReason::DevWalletDumping,
                            current_price,
                            entry_price_usd,
                            sol_spent: position.sol_spent,
                            token_amount: remaining_token_amount,
                            is_paper_trade: position.is_paper_trade,
                            sub_reason: None,
                        };

                        if exit_tx.send(signal).await.is_err() {
                            warn!(mint = %position.mint, "Monitoring → exit channel closed");
                        }
                        last_exit_reason = Some("cto_failed_collapse".to_string());

                        let payload = serde_json::json!({
                            "event_type": "cto_failed",
                            "message": format!(
                                "CTO collapse for mint {}: price at {:.1}% of pre-CTO (below {:.0}% threshold)",
                                position.mint, recovery_pct, cto_collapse_pct
                            ),
                        });
                        let url = format!("{}/system_events", supabase.base_url);
                        let _ = supabase.client.post(&url).json(&payload).send().await;
                        break;
                    }

                    // Check if we've reached the next staged checkpoint
                    let stage_target = cto_stages[cto_stage_idx];
                    if elapsed >= stage_target {
                        let stage_num = cto_stage_idx + 1;
                        let is_final = cto_stage_idx + 1 >= cto_stages.len();

                        // Track price at each checkpoint for higher-lows detection
                        cto_stage_prices.push(current_price);
                        let making_higher_lows = cto_stage_prices.len() >= 2
                            && cto_stage_prices.last() > cto_stage_prices.get(cto_stage_prices.len() - 2);

                        info!(
                            mint = %position.mint,
                            stage = stage_num,
                            elapsed_secs = elapsed,
                            recovery = format!("{:.1}%", recovery_pct),
                            momentum = format!("{:.2}", momentum),
                            higher_lows = making_higher_lows,
                            is_final,
                            "🔍 CTO stage {} check at {}s",
                            stage_num, stage_target
                        );

                        // Log each stage to system_events
                        let payload = serde_json::json!({
                            "event_type": format!("cto_stage_{}", stage_num),
                            "message": format!(
                                "CTO stage {} for mint {} at {}s: recovery {:.1}%, momentum {:.2}, higher_lows: {}{}",
                                stage_num, position.mint, elapsed, recovery_pct, momentum, making_higher_lows,
                                if is_final { " [FINAL]" } else { "" }
                            ),
                        });
                        let url = format!("{}/system_events", supabase.base_url);
                        let _ = supabase.client.post(&url).json(&payload).send().await;

                        // Stage 2 (5min) early kill: dead momentum + lower lows = token is done
                        if stage_num == 2 && !is_final {
                            if momentum < cto_early_kill_momentum && !making_higher_lows {
                                warn!(
                                    mint = %position.mint,
                                    recovery = format!("{:.1}%", recovery_pct),
                                    momentum = format!("{:.2}", momentum),
                                    "💀 CTO early kill at 5min — weak momentum + lower lows. Exiting."
                                );

                                let signal = ExitSignal {
                                    position_id: position.position_id,
                                    mint: position.mint.clone(),
                                    pct_to_sell: 100,
                                    reason: types::ExitReason::DevWalletDumping,
                                    current_price,
                                    entry_price_usd,
                                    sol_spent: position.sol_spent,
                                    token_amount: remaining_token_amount,
                                    is_paper_trade: position.is_paper_trade,
                                    sub_reason: None,
                                };

                                if exit_tx.send(signal).await.is_err() {
                                    warn!(mint = %position.mint, "Monitoring → exit channel closed");
                                }
                                last_exit_reason = Some("cto_failed_early_kill".to_string());

                                let payload = serde_json::json!({
                                    "event_type": "cto_failed",
                                    "message": format!(
                                        "CTO early kill for mint {}: momentum {:.2} < {:.1}, no higher lows at 5min",
                                        position.mint, momentum, cto_early_kill_momentum
                                    ),
                                });
                                let url = format!("{}/system_events", supabase.base_url);
                                let _ = supabase.client.post(&url).json(&payload).send().await;
                                break;
                            }
                        }

                        if is_final {
                            // Final stage (10min) — grade the CTO using momentum + price
                            let is_strong = recovery_pct >= cto_strong_pct
                                && momentum >= cto_strong_momentum;
                            let is_strong_by_trend = momentum >= cto_strong_momentum
                                && making_higher_lows;
                            let is_moderate = recovery_pct >= cto_moderate_pct
                                && momentum >= cto_moderate_momentum;

                            if is_strong || is_strong_by_trend {
                                // Strong CTO: clear absorption, price holding or trending up
                                info!(
                                    mint = %position.mint,
                                    recovery = format!("{:.1}%", recovery_pct),
                                    momentum = format!("{:.2}", momentum),
                                    higher_lows = making_higher_lows,
                                    "✅ STRONG CTO — promoting to moonbag (24-48h, 55% trail)"
                                );
                                if narrative_state < NarrativeState::RunnerConfirmed {
                                    narrative_state = NarrativeState::RunnerConfirmed;
                                }

                                let payload = serde_json::json!({
                                    "event_type": "cto_strong",
                                    "message": format!(
                                        "Strong CTO for mint {}: {:.1}% recovery, momentum {:.2}, higher_lows: {}. → moonbag (RunnerConfirmed)",
                                        position.mint, recovery_pct, momentum, making_higher_lows
                                    ),
                                });
                                let url = format!("{}/system_events", supabase.base_url);
                                let _ = supabase.client.post(&url).json(&payload).send().await;

                                // Promote to moonbag with strong CTO params
                                let cmd = MoonbagCommand {
                                    position_id: position.position_id,
                                    mint: position.mint.clone(),
                                    token_name: position.token_name.clone(),
                                    token_symbol: position.token_symbol.clone(),
                                    entry_price_usd,
                                    token_amount: remaining_token_amount,
                                    sol_value: remaining_sol_spent,
                                    peak_price,
                                    narrative_state,
                                    is_paper_trade: position.is_paper_trade,
                                    narrative_result: last_narrative_result.clone(),
                                    promotion_source: PromotionSource::CtoStrong,
                                    price_at_promotion: last_price,
                                };

                                if moonbag_tx.send(cmd).await.is_ok() {
                                    trading_state.record_exit(
                                        &position.mint,
                                        position.sol_spent,
                                        0.0,
                                        true,
                                    ).await;
                                    info!(mint = %position.mint, "🌙 CTO strong → moonbag promotion complete");
                                    last_exit_reason = Some("cto_strong_moonbag".to_string());
                                    break;
                                } else {
                                    warn!(mint = %position.mint, "Moonbag channel closed — continuing normal monitoring");
                                }
                            } else if is_moderate {
                                // Moderate CTO: stabilizing, decent buy flow
                                info!(
                                    mint = %position.mint,
                                    recovery = format!("{:.1}%", recovery_pct),
                                    momentum = format!("{:.2}", momentum),
                                    "✅ Moderate CTO — promoting to moonbag (12-24h, 45% trail)"
                                );
                                if narrative_state < NarrativeState::ExpandingAttention {
                                    narrative_state = NarrativeState::ExpandingAttention;
                                }

                                let payload = serde_json::json!({
                                    "event_type": "cto_moderate",
                                    "message": format!(
                                        "Moderate CTO for mint {}: {:.1}% recovery, momentum {:.2}. → moonbag (ExpandingAttention)",
                                        position.mint, recovery_pct, momentum
                                    ),
                                });
                                let url = format!("{}/system_events", supabase.base_url);
                                let _ = supabase.client.post(&url).json(&payload).send().await;

                                // Promote to moonbag with moderate CTO params
                                let cmd = MoonbagCommand {
                                    position_id: position.position_id,
                                    mint: position.mint.clone(),
                                    token_name: position.token_name.clone(),
                                    token_symbol: position.token_symbol.clone(),
                                    entry_price_usd,
                                    token_amount: remaining_token_amount,
                                    sol_value: remaining_sol_spent,
                                    peak_price,
                                    narrative_state,
                                    is_paper_trade: position.is_paper_trade,
                                    narrative_result: last_narrative_result.clone(),
                                    promotion_source: PromotionSource::CtoModerate,
                                    price_at_promotion: last_price,
                                };

                                if moonbag_tx.send(cmd).await.is_ok() {
                                    trading_state.record_exit(
                                        &position.mint,
                                        position.sol_spent,
                                        0.0,
                                        true,
                                    ).await;
                                    info!(mint = %position.mint, "🌙 CTO moderate → moonbag promotion complete");
                                    last_exit_reason = Some("cto_moderate_moonbag".to_string());
                                    break;
                                } else {
                                    warn!(mint = %position.mint, "Moonbag channel closed — continuing normal monitoring");
                                }
                            } else {
                                // Failed CTO at final stage
                                warn!(
                                    mint = %position.mint,
                                    recovery = format!("{:.1}%", recovery_pct),
                                    momentum = format!("{:.2}", momentum),
                                    "💀 CTO failed at 10min — weak momentum + low recovery. Exiting."
                                );

                                let signal = ExitSignal {
                                    position_id: position.position_id,
                                    mint: position.mint.clone(),
                                    pct_to_sell: 100,
                                    reason: types::ExitReason::DevWalletDumping,
                                    current_price,
                                    entry_price_usd,
                                    sol_spent: position.sol_spent,
                                    token_amount: remaining_token_amount,
                                    is_paper_trade: position.is_paper_trade,
                                    sub_reason: None,
                                };

                                if exit_tx.send(signal).await.is_err() {
                                    warn!(mint = %position.mint, "Monitoring → exit channel closed");
                                }
                                last_exit_reason = Some("cto_failed".to_string());

                                let payload = serde_json::json!({
                                    "event_type": "cto_failed",
                                    "message": format!(
                                        "CTO failed for mint {}: {:.1}% recovery, momentum {:.2} at final stage",
                                        position.mint, recovery_pct, momentum
                                    ),
                                });
                                let url = format!("{}/system_events", supabase.base_url);
                                let _ = supabase.client.post(&url).json(&payload).send().await;
                                break;
                            }

                            // CTO evaluation complete — clear state
                            cto_detect_time = None;
                        }

                        cto_stage_idx += 1;
                    }
                }
            }
        }

        // ── Narrative check (decaying schedule) ─────────────
        // Gates (cost reduction):
        //   1. Stop re-checks once score already meets moonbag threshold (no point)
        //   2. Price gate: skip if price < 1.2× entry (flat/down token won't promote)
        //   3. Momentum gate: skip if momentum < 0.5 (more selling than buying)
        let current_score = last_narrative_result.as_ref().map(|nr| nr.score as f64).unwrap_or(0.0);
        let score_below_threshold = current_score < cfg.strategy.monitoring.moonbag_promotion_min_score;
        if has_narrative && !narrative_in_flight && narrative_check_idx < narrative_intervals.len()
            && score_below_threshold
        {
            let target_secs = narrative_intervals[narrative_check_idx];
            if elapsed_seconds >= target_secs {
                // Price gate: token must be up at least 20% from entry
                let price_ratio = if entry_price_usd > 0.0 { current_price / entry_price_usd } else { 0.0 };
                if price_ratio < 1.2 || last_momentum.momentum_ratio < 0.5 {
                    // Skip this check — token is flat/down or momentum is weak
                    info!(
                        mint = %position.mint,
                        price_ratio = format!("{:.2}x", price_ratio),
                        momentum = format!("{:.2}", last_momentum.momentum_ratio),
                        check_idx = narrative_check_idx,
                        "🔮 Narrative check SKIPPED — price/momentum gate (saved $0.025)"
                    );
                    narrative_check_idx += 1;
                } else {
                narrative_in_flight = true;
                let api_key = cfg.env.openai_api_key.clone().unwrap_or_default();
                let birdeye_key = cfg.env.birdeye_api_key.clone().unwrap_or_default();
                let x_bearer = cfg.env.x_api_bearer_token.clone().unwrap_or_default();
                let ctx = NarrativeContext {
                    mint: position.mint.clone(),
                    name: position.token_name.clone(),
                    symbol: position.token_symbol.clone(),
                    current_price_usd: current_price,
                    entry_price_usd,
                    peak_multiplier: if entry_price_usd > 0.0 { peak_price / entry_price_usd } else { 1.0 },
                    hold_seconds: elapsed_seconds,
                    buy_count: last_momentum.buy_count,
                    sell_count: last_momentum.sell_count,
                    momentum_ratio: last_momentum.momentum_ratio,
                    buy_volume_sol: last_momentum.buy_volume_sol,
                    sell_volume_sol: last_momentum.sell_volume_sol,
                };
                let client = http_client.clone();
                let mint_log = position.mint.clone();

                // Run narrative check in a spawned task to avoid blocking price polling
                let (ntx, nrx) = tokio::sync::oneshot::channel();
                tokio::spawn(async move {
                    let result = narrative::check_narrative(&client, &api_key, &birdeye_key, &x_bearer, &ctx).await;
                    let _ = ntx.send(result);
                });

                // Check for result non-blocking on next iterations
                // Store the receiver for later polling
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(35),
                    nrx,
                ).await;

                narrative_in_flight = false;
                narrative_check_idx += 1;

                match result {
                    Ok(Ok(Ok(nr))) => {
                        // Ratchet-only: state can only go UP
                        if nr.state > narrative_state {
                            info!(
                                mint = %mint_log,
                                old_state = %narrative_state,
                                new_state = %nr.state,
                                score = nr.score,
                                check_num = narrative_check_idx,
                                "🔮 Narrative state UPGRADED"
                            );
                            narrative_state = nr.state;
                        } else {
                            info!(
                                mint = %mint_log,
                                state = %narrative_state,
                                score = nr.score,
                                check_num = narrative_check_idx,
                                "🔮 Narrative check — no upgrade"
                            );
                        }

                        // Persist full OpenAI output to positions table
                        {
                            let sb = Arc::clone(&supabase);
                            let pid = position.position_id;
                            let nr_json = serde_json::to_value(&nr).unwrap_or_default();
                            tokio::spawn(async move {
                                let url = format!("{}/positions?id=eq.{}", sb.base_url, pid);
                                let payload = serde_json::json!({
                                    "narrative_state": nr.state.to_string(),
                                    "narrative_score": nr.score,
                                    "narrative_result": nr_json,
                                });
                                let _ = sb.client.patch(&url).json(&payload).send().await;
                            });
                        }

                        last_narrative_result = Some(nr);
                    }
                    Ok(Ok(Err(e))) => {
                        warn!(mint = %mint_log, "Narrative check failed: {}", e);
                    }
                    _ => {
                        warn!(mint = %mint_log, "Narrative check timed out or channel dropped");
                    }
                }
                } // end else (price/momentum gate passed)
            }
        }

        // Build position state for trigger evaluation
        let pos_state = PositionState {
            position_id: position.position_id,
            mint: position.mint.clone(),
            entry_price_usd,
            current_price,
            peak_price,
            sol_spent: position.sol_spent,
            token_amount: remaining_token_amount,
            tp1_triggered,
            tp2_triggered,
            elapsed_seconds,
            is_paper_trade: position.is_paper_trade,
            initial_liquidity_sol: position.initial_liquidity_sol,
        };

        // ── Tick stream momentum + dip state machine ──────────
        let momentum = tick_window.lock().await.snapshot();
        let dip_action = evaluate_dip(
            &mut dip_state,
            current_price,
            peak_price,
            entry_price_usd,
            &momentum,
            &dip_cfg,
            elapsed_seconds,
            pnl_pct,
            &mut consecutive_death_ticks,
        );

        // Track last state for exit snapshot (Gap 6)
        last_momentum = momentum.clone();
        last_pnl_pct = pnl_pct;
        last_price = current_price;

        // Periodic momentum logged at debug level (shadow_log handles price curve separately)
        if last_momentum_write.elapsed().as_secs() >= 30 {
            last_momentum_write = Instant::now();
            debug!(
                mint = %position.mint,
                elapsed_secs = elapsed_seconds,
                price = current_price,
                pnl_pct = format!("{:.2}", pnl_pct),
                momentum_ratio = format!("{:.2}", momentum.momentum_ratio),
                buys = momentum.buy_count,
                sells = momentum.sell_count,
                "📊 Periodic momentum snapshot"
            );
        }

        // Handle dip death signals (immediate exit)
        // Gate: skip dip_death if position is younger than min_hold_before_dip_death
        if let DipAction::ImmediateExit { reason: dip_reason } = &dip_action {
            if elapsed_seconds < cfg.strategy.monitoring.min_hold_before_dip_death {
                debug!(
                    mint = %position.mint,
                    reason = dip_reason,
                    age_secs = elapsed_seconds,
                    min_hold = cfg.strategy.monitoring.min_hold_before_dip_death,
                    "⏳ Dip death suppressed — position too young"
                );
            } else {
                warn!(
                    mint = %position.mint,
                    reason = dip_reason,
                    momentum_ratio = format!("{:.2}", momentum.momentum_ratio),
                    "🚨 Dip death signal — immediate exit"
                );
                // Tier 2 — snapshot on-chain state right before dip_death exit.
                // Pure logging; no strategy change. Fire-and-forget.
                if let Some(ctx) = &sampler {
                    let ctx_c = ctx.clone();
                    let pos_id = position.position_id;
                    let mint_c = position.mint.clone();
                    let entry_px = entry_price_usd;
                    let dev_wallet = position.dev_wallet.clone();
                    let elapsed = elapsed_seconds as i64;
                    let dip_reason_c = dip_reason.to_string();
                    tokio::spawn(async move {
                        let trigger = format!("pre_dip_death:{}", dip_reason_c);
                        enrichment_sampler::snapshot_ad_hoc(
                            &ctx_c,
                            pos_id,
                            &mint_c,
                            entry_px,
                            dev_wallet.as_deref(),
                            elapsed,
                            &trigger,
                        ).await;
                    });
                }
                let signal = ExitSignal {
                    position_id: position.position_id,
                    mint: position.mint.clone(),
                    pct_to_sell: 100,
                    reason: types::ExitReason::DipDeath,
                    current_price,
                    entry_price_usd,
                    sol_spent: position.sol_spent,
                    token_amount: remaining_token_amount,
                    is_paper_trade: position.is_paper_trade,
                    sub_reason: Some(dip_reason.to_string()),
                };
                if exit_tx.send(signal).await.is_err() {
                    warn!(mint = %position.mint, "Monitoring → exit channel closed");
                }
                last_exit_reason = Some("dip_death".to_string());
                break;
            }
        }

        let suppress_trailing = matches!(dip_action, DipAction::SuppressTrailingStop);

        if suppress_trailing {
            debug!(
                mint = %position.mint,
                dip_state = ?dip_state,
                momentum_ratio = format!("{:.2}", momentum.momentum_ratio),
                buys = momentum.buy_count,
                sells = momentum.sell_count,
                "⏸️ Trailing stop suppressed (dip grace active)"
            );
        }

        // Check exit triggers (trailing stop may be suppressed by dip state)
        if let Some(signal) = check_triggers(&pos_state, exit_cfg, suppress_trailing) {
            info!(
                mint = %position.mint,
                reason = %signal.reason,
                pct_to_sell = signal.pct_to_sell,
                current_price,
                pnl_pct = format!("{:.2}", pnl_pct),
                "🔔 Exit trigger fired"
            );

            let mut is_full_exit = signal.pct_to_sell == 100;

            // ── FIX: capture token amount BEFORE reducing, send that in signal ──
            let tokens_for_signal = remaining_token_amount;
            let sold_pct = signal.pct_to_sell as f64 / 100.0;
            let new_remaining = remaining_token_amount * (1.0 - sold_pct);

            // Set local flags to prevent re-fire. DB flags written by exit engine after confirmed sell.
            let signal_reason_clone = signal.reason.clone();
            match signal.reason {
                types::ExitReason::TakeProfit1 => {
                    tp1_triggered = true;
                }
                types::ExitReason::TakeProfit2 => {
                    tp2_triggered = true;
                }
                _ => {}
            }

            // ── TP2 moonbag intercept: score check OR fast-runner auto-promote ──
            if signal_reason_clone == types::ExitReason::TakeProfit2
                && remaining_token_amount > 1.0
            {
                let openai_score = last_narrative_result.as_ref().map(|nr| nr.score as f64).unwrap_or(0.0);
                let min_score = cfg.strategy.monitoring.moonbag_promotion_min_score;
                let elapsed_secs = started_at.elapsed().as_secs();
                let fast_runner_threshold = cfg.strategy.monitoring.fast_runner_threshold_secs;
                let is_fast_runner = elapsed_secs < fast_runner_threshold && last_narrative_result.is_none();

                // Determine if we should promote
                let should_promote = openai_score >= min_score || is_fast_runner;
                let promotion_source = if is_fast_runner {
                    PromotionSource::FastRunner
                } else {
                    PromotionSource::NarrativeTp2
                };

                // Log TP2 moonbag evaluation
                {
                    let sb = Arc::clone(&supabase);
                    let mint_bg = position.mint.clone();
                    let name_bg = position.token_name.clone();
                    let sym_bg = position.token_symbol.clone();
                    let promoted = should_promote;
                    let ns = format!("{}", narrative_state);
                    let hold_secs = elapsed_secs as i64;
                    let pos_id = position.position_id;
                    let lp = last_price;
                    let ep = entry_price_usd;
                    let mr = last_momentum.momentum_ratio;
                    let pk = if entry_price_usd > 0.0 { peak_price / entry_price_usd } else { 1.0 };
                    let nr_clone = last_narrative_result.clone();
                    let source_str = promotion_source.to_string();
                    let is_fr = is_fast_runner;
                    tokio::spawn(async move {
                        let nr_json = nr_clone.as_ref()
                            .and_then(|nr| serde_json::to_value(nr).ok())
                            .unwrap_or(serde_json::json!(null));
                        let payload = serde_json::json!({
                            "position_id": pos_id,
                            "mint": mint_bg,
                            "token_name": name_bg,
                            "token_symbol": sym_bg,
                            "check_phase": "moonbag_promotion_tp2",
                            "check_index": 0,
                            "narrative_state": ns,
                            "score": openai_score as i64,
                            "narrative_strength": nr_clone.as_ref().map(|nr| nr.narrative_strength.as_str()).unwrap_or("none"),
                            "market_strength": nr_clone.as_ref().map(|nr| nr.market_strength.as_str()).unwrap_or("weak"),
                            "web_sources_found": nr_clone.as_ref().map(|nr| nr.web_sources_found).unwrap_or(0),
                            "reasons": serde_json::json!([{
                                "openai_score": openai_score,
                                "min_score": min_score,
                                "promoted": promoted,
                                "scoring_method": if is_fr { "fast_runner_auto" } else { "openai_holistic" },
                                "trigger": "tp2_intercept",
                                "promotion_source": source_str,
                                "is_fast_runner": is_fr,
                                "hold_seconds_at_tp2": hold_secs,
                            }]),
                            "current_price_usd": lp,
                            "entry_price_usd": ep,
                            "peak_multiplier": pk,
                            "hold_seconds": hold_secs,
                            "momentum_ratio": mr,
                        });
                        let url = format!("{}/narrative_checks", sb.base_url);
                        let _ = sb.client.post(&url).json(&payload).send().await;
                    });
                }

                if should_promote {
                    if is_fast_runner {
                        info!(
                            mint = %position.mint,
                            hold_secs = elapsed_secs,
                            threshold = fast_runner_threshold,
                            remaining_tokens = remaining_token_amount,
                            "🚀 FAST RUNNER — auto-promoting to moonbag (no narrative yet, background check will fire)"
                        );
                    } else {
                        info!(
                            mint = %position.mint,
                            openai_score = format!("{:.0}/100", openai_score),
                            min_score = format!("{:.0}", min_score),
                            narrative_state = %narrative_state,
                            remaining_tokens = remaining_token_amount,
                            "🌙 TP2 intercept — score passed → PROMOTING to moonbag (skipping TP2 sell)"
                        );
                    }

                    // Reset TP2 flag since we're not actually selling
                    tp2_triggered = false;

                    let cmd = MoonbagCommand {
                        position_id: position.position_id,
                        mint: position.mint.clone(),
                        token_name: position.token_name.clone(),
                        token_symbol: position.token_symbol.clone(),
                        entry_price_usd,
                        token_amount: remaining_token_amount,
                        sol_value: remaining_sol_spent,
                        peak_price,
                        narrative_state,
                        is_paper_trade: position.is_paper_trade,
                        narrative_result: last_narrative_result.clone(),
                        promotion_source,
                        price_at_promotion: last_price,
                    };

                    if moonbag_tx.send(cmd).await.is_ok() {
                        trading_state.record_exit(
                            &position.mint,
                            position.sol_spent,
                            0.0,
                            true,
                        ).await;

                        info!(
                            mint = %position.mint,
                            "🌙 TP2 → moonbag promotion complete — slot freed"
                        );
                        last_exit_reason = Some("moonbag_promoted_tp2".to_string());
                        break;
                    } else {
                        warn!(mint = %position.mint, "Moonbag channel closed — falling through to normal TP2 sell");
                        tp2_triggered = true; // Re-set so normal sell proceeds
                    }
                } else {
                    debug!(
                        mint = %position.mint,
                        openai_score = format!("{:.0}/100", openai_score),
                        min_score = format!("{:.0}", min_score),
                        "🌙 TP2 moonbag check — score too low, proceeding with TP2 sell"
                    );
                }
            }

            // ── FIX: send signal with ORIGINAL token amount, not zeroed amount ──
            let exit_signal = ExitSignal {
                position_id: signal.position_id,
                mint: signal.mint.clone(),
                pct_to_sell: signal.pct_to_sell,
                reason: signal.reason,
                current_price: signal.current_price,
                entry_price_usd: signal.entry_price_usd,
                sol_spent: signal.sol_spent,
                token_amount: tokens_for_signal, // ← actual tokens, not 0
                is_paper_trade: signal.is_paper_trade,
                sub_reason: None,
            };

            if exit_tx.send(exit_signal).await.is_err() {
                warn!(mint = %position.mint, "Monitoring → exit channel closed");
                break;
            }

            // ── FIX: only reduce remaining_token_amount in memory AFTER signal sent ──
            // DB token_amount is updated by the exit engine after confirmed sell,
            // NOT here. We only track it locally to know how many tokens remain.
            remaining_token_amount = new_remaining;
            remaining_sol_spent *= 1.0 - sold_pct;

            // Wait for exit engine confirmation before continuing.
            // This prevents race conditions where TP1, TP2, and trailing_stop
            // all fire concurrently and compete over the same tokens.
            info!(mint = %position.mint, "⏳ Exit signal sent — waiting for confirmation");
            let mut got_confirmation = false;
            let wait_timeout = tokio::time::sleep(std::time::Duration::from_secs(120));
            tokio::pin!(wait_timeout);

            loop {
                tokio::select! {
                    result = confirm_rx.recv() => {
                        match result {
                            Ok(r) if r.mint == position.mint => {
                                if r.success {
                                    consecutive_exit_failures = 0;
                                    info!(
                                        mint = %position.mint,
                                        reason = %r.reason,
                                        is_full_exit,
                                        "✅ Exit confirmed"
                                    );
                                    got_confirmation = true;
                                    break;
                                } else if r.permanent {
                                    warn!(
                                        mint = %position.mint,
                                        reason = %r.reason,
                                        "🛑 Exit permanently failed (e.g. TOKEN_NOT_TRADABLE) — giving up"
                                    );
                                    is_full_exit = true; // force break from outer loop
                                    got_confirmation = true;
                                    break;
                                } else {
                                    consecutive_exit_failures += 1;
                                    if consecutive_exit_failures >= MAX_EXIT_FAILURES {
                                        warn!(
                                            mint = %position.mint,
                                            failures = consecutive_exit_failures,
                                            "🛑 Max consecutive exit failures reached — abandoning position"
                                        );
                                        is_full_exit = true;
                                        got_confirmation = true;
                                        break;
                                    }
                                    warn!(
                                        mint = %position.mint,
                                        reason = %r.reason,
                                        failures = consecutive_exit_failures,
                                        "❌ Exit failed — restoring tokens for retry"
                                    );
                                    // Restore token amounts so trigger can re-fire
                                    remaining_token_amount = tokens_for_signal;
                                    remaining_sol_spent /= 1.0 - sold_pct;
                                    // Reset TP flag if it was a partial that failed
                                    match r.reason {
                                        types::ExitReason::TakeProfit1 => {
                                            tp1_triggered = false;
                                        }
                                        types::ExitReason::TakeProfit2 => {
                                            tp2_triggered = false;
                                        }
                                        _ => {}
                                    }
                                    got_confirmation = true;
                                    break;
                                }
                            }
                            Ok(_) => continue, // confirmation for a different mint
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(_) => {
                                warn!(mint = %position.mint, "Confirm channel closed — ending monitoring");
                                got_confirmation = true;
                                break;
                            }
                        }
                    }
                    _ = &mut wait_timeout => {
                        warn!(mint = %position.mint, "⏰ Timed out waiting for exit confirmation — restoring for retry");
                        remaining_token_amount = tokens_for_signal;
                        remaining_sol_spent /= 1.0 - sold_pct;
                        // Reset TP flag on timeout too
                        match signal_reason_clone {
                            types::ExitReason::TakeProfit1 => {
                                tp1_triggered = false;
                            }
                            types::ExitReason::TakeProfit2 => {
                                tp2_triggered = false;
                            }
                            _ => {}
                        }
                        break;
                    }
                }
            }

            // Only break out of the monitoring loop if full exit succeeded
            if is_full_exit && got_confirmation && remaining_token_amount < 1.0 {
                last_exit_reason = Some(signal_reason_clone.to_string());
                break;
            }

            // ── Moonbag promotion: TP1 confirmed + OpenAI holistic score ──
            // The OpenAI score (0-100) is the sole decision-maker — it evaluates
            // on-chain flow, social/narrative presence, and market data together.
            // No more manual on-chain scoring + narrative bonus split.
            if signal_reason_clone == types::ExitReason::TakeProfit1
                && got_confirmation
                && tp1_triggered
                && remaining_token_amount > 1.0
            {
                let peak_mult = if entry_price_usd > 0.0 { peak_price / entry_price_usd } else { 1.0 };
                let openai_score = last_narrative_result.as_ref().map(|nr| nr.score as f64).unwrap_or(0.0);
                let min_score = cfg.strategy.monitoring.moonbag_promotion_min_score;

                // Log every moonbag evaluation to narrative_checks for calibration
                {
                    let sb = Arc::clone(&supabase);
                    let mint_bg = position.mint.clone();
                    let name_bg = position.token_name.clone();
                    let sym_bg = position.token_symbol.clone();
                    let promoted = openai_score >= min_score;
                    let ns = format!("{}", narrative_state);
                    let hold_secs = started_at.elapsed().as_secs() as i64;
                    let pos_id = position.position_id;
                    let lp = last_price;
                    let ep = entry_price_usd;
                    let mr = last_momentum.momentum_ratio;
                    let nr_clone = last_narrative_result.clone();
                    tokio::spawn(async move {
                        let nr_json = nr_clone.as_ref()
                            .and_then(|nr| serde_json::to_value(nr).ok())
                            .unwrap_or(serde_json::json!(null));
                        let payload = serde_json::json!({
                            "position_id": pos_id,
                            "mint": mint_bg,
                            "token_name": name_bg,
                            "token_symbol": sym_bg,
                            "check_phase": "moonbag_promotion",
                            "check_index": 0,
                            "narrative_state": ns,
                            "score": openai_score as i64,
                            "narrative_strength": nr_clone.as_ref().map(|nr| nr.narrative_strength.as_str()).unwrap_or("none"),
                            "market_strength": nr_clone.as_ref().map(|nr| nr.market_strength.as_str()).unwrap_or("weak"),
                            "web_sources_found": nr_clone.as_ref().map(|nr| nr.web_sources_found).unwrap_or(0),
                            "reasons": serde_json::json!([{
                                "openai_score": openai_score,
                                "min_score": min_score,
                                "promoted": promoted,
                                "scoring_method": "openai_holistic",
                            }]),
                            "current_price_usd": lp,
                            "entry_price_usd": ep,
                            "peak_multiplier": peak_mult,
                            "hold_seconds": hold_secs,
                            "momentum_ratio": mr,
                        });
                        let url = format!("{}/narrative_checks", sb.base_url);
                        let _ = sb.client.post(&url).json(&payload).send().await;
                    });
                }

                if openai_score >= min_score {
                    info!(
                        mint = %position.mint,
                        openai_score = format!("{:.0}/100", openai_score),
                        min_score = format!("{:.0}", min_score),
                        narrative_state = %narrative_state,
                        remaining_tokens = remaining_token_amount,
                        "🌙 TP1 + OpenAI score → PROMOTING to moonbag"
                    );

                    let cmd = MoonbagCommand {
                        position_id: position.position_id,
                        mint: position.mint.clone(),
                        token_name: position.token_name.clone(),
                        token_symbol: position.token_symbol.clone(),
                        entry_price_usd,
                        token_amount: remaining_token_amount,
                        sol_value: remaining_sol_spent,
                        peak_price,
                        narrative_state,
                        is_paper_trade: position.is_paper_trade,
                        narrative_result: last_narrative_result.clone(),
                        promotion_source: PromotionSource::NarrativeTp1,
                        price_at_promotion: last_price,
                    };

                    if moonbag_tx.send(cmd).await.is_ok() {
                        trading_state.record_exit(
                            &position.mint,
                            position.sol_spent,
                            0.0,
                            true,
                        ).await;

                        info!(
                            mint = %position.mint,
                            "🌙 Slot freed — moonbag tracker now owns this position"
                        );
                        last_exit_reason = Some("moonbag_promoted".to_string());
                        break;
                    } else {
                        warn!(mint = %position.mint, "Moonbag channel closed — continuing normal monitoring");
                    }
                } else {
                    debug!(
                        mint = %position.mint,
                        openai_score = format!("{:.0}/100", openai_score),
                        min_score = format!("{:.0}", min_score),
                        "🌙 TP1 hit but OpenAI score too low for moonbag"
                    );
                }
            }

            // Otherwise, continue monitoring — next trigger will fire on next iteration
            continue;
        }

    }

    // ── Gap 6: Write monitoring_snapshot JSONB to positions at exit ──
    // Capture efficiency: what % of the upside from entry→peak did we actually capture?
    let capture_efficiency_pct = if peak_price > entry_price_usd && entry_price_usd > 0.0 {
        let total_upside = peak_price - entry_price_usd;
        let captured = last_price - entry_price_usd;
        (captured / total_upside * 100.0).clamp(-999.0, 100.0)
    } else {
        0.0
    };
    let monitoring_snapshot = serde_json::json!({
        "exit_price_usd": last_price,
        "peak_price_usd": peak_price,
        "entry_price_usd": entry_price_usd,
        "pnl_pct": last_pnl_pct,
        "hold_seconds": started_at.elapsed().as_secs(),
        "tp1_triggered": tp1_triggered,
        "tp2_triggered": tp2_triggered,
        "remaining_token_amount": remaining_token_amount,
        "capture_efficiency_pct": capture_efficiency_pct,
        "detection_source": position.detection_source,
        "dip_state": format!("{:?}", dip_state),
        "momentum": {
            "momentum_ratio": last_momentum.momentum_ratio,
            "buy_volume_sol": last_momentum.buy_volume_sol,
            "sell_volume_sol": last_momentum.sell_volume_sol,
            "buy_count": last_momentum.buy_count,
            "sell_count": last_momentum.sell_count,
            "consecutive_buys": last_momentum.consecutive_buys,
            "consecutive_sells": last_momentum.consecutive_sells,
            "max_single_sell_sol": last_momentum.max_single_sell_sol,
            "avg_trade_sol": last_momentum.avg_trade_sol,
            "ticks_per_second": last_momentum.ticks_per_second,
            "sell_accelerating": last_momentum.sell_accelerating,
            "total_ticks": last_momentum.total_ticks,
        },
    });
    {
        let url = format!("{}/positions?id=eq.{}", supabase.base_url, position.position_id);
        let payload = serde_json::json!({ "monitoring_snapshot": monitoring_snapshot });
        let supabase_bg = Arc::clone(&supabase);
        tokio::spawn(async move {
            match supabase_bg.client.patch(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!("Monitoring snapshot written to positions");
                }
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    warn!("Failed to write monitoring snapshot: {}", body);
                }
                Err(e) => {
                    warn!("Failed to write monitoring snapshot: {}", e);
                }
            }
        });
    }

    // Signal Helius watchers to shut down when monitoring ends
    let _ = helius_shutdown_tx.send(true);

    // Notify shadow logger that position exited (it will continue for remaining duration)
    let exit_reason_str = last_exit_reason.unwrap_or_else(|| "closed".to_string());

    // If this was a moonbag promotion, persist exit_reason marker.
    // moonbag_promoted/status are now set by moonbag tracker only after INSERT succeeds.
    if exit_reason_str.contains("moonbag_promoted") {
        let url = format!("{}/positions?id=eq.{}", supabase.base_url, position.position_id);
        let payload = serde_json::json!({
            "exit_reason": &exit_reason_str,
        });
        let supabase_bg = Arc::clone(&supabase);
        let mint_str = position.mint.clone();
        tokio::spawn(async move {
            match supabase_bg.client.patch(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(mint = %mint_str, "Moonbag promotion written to positions table");
                }
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    warn!(mint = %mint_str, "Failed to write moonbag promotion: {}", body);
                }
                Err(e) => {
                    warn!(mint = %mint_str, "Failed to write moonbag promotion: {}", e);
                }
            }
        });
    }

    let _ = shadow_exit_tx.send(Some(exit_reason_str));

    // Tier 3 — schedule post-exit T+1h moonbag check (pure logging).
    // Skip for paper trades only if sampler disabled. Writes to
    // position_enrichment_snapshots with trigger='post_exit_1h' when the
    // post-exit price has exceeded 3× entry.
    if let Some(ctx) = sampler.clone() {
        if cfg.strategy.monitoring.enrichment_post_exit_check_enabled {
            enrichment_sampler::spawn_post_exit_moonbag_check(
                ctx,
                position.position_id,
                position.mint.clone(),
                entry_price_usd,
                position.dev_wallet.clone(),
            );
        }
    }

    // Spawn price tracker for bought positions (counterfactual "what if we held" data).
    // Looks up the sniper_candidates row by mint and spawns the same tracker used for
    // rejected tokens — fills price_1m..price_1h and peak_multiplier on the candidate row.
    {
        let sb = Arc::clone(&supabase);
        let mint_c = position.mint.clone();
        tokio::spawn(async move {
            // Look up candidate_id from sniper_candidates by mint
            let url = format!(
                "{}/sniper_candidates?mint=eq.{}&action=eq.sniper_passed&select=id&order=id.desc&limit=1",
                sb.base_url, mint_c
            );
            if let Ok(resp) = sb.client.get(&url).send().await {
                if let Ok(rows) = resp.json::<Vec<serde_json::Value>>().await {
                    if let Some(cid) = rows.first().and_then(|r| r.get("id")).and_then(|v| v.as_i64()) {
                        crate::sniper::tracker::spawn_rejected_tracker(
                            Arc::clone(&sb),
                            cid,
                            mint_c,
                        );
                    }
                }
            }
        });
    }

    // Spawn post-trade enrichment (creator reputation, Birdeye data, etc.)
    let creator_wallet = position.dev_wallet.clone().unwrap_or_default();
    if !creator_wallet.is_empty() {
        crate::sniper::post_trade::spawn_post_trade(
            Arc::clone(&cfg),
            Arc::clone(&supabase),
            position.position_id,
            position.mint.clone(),
            creator_wallet,
            chrono::Utc::now().timestamp(),
            position.sniper_features.clone(),
        );
    }
}

// ─── Combined promotion scoring ──────────────────────────────

/// Compute on-chain strength score (0-70) from real-time trading signals.
///
/// Components:
///   - Momentum ratio (buy/sell flow): 0-25 pts
///   - Buy count (active buying): 0-15 pts
///   - Buy volume SOL (capital inflow): 0-15 pts
///   - Peak multiplier (price performance): 0-15 pts
#[allow(dead_code)]
fn compute_on_chain_score(
    momentum_ratio: f64,
    buy_count: u32,
    sell_count: u32,
    buy_volume_sol: f64,
    peak_multiplier: f64,
) -> f64 {
    // Momentum ratio: 0.5 = awful, 1.0 = balanced, 2.0+ = strong buying
    let momentum_pts = if momentum_ratio >= 2.0 {
        25.0
    } else if momentum_ratio >= 1.5 {
        20.0
    } else if momentum_ratio >= 1.0 {
        12.0
    } else if momentum_ratio >= 0.7 {
        5.0
    } else {
        0.0
    };

    // Buy count: more active buyers = healthier
    let total_trades = buy_count + sell_count;
    let buy_pts = if total_trades == 0 {
        0.0
    } else if buy_count >= 30 {
        15.0
    } else if buy_count >= 15 {
        10.0
    } else if buy_count >= 8 {
        5.0
    } else {
        0.0
    };

    // Buy volume: capital inflow in SOL (from tick window)
    let volume_pts = if buy_volume_sol >= 5.0 {
        15.0
    } else if buy_volume_sol >= 2.0 {
        10.0
    } else if buy_volume_sol >= 0.5 {
        5.0
    } else {
        0.0
    };

    // Peak multiplier: higher peak = stronger price action
    let peak_pts = if peak_multiplier >= 3.0 {
        15.0
    } else if peak_multiplier >= 2.5 {
        12.0
    } else if peak_multiplier >= 2.0 {
        8.0
    } else if peak_multiplier >= 1.8 {
        4.0 // just hit TP1
    } else {
        0.0
    };

    momentum_pts + buy_pts + volume_pts + peak_pts
}

/// Compute narrative bonus (0-30) from the narrative state.
///
/// Narrative acts as a multiplier, not the gatekeeper.
/// A token with strong on-chain (65+/70) can promote with NoSignal.
/// A token with weak on-chain (30/70) needs RunnerConfirmed to qualify.
#[allow(dead_code)]
fn compute_narrative_bonus(state: NarrativeState) -> f64 {
    match state {
        NarrativeState::NoSignal => 0.0,
        NarrativeState::EarlyAttention => 10.0,
        NarrativeState::ExpandingAttention => 20.0,
        NarrativeState::RunnerConfirmed => 30.0,
    }
}

// ─── Supabase helpers ────────────────────────────────────────

async fn update_peak_price(
    supabase: &SupabaseClient,
    position_id: i64,
    peak_price: f64,
    peak_multiplier: f64,
) {
    let url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
    let payload = serde_json::json!({
        "peak_price_usd": peak_price,
        "peak_multiplier": peak_multiplier,
    });

    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to update peak price: {}", body);
        }
        Err(e) => {
            warn!("Failed to update peak price: {}", e);
        }
    }
}

// ─── Shadow logging ───────────────────────────────────────────────────

/// Shadow-log the full price curve from entry through post-exit.
///
/// Starts polling at buy time and records snapshots to the `shadow_log`
/// Supabase table. When the exit watch fires, it marks subsequent rows
/// as `phase = "post_exit"` and continues for the remaining configured
/// duration. This captures the full “what if we held” curve.
async fn shadow_log_loop(
    price_fetcher: Arc<PriceFetcher>,
    supabase: Arc<SupabaseClient>,
    mint: String,
    position_id: i64,
    entry_price_usd: f64,
    duration_secs: u64,
    exit_rx: tokio::sync::watch::Receiver<Option<String>>,
) {
    info!(
        mint = %mint,
        position_id,
        duration_secs,
        "👻 Shadow logging started from ENTRY"
    );

    let started = Instant::now();
    let mut shadow_peak = 0.0_f64;
    let mut shadow_low = f64::MAX;
    let mut tick_count: u64 = 0;
    let mut exit_elapsed_secs: Option<u64> = None;
    let mut last_flush_secs: u64 = 0;
    let mut snapshots: Vec<serde_json::Value> = Vec::new();

    // INSERT the initial shadow_log row for this position
    let insert_url = format!("{}/shadow_log", supabase.base_url);
    let insert_payload = serde_json::json!({
        "position_id": position_id,
        "mint": mint,
        "entry_price_usd": entry_price_usd,
    });
    let row_inserted = match supabase.client.post(&insert_url).json(&insert_payload).send().await {
        Ok(resp) if resp.status().is_success() => true,
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, position_id, "Shadow log INSERT failed: {}", body);
            false
        }
        Err(e) => {
            warn!(mint = %mint, position_id, "Shadow log INSERT error: {}", e);
            false
        }
    };

    if !row_inserted {
        warn!(mint = %mint, position_id, "Shadow log row not created — skipping shadow logging");
        return;
    }

    let patch_url = format!(
        "{}/shadow_log?position_id=eq.{}",
        supabase.base_url, position_id
    );

    // Active phase: 5s ticks (matches monitoring cadence).
    // Post-exit phase: 30s ticks (we only need a price curve, not tick precision)
    // — keeps Jupiter RPS bounded when many positions tail in parallel.
    let active_interval = Duration::from_secs(5);
    let post_exit_interval = Duration::from_secs(30);

    loop {
        let poll_interval = if exit_elapsed_secs.is_some() {
            post_exit_interval
        } else {
            active_interval
        };
        tokio::time::sleep(poll_interval).await;

        let elapsed_secs = started.elapsed().as_secs();

        // Check if position has exited
        if exit_elapsed_secs.is_none() {
            if exit_rx.borrow().is_some() {
                exit_elapsed_secs = Some(elapsed_secs);
                info!(
                    mint = %mint,
                    position_id,
                    exit_at_secs = elapsed_secs,
                    "👻 Position exited — shadow logging continues post-exit"
                );
            }
        }

        // Stop condition: duration_secs after entry (covers both active + post-exit)
        if elapsed_secs >= duration_secs {
            break;
        }

        // Shadow logs poll every 5s.
        // ACTIVE phase: cache-first (WS feeds real-time prices), fall back to Jupiter.
        // POST-EXIT phase: WS subscription is dropped so cache is stale — always
        // use Jupiter API to get fresh prices.
        let mut price = if exit_elapsed_secs.is_some() {
            // Post-exit: skip stale cache, go straight to Jupiter
            price_fetcher.get_price(&mint).await
        } else {
            let cached = price_fetcher.get_monitoring_price(&mint);
            if cached <= 0.0 {
                price_fetcher.get_price(&mint).await
            } else {
                cached
            }
        };
        if price <= 0.0 {
            continue;
        }

        tick_count += 1;
        if price > shadow_peak {
            shadow_peak = price;
        }
        if price < shadow_low {
            shadow_low = price;
        }

        let multiplier_from_entry = if entry_price_usd > 0.0 {
            price / entry_price_usd
        } else {
            0.0
        };
        let peak_multiplier = if entry_price_usd > 0.0 {
            shadow_peak / entry_price_usd
        } else {
            0.0
        };

        let phase = if exit_elapsed_secs.is_some() {
            "post_exit"
        } else {
            "active"
        };

        // Accumulate snapshot in memory
        snapshots.push(serde_json::json!({
            "t": elapsed_secs,
            "p": price,
            "m": multiplier_from_entry,
            "phase": phase,
        }));

        // Flush cadence: every 30s during active phase, every 5min post-exit.
        // Each PATCH re-sends the full (growing) snapshot array, so we throttle
        // post-exit to keep network/Supabase write amplification bounded over 24h.
        let flush_period_secs: u64 = if exit_elapsed_secs.is_some() { 300 } else { 30 };
        let should_flush = tick_count == 1
            || elapsed_secs.saturating_sub(last_flush_secs) >= flush_period_secs;
        if should_flush {
            last_flush_secs = elapsed_secs;
            let payload = serde_json::json!({
                "snapshots": snapshots,
                "shadow_peak_usd": shadow_peak,
                "shadow_peak_multiplier": peak_multiplier,
                "shadow_low_usd": if shadow_low == f64::MAX { 0.0 } else { shadow_low },
                "total_ticks": tick_count,
                "exit_at_secs": exit_elapsed_secs,
            });

            match supabase.client.patch(&patch_url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {}
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    warn!(mint = %mint, position_id, "Shadow log PATCH failed: {}", body);
                }
                Err(e) => {
                    warn!(mint = %mint, position_id, "Shadow log PATCH error: {}", e);
                }
            }
        }
    }

    // Final summary + completion write
    let final_multiplier = if entry_price_usd > 0.0 {
        shadow_peak / entry_price_usd
    } else {
        0.0
    };

    let final_payload = serde_json::json!({
        "snapshots": snapshots,
        "shadow_peak_usd": shadow_peak,
        "shadow_peak_multiplier": final_multiplier,
        "shadow_low_usd": if shadow_low == f64::MAX { 0.0 } else { shadow_low },
        "total_ticks": tick_count,
        "exit_at_secs": exit_elapsed_secs,
        "exit_reason": exit_rx.borrow().as_deref().unwrap_or("unknown"),
        "duration_secs": started.elapsed().as_secs(),
        "completed_at": chrono::Utc::now().to_rfc3339(),
    });

    match supabase.client.patch(&patch_url).json(&final_payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            info!(
                mint = %mint,
                position_id,
                shadow_peak_usd = format!("{:.10}", shadow_peak),
                shadow_peak_multiplier = format!("{:.2}x", final_multiplier),
                shadow_low_usd = format!("{:.10}", if shadow_low == f64::MAX { 0.0 } else { shadow_low }),
                ticks = tick_count,
                exit_at_secs = ?exit_elapsed_secs,
                "👻 Shadow logging complete — final row written"
            );
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, position_id, "Shadow log final PATCH failed: {}", body);
        }
        Err(e) => {
            warn!(mint = %mint, position_id, "Shadow log final PATCH error: {}", e);
        }
    }
}

/// Correct a broken entry price in Supabase (when price was near-zero at buy time).
async fn update_entry_price(
    supabase: &SupabaseClient,
    position_id: i64,
    entry_price_usd: f64,
) {
    let url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
    let payload = serde_json::json!({
        "entry_price_usd": entry_price_usd,
        "peak_price_usd": entry_price_usd,
        "peak_multiplier": 1.0,
    });

    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            tracing::info!(position_id, entry_price_usd, "✅ Corrected entry price in DB");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to correct entry price: {}", body);
        }
        Err(e) => {
            warn!("Failed to correct entry price: {}", e);
        }
    }
}

async fn update_tp_flags(
    supabase: &SupabaseClient,
    position_id: i64,
    tp1: bool,
    tp2: bool,
) {
    let url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
    let payload = serde_json::json!({
        "tp1_triggered": tp1,
        "tp2_triggered": tp2,
    });

    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to update TP flags: {}", body);
        }
        Err(e) => {
            warn!("Failed to update TP flags: {}", e);
        }
    }
}

// ─── Dev wallet monitoring helpers ───────────────────────────

async fn check_dev_wallet(
    rpc: &RpcClient,
    dev_wallet: &Pubkey,
    mint: &Pubkey,
    initial_dev_balance: u64,
    dev_dump_threshold_pct: f64,
) -> DevWalletStatus {
    let token_accounts = match rpc
        .get_token_accounts_by_owner(
            dev_wallet,
            solana_client::rpc_request::TokenAccountsFilter::Mint(*mint),
        )
        .await
    {
        Ok(accounts) => accounts,
        Err(e) => {
            warn!(
                dev_wallet = %dev_wallet,
                mint = %mint,
                "Dev wallet balance fetch failed: {} — skipping check",
                e
            );
            return DevWalletStatus::Unknown;
        }
    };

    let current_balance: u64 = token_accounts
        .iter()
        .filter_map(|account| {
            let data: serde_json::Value =
                serde_json::to_value(&account.account.data).ok()?;
            data.get("parsed")
                .and_then(|p| p.get("info"))
                .and_then(|i| i.get("tokenAmount"))
                .and_then(|t| t.get("amount"))
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse::<u64>().ok())
        })
        .sum();

    if initial_dev_balance == 0 {
        return DevWalletStatus::Stable;
    }

    let drop_pct = if current_balance < initial_dev_balance {
        (initial_dev_balance - current_balance) as f64 / initial_dev_balance as f64 * 100.0
    } else {
        0.0
    };

    debug!(
        dev_wallet = %dev_wallet,
        mint = %mint,
        initial_dev_balance,
        current_balance,
        drop_pct = format!("{:.1}%", drop_pct),
        "Dev wallet balance check"
    );

    if current_balance == 0 {
        // Dev sold 100% of tokens → Community Takeover signal (bullish)
        DevWalletStatus::CTO
    } else if drop_pct > dev_dump_threshold_pct {
        DevWalletStatus::Dumping { drop_pct }
    } else {
        DevWalletStatus::Stable
    }
}

async fn log_dev_dump_event(supabase: &SupabaseClient, mint: &str, drop_pct: f64) {
    let payload = serde_json::json!({
        "event_type": "dev_wallet_dump_detected",
        "message": format!("Dev sold {:.1}% of holdings for mint {}", drop_pct, mint),
    });
    let url = format!("{}/system_events", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to log dev dump event: {}", body);
        }
        Err(e) => {
            warn!("Failed to log dev dump event: {}", e);
        }
    }
}
