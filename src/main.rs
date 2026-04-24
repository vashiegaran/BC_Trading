mod config;
mod detection;
mod execution;
mod exit;
mod filters;
mod logger;
mod monitoring;
mod narrative;
mod reentry;
mod sniper;

/// Captures a build timestamp at compile time via env vars set by Cargo.
/// Falls back to a manual marker so you can always tell which build you're running.
macro_rules! compile_time_stamp {
    () => {
        "2026-03-16-v7 | dynamicSlippage_range, strip_quoteBps, dynamicCU"
    };
}

use config::AppConfig;
use execution::types::PositionOpened;
use execution::wallet::BotWallet;
use logger::SupabaseClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::native_token::lamports_to_sol;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

/// Global shutdown flag — set by Ctrl+C handler.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Clean up old records from Supabase tables on startup.
/// Runs DELETE queries to prune stale data so tables don't grow unbounded.
async fn cleanup_old_records(supabase: &SupabaseClient) {
    let tables: &[(&str, &str, i64)] = &[
        ("tokens_seen", "detected_at", 7),
        ("filter_results", "checked_at", 30),
        ("system_events", "occurred_at", 14),
    ];

    for &(table, column, days) in tables {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(days);
        let cutoff_str = cutoff.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let url = format!("{}/{}?{}=lt.{}", supabase.base_url, table, column, cutoff_str);

        match supabase.client.delete(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                info!(table = table, days = days, "Cleaned up old records");
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                warn!(
                    table = table,
                    "Cleanup failed: HTTP {} — {}",
                    status,
                    body
                );
            }
            Err(e) => {
                warn!(table = table, "Cleanup failed: {}", e);
            }
        }
    }
}

/// Log a shutdown event to Supabase.
async fn log_shutdown_event(supabase: &SupabaseClient) {
    let payload = serde_json::json!({
        "event_type": "shutdown",
        "message": "Bot shutting down gracefully.",
    });
    let url = format!("{}/system_events", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            info!("Shutdown event logged to Supabase");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to log shutdown event: {}", body);
        }
        Err(e) => {
            warn!("Failed to log shutdown event: {}", e);
        }
    }
}

#[tokio::main]
async fn main() {
    // ── 1. Load config ──────────────────────────────────
    let cfg = AppConfig::load();

    // ── 2. Initialize tracing ───────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&cfg.env.log_level)),
        )
        .init();

    info!("Config loaded successfully");

    // ── 3. Load wallet (SAFETY Rule 1: never log private key) ──
    let wallet = BotWallet::from_env(&cfg.env.wallet_private_key)
        .expect("Failed to load wallet from WALLET_PRIVATE_KEY");
    let pubkey = wallet.pubkey();
    let wallet = Arc::new(wallet);

    // Build version compiled into the binary at compile time
    const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");
    const BUILD_TIMESTAMP: &str = compile_time_stamp!();

    println!("═══════════════════════════════════════════════════");
    println!("  Build version  : {} (built {})", BUILD_VERSION, BUILD_TIMESTAMP);
    println!("  Wallet address : {}", pubkey);
    println!("  Paper trade    : {}", cfg.env.paper_trade);
    println!("  Network        : {}", cfg.env.solana_network);

    // ── 4. Initialize Supabase logger client ────────────
    let supabase = SupabaseClient::init(
        &cfg.env.supabase_url,
        &cfg.env.supabase_service_key,
    )
    .await;

    info!("Supabase client initialized");

    // ── 4b. Auto-cleanup stale records ──────────────────
    cleanup_old_records(&supabase).await;
    info!("Startup cleanup complete");

    // ── 5. Log startup event to Supabase system_events ──
    let startup_payload = serde_json::json!({
        "event_type": "startup",
        "message": format!(
            "Bot started. Build: {}. Wallet: {}. Network: {}. Paper trade: {}.",
            BUILD_TIMESTAMP, pubkey, cfg.env.solana_network, cfg.env.paper_trade
        ),
    });

    let url = format!("{}/system_events", supabase.base_url);
    match supabase.client.post(&url).json(&startup_payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            info!("Startup event logged to Supabase");
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            error!(
                "Failed to log startup event: HTTP {} — {}",
                status, body
            );
        }
        Err(e) => {
            error!("Failed to log startup event: {}", e);
        }
    }

    // ── 6. Check wallet SOL balance via RPC ─────────────
    let rpc = RpcClient::new(cfg.env.solana_rpc_url.clone());
    let mut startup_balance_sol: Option<f64> = None;

    match rpc.get_balance(&pubkey).await {
        Ok(lamports) => {
            let sol = lamports_to_sol(lamports);
            startup_balance_sol = Some(sol);
            println!("  SOL balance    : {:.4} SOL", sol);
            info!("Wallet SOL balance: {:.4} SOL", sol);

            if sol < cfg.strategy.risk.min_sol_balance {
                if cfg.env.paper_trade {
                    warn!(
                        "⚠️ SOL balance ({:.4}) is below min_sol_balance ({:.4}) — continuing in paper mode",
                        sol, cfg.strategy.risk.min_sol_balance
                    );
                } else {
                    panic!(
                        "REFUSING TO START: SOL balance ({:.4}) is below min_sol_balance ({:.4}). \
                         Top up the wallet before running in live mode.",
                        sol, cfg.strategy.risk.min_sol_balance
                    );
                }
            }
        }
        Err(e) => {
            error!("Failed to fetch SOL balance from RPC: {}", e);
            println!("  SOL balance    : ERROR — {}", e);
        }
    }

    // Log startup balance to Supabase system_events
    if let Some(bal) = startup_balance_sol {
        let balance_payload = serde_json::json!({
            "event_type": "startup_balance",
            "message": format!("Starting SOL balance: {:.4}", bal),
        });
        let balance_url = format!("{}/system_events", supabase.base_url);
        let _ = supabase.client.post(&balance_url).json(&balance_payload).send().await;
    }

    println!("═══════════════════════════════════════════════════");

    let cfg_arc = Arc::new(cfg);
    let supabase_arc = Arc::new(supabase);

    // ── 6b. Initialize in-memory trading state ──────────
    let trading_state = execution::state::TradingState::new();
    trading_state.hydrate_from_supabase(&supabase_arc, cfg_arc.env.paper_trade).await;

    // ── 6c. Recover rejected-token trackers interrupted by restart ──
    sniper::tracker::recover_pending_trackers(Arc::clone(&supabase_arc)).await;

    // ── 7. Start detection engine ───────────────────────
    let (detection_rx, bc_cache) = detection::start(Arc::clone(&cfg_arc), Arc::clone(&supabase_arc));
    info!("Detection engine started — listening for new tokens");

    // ── 7b. Start sniper enrichment pipeline ────────────
    let enriched_rx = sniper::start(
        Arc::clone(&cfg_arc),
        detection_rx,
        Arc::clone(&supabase_arc),
        bc_cache,
    );
    info!("Sniper enrichment pipeline started — enriching & hard-filtering tokens");

    // ── 8. Start filter engine ──────────────────────────
    let filter_rx = filters::start(
        Arc::clone(&cfg_arc),
        enriched_rx,
        Arc::clone(&supabase_arc),
        Arc::clone(&wallet),
    );
    info!("Filter engine started — screening graduated tokens");

    // ── 9. Start execution engine ───────────────────────
    let (position_rx, alert_rx) = execution::start(
        Arc::clone(&cfg_arc),
        filter_rx,
        Arc::clone(&supabase_arc),
        Arc::clone(&wallet),
        Arc::clone(&trading_state),
    );
    info!("Execution engine started — {} mode",
        if cfg_arc.env.paper_trade { "PAPER" } else { "LIVE" }
    );

    // ── 10. Start moonbag tracker ──────────────────────
    let (moonbag_tx, moonbag_rx) = mpsc::channel::<monitoring::moonbag::MoonbagCommand>(50);

    // ── 10b. Start monitoring engine ─────────────────
    let (exit_rx, confirm_tx, recovery_tx, exit_tx_for_moonbag) = monitoring::start(
        Arc::clone(&cfg_arc),
        position_rx,
        Arc::clone(&supabase_arc),
        Arc::clone(&trading_state),
        alert_rx,
        moonbag_tx.clone(),
    );
    info!("Monitoring engine started — watching open positions");

    // ── 10c. Spawn moonbag tracker task ──────────────
    {
        let mb_cfg = Arc::clone(&cfg_arc);
        let mb_supa = Arc::clone(&supabase_arc);
        tokio::spawn(async move {
            monitoring::moonbag::run_moonbag_tracker(
                moonbag_rx,
                exit_tx_for_moonbag,
                mb_cfg,
                mb_supa,
            ).await;
        });
        info!("Moonbag tracker started — {} max concurrent",
            cfg_arc.strategy.monitoring.moonbag_max_concurrent
        );
    }

    // ── 11. Start exit engine ───────────────────────────
    exit::start(
        Arc::clone(&cfg_arc),
        exit_rx,
        Arc::clone(&supabase_arc),
        Arc::clone(&wallet),
        confirm_tx,
        Arc::clone(&trading_state),
    );
    info!("Exit engine started — ready to process exit signals");

    // ── 11a. Start re-entry shadow watcher (post-moonbag exit) ──
    reentry::start(Arc::clone(&cfg_arc), Arc::clone(&supabase_arc));

    // ── 11b. Recover stuck positions from previous runs ─
    recover_stuck_positions(
        &cfg_arc,
        &supabase_arc,
        &wallet,
        &recovery_tx,
    )
    .await;

    // ── 12. Register Ctrl+C handler (SAFETY Rule 8) ─────
    let shutdown_supabase = Arc::clone(&supabase_arc);
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");

        info!("🛑 Ctrl+C received — initiating graceful shutdown");
        SHUTDOWN.store(true, Ordering::SeqCst);

        // Log shutdown event to Supabase
        log_shutdown_event(&shutdown_supabase).await;

        // Give monitoring tasks time to fire exit signals for open positions
        // before the process exits. 10s is enough for paper; increase for live
        // if positions need time to send real transactions.
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        info!("Shutdown complete. Goodbye!");
        std::process::exit(0);
    });

    // ── 13. Keep main alive ─────────────────────────────
    // The bot runs until Ctrl+C or all channels close.
    info!("🚀 All engines running. Press Ctrl+C to shut down.");

    // Park the main task — engines run in their own spawned tasks.
    // We use a sleep loop so that the Ctrl+C handler can do
    // cleanup before calling process::exit.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        if SHUTDOWN.load(Ordering::SeqCst) {
            break;
        }
    }
}

// ─── Startup recovery for stuck positions ────────────────────

/// Query Supabase for positions with status `open` or `exit_failed`
/// that were orphaned by a previous shutdown. For each:
///   - Check on-chain token balance
///   - If tokens remain → re-inject into monitoring (which will trigger exit)
///   - If balance is 0  → mark as closed with realised loss
async fn recover_stuck_positions(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    wallet: &BotWallet,
    recovery_tx: &mpsc::Sender<PositionOpened>,
) {
    let rpc = RpcClient::new(cfg.env.solana_rpc_url.clone());

    // Query positions that need recovery (include sol_received for partial TP accounting)
    let url = format!(
        "{}/positions?select=id,mint,entry_price_usd,sol_spent,token_amount,is_paper_trade,status,pool_address,dev_wallet,sol_received&or=(status.eq.open,status.eq.exit_failed)&is_paper_trade=eq.false",
        supabase.base_url
    );

    let rows: Vec<serde_json::Value> = match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => resp.json().await.unwrap_or_default(),
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to fetch stuck positions: {}", body);
            return;
        }
        Err(e) => {
            warn!("Failed to fetch stuck positions: {}", e);
            return;
        }
    };

    if rows.is_empty() {
        info!("No stuck positions to recover");
        return;
    }

    info!(count = rows.len(), "🔄 Found stuck positions — starting recovery");

    for row in &rows {
        let position_id = row.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
        let mint = row.get("mint").and_then(|v| v.as_str()).unwrap_or("");
        let entry_price = row.get("entry_price_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let sol_spent = row.get("sol_spent").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let _db_token_amount = row.get("token_amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("");

        if mint.is_empty() || position_id == 0 {
            continue;
        }

        // Check on-chain token balance
        let on_chain_balance = fetch_recovery_token_balance(
            &rpc,
            &wallet.pubkey(),
            mint,
        )
        .await
        .unwrap_or(0.0);

        if on_chain_balance <= 0.0 {
            // No tokens on-chain — mark as closed.
            // Preserve sol_received from any partial TP sells that already executed.
            let prev_sol_received = row.get("sol_received")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let actual_pnl_sol = prev_sol_received - sol_spent;
            let actual_pnl_pct = if sol_spent > 0.0 {
                actual_pnl_sol / sol_spent * 100.0
            } else {
                -100.0
            };

            info!(
                position_id,
                mint,
                prev_status = %status,
                prev_sol_received,
                actual_pnl_pct = format!("{:.2}", actual_pnl_pct),
                "Position has 0 on-chain balance — marking as closed (tokens already gone, e.g. rug or prior successful sell)"
            );

            let close_url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
            let payload = serde_json::json!({
                "status": "closed",
                "exit_reason": format!("recovery_closed: {} with 0 on-chain balance", status),
                "exit_time": chrono::Utc::now().to_rfc3339(),
                "sol_received": prev_sol_received,
                "pnl_sol": actual_pnl_sol,
                "pnl_pct": actual_pnl_pct,
                "token_amount": 0,
            });
            match supabase.client.patch(&close_url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(position_id, "Position closed via recovery");
                }
                _ => warn!(position_id, "Failed to close position via recovery"),
            }
            continue;
        }

        // Tokens still on-chain — always use on-chain balance (raw units) as
        // authoritative, update DB, and re-monitor
        info!(
            position_id,
            mint,
            on_chain_balance,
            "Updating token_amount from chain and re-monitoring"
        );
        let fix_url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
        let payload = serde_json::json!({ "token_amount": on_chain_balance, "status": "open" });
        let _ = supabase.client.patch(&fix_url).json(&payload).send().await;
        let token_amount = on_chain_balance;

        info!(
            position_id,
            mint,
            token_amount,
            "Re-injecting position into monitoring"
        );

        let pool_address = row.get("pool_address").and_then(|v| v.as_str()).map(|s| s.to_string());
        let dev_wallet = row.get("dev_wallet").and_then(|v| v.as_str()).map(|s| s.to_string());

        let opened = PositionOpened {
            position_id,
            mint: mint.to_string(),
            entry_price_usd: entry_price,
            sol_spent,
            token_amount,
            is_paper_trade: false,
            dev_wallet,
            dev_initial_balance: None,
            pool_address,
            sniper_features: None,
            initial_liquidity_sol: 0.0, // unknown for recovered positions
            detection_source: "recovery".to_string(),
            token_name: String::new(),
            token_symbol: String::new(),
        };

        if recovery_tx.send(opened).await.is_err() {
            warn!(position_id, "Recovery → monitoring channel closed");
            break;
        }
    }

    info!("Startup recovery complete");
}

/// Fetch on-chain SPL token balance (raw smallest units) for recovery.
async fn fetch_recovery_token_balance(
    rpc: &RpcClient,
    wallet: &solana_sdk::pubkey::Pubkey,
    mint_str: &str,
) -> Option<f64> {
    let mint = solana_sdk::pubkey::Pubkey::from_str(mint_str).ok()?;

    let accounts = rpc
        .get_token_accounts_by_owner(
            wallet,
            solana_client::rpc_request::TokenAccountsFilter::Mint(mint),
        )
        .await
        .ok()?;

    let balance: f64 = accounts
        .iter()
        .filter_map(|account| {
            let data: serde_json::Value =
                serde_json::to_value(&account.account.data).ok()?;
            data.get("parsed")
                .and_then(|p| p.get("info"))
                .and_then(|i| i.get("tokenAmount"))
                .and_then(|t| t.get("amount"))
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse::<f64>().ok())
        })
        .sum();

    if balance > 0.0 { Some(balance) } else { None }
}
