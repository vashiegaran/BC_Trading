use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use super::types::{DetectionSource, GraduatedToken, PipelineTiming, WatchlistEntry, BcScoreCache, BcScoreEntry, compute_bc_score, prune_bc_score_cache};
use crate::config::AppConfig;
use crate::logger::SupabaseClient;

/// Public pump.fun portal WebSocket endpoint.
const PUMPFUN_WS_URL: &str = "wss://pumpportal.fun/api/data";

/// Maximum back-off duration between reconnection attempts.
const MAX_BACKOFF_SECS: u64 = 30;

/// Initial back-off duration after the first disconnect.
const INITIAL_BACKOFF_SECS: u64 = 1;

/// Maximum number of tokens held in the watchlist before the oldest
/// entries are pruned to prevent unbounded memory growth.
const MAX_WATCHLIST_SIZE: usize = 5_000;

/// Run the pump.fun WebSocket listener.
///
/// Connects to the PumpPortal WebSocket, subscribes to `newToken`,
/// `tokenTrade`, and `migration` (graduation) events.
///
/// - **newToken**: adds a token to an in-memory watchlist (does NOT
///   forward to the filter engine).
/// - **tokenTrade**: updates volume / buy-sell counts on the watchlist
///   entry for the traded mint.
/// - **tokenComplete** (graduation): builds a [`GraduatedToken`] with
///   all aggregated data and sends it through the MPSC channel.
///
/// On any connection or read error the function reconnects with
/// exponential back-off (1 s → 2 s → 4 s … capped at 30 s).
pub async fn run(tx: mpsc::Sender<GraduatedToken>, supabase: Arc<SupabaseClient>, rpc_url: String, cfg: Arc<AppConfig>, bc_cache: BcScoreCache) {
    let mut backoff_secs = INITIAL_BACKOFF_SECS;

    loop {
        info!("Connecting to PumpFun WebSocket \u{2026}");

        match connect_and_listen(&tx, &supabase, &rpc_url, &cfg, &bc_cache).await {
            Ok(()) => {
                // Clean close (shouldn't normally happen); reset back-off.
                warn!("PumpFun WebSocket closed cleanly — reconnecting");
                backoff_secs = INITIAL_BACKOFF_SECS;
            }
            Err(e) => {
                error!("PumpFun WebSocket error: {:#}. Reconnecting in {}s …", e, backoff_secs);
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;

        // Exponential back-off: double each time, cap at MAX_BACKOFF_SECS.
        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
    }
}

/// Inner helper: connect once, subscribe, and read messages until error.
async fn connect_and_listen(tx: &mpsc::Sender<GraduatedToken>, supabase: &Arc<SupabaseClient>, rpc_url: &str, cfg: &Arc<AppConfig>, bc_cache: &BcScoreCache) -> Result<()> {
    let (ws_stream, _response) = connect_async(PUMPFUN_WS_URL)
        .await
        .context("Failed to connect to PumpFun WebSocket")?;

    info!("Connected to PumpFun WebSocket");

    let (mut write, mut read) = ws_stream.split();

    // ── Subscribe to newToken & migration only ────────────
    // Trade subscriptions happen per-token inside handle_new_token.
    let subscriptions = [
        r#"{"method":"subscribeNewToken","keys":[]}"#,
        r#"{"method":"subscribeMigration","keys":[]}"#,
    ];

    for payload in &subscriptions {
        write
            .send(Message::Text(payload.to_string()))
            .await
            .context("Failed to send subscribe payload")?;
    }

    info!("Subscribed to PumpFun newToken + migration (trade subs are per-token)");

    // Channel for outgoing WebSocket messages (per-token subscriptions, pong frames).
    let (ws_write_tx, mut ws_write_rx) = mpsc::channel::<Message>(256);

    // Spawn a writer task that forwards channel messages to the WS write half.
    tokio::spawn(async move {
        while let Some(msg) = ws_write_rx.recv().await {
            if let Err(e) = write.send(msg).await {
                error!("PumpFun WS writer error: {}", e);
                break;
            }
        }
        debug!("PumpFun WS writer task exiting");
    });

    // In-memory watchlist: mint (base58 string) → WatchlistEntry.
    // Populated by newToken, enriched by tokenTrade, consumed by tokenComplete.
    let mut watchlist: HashMap<String, WatchlistEntry> = HashMap::new();

    // ── Read loop ─────────────────────────────────────────
    while let Some(msg_result) = read.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                return Err(anyhow::anyhow!("WebSocket read error: {}", e));
            }
        };

        match msg {
            Message::Text(text) => {
                if let Err(e) = handle_text_message(&text, tx, &mut watchlist, supabase, &ws_write_tx, rpc_url, cfg, bc_cache).await {
                    // Log parse errors but keep the connection alive.
                    warn!("Failed to process PumpFun message: {:#}", e);
                }
            }
            Message::Ping(payload) => {
                // Respond with Pong via the writer task.
                if let Err(e) = ws_write_tx.send(Message::Pong(payload)).await {
                    return Err(anyhow::anyhow!("WS writer channel closed (pong): {}", e));
                }
            }
            Message::Close(_) => {
                info!("PumpFun WebSocket sent Close frame");
                return Ok(());
            }
            _ => {
                // Binary / Pong / Frame — ignore silently
            }
        }
    }

    // Stream ended without a Close frame.
    Err(anyhow::anyhow!("PumpFun WebSocket stream ended unexpectedly"))
}

// ─── Event type detection ────────────────────────────────────────────────────

/// Determine the event type from a PumpPortal JSON message.
///
/// PumpPortal messages use a `txType` field:
/// - `"create"`   → newToken
/// - `"buy"` / `"sell"` → tokenTrade
/// - `"migrate"` → migration (graduation to Raydium)
fn classify_event(v: &serde_json::Value) -> EventKind {
    match v.get("txType").and_then(|t| t.as_str()) {
        Some("create") => EventKind::NewToken,
        Some("buy") | Some("sell") => EventKind::TokenTrade,
        Some("migrate") | Some("migration") | Some("complete") => EventKind::TokenComplete,
        _ => EventKind::Unknown,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum EventKind {
    NewToken,
    TokenTrade,
    TokenComplete,
    Unknown,
}

// ─── Message handler ─────────────────────────────────────────────────────────

/// Route a text message to the appropriate handler based on event type.
async fn handle_text_message(
    text: &str,
    tx: &mpsc::Sender<GraduatedToken>,
    watchlist: &mut HashMap<String, WatchlistEntry>,
    supabase: &Arc<SupabaseClient>,
    ws_write_tx: &mpsc::Sender<Message>,
    rpc_url: &str,
    cfg: &Arc<AppConfig>,
    bc_cache: &BcScoreCache,
) -> Result<()> {
    let v: serde_json::Value =
        serde_json::from_str(text).context("Message is not valid JSON")?;

    let kind = classify_event(&v);

    // Log raw graduation payload so we can inspect the exact field names
    if kind == EventKind::TokenComplete {
        info!("\n\n====================================================\n  RAW GRADUATION EVENT\n====================================================\n{}\n====================================================\n", serde_json::to_string_pretty(&v).unwrap_or_default());
    }

    match kind {
        EventKind::NewToken => handle_new_token(&v, watchlist, ws_write_tx).await?,
        EventKind::TokenTrade => handle_token_trade(&v, watchlist, cfg, supabase, bc_cache).await?,
        EventKind::TokenComplete => handle_token_complete(&v, tx, watchlist, supabase, rpc_url).await?,
        EventKind::Unknown => {
            // Subscription acks have no txType — only log events that have one
            if let Some(tx_type) = v.get("txType").and_then(|t| t.as_str()) {
                info!("UNKNOWN EVENT txType={} payload={}", tx_type, serde_json::to_string_pretty(&v).unwrap_or_default());
            }
        }
    }

    Ok(())
}

// ─── 1. newToken ─────────────────────────────────────────────────────────────

/// Add the newly created token to the in-memory watchlist and subscribe
/// to its trade events on the PumpPortal WebSocket.
/// Does **not** send anything downstream — the token is not on Raydium yet.
async fn handle_new_token(
    v: &serde_json::Value,
    watchlist: &mut HashMap<String, WatchlistEntry>,
    ws_write_tx: &mpsc::Sender<Message>,
) -> Result<()> {
    let mint_str = v
        .get("mint")
        .and_then(|m| m.as_str())
        .ok_or_else(|| anyhow::anyhow!("newToken: missing 'mint' field"))?;

    let mint = Pubkey::from_str(mint_str)
        .map_err(|e| anyhow::anyhow!("newToken: invalid mint '{}': {}", mint_str, e))?;

    let creator_wallet = v
        .get("traderPublicKey")
        .and_then(|c| c.as_str())
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or_default();

    let initial_buy_sol = v
        .get("initialBuy")
        .and_then(|b| b.as_f64())
        .unwrap_or(0.0);

    let name = v
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();

    let symbol = v
        .get("symbol")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    let now = chrono::Utc::now().timestamp_millis();

    // Prune watchlist if it's getting too large (simple eviction: clear oldest half).
    if watchlist.len() >= MAX_WATCHLIST_SIZE {
        let mut entries: Vec<(String, i64)> = watchlist
            .iter()
            .map(|(k, v)| (k.clone(), v.detected_at))
            .collect();
        entries.sort_by_key(|(_k, ts)| *ts);
        let to_remove = entries.len() / 2;
        for (key, _) in entries.into_iter().take(to_remove) {
            watchlist.remove(&key);
        }
        info!(remaining = watchlist.len(), "Pruned watchlist (exceeded max size)");
    }

    // NOTE: `initialBuy` from PumpFun is the TOKEN quantity purchased,
    // NOT a SOL amount.  We store it for reference but must NOT seed
    // total_volume_sol or trade_log with it — those track real SOL flow
    // from subsequent tokenTrade events.
    let entry = WatchlistEntry {
        mint,
        creator_wallet,
        detected_at: now,
        initial_buy_sol,
        name,
        symbol,
        total_volume_sol: 0.0,
        buy_count: 0,
        sell_count: 0,
        unique_buyers: std::collections::HashSet::new(),
        trade_timestamps: vec![],
        trade_log: vec![],
        signal_recorded: false,
        progress_signal_recorded: false,
        last_v_sol_reserves: 0.0,
        last_v_token_reserves: 0.0,
        last_market_cap_sol: 0.0,
    };


    watchlist.insert(mint_str.to_string(), entry);

    // ── Subscribe to trade events for this specific token ──
    let sub_payload = format!(
        r#"{{"method":"subscribeTokenTrade","keys":["{}"]}}"#,
        mint_str
    );
    ws_write_tx
        .send(Message::Text(sub_payload))
        .await
        .context("Failed to send per-token trade subscription")?;

    Ok(())
}

// ─── 2. tokenTrade ───────────────────────────────────────────────────────────

/// Update volume and buy/sell counts for a token already in the watchlist.
async fn handle_token_trade(
    v: &serde_json::Value,
    watchlist: &mut HashMap<String, WatchlistEntry>,
    cfg: &Arc<AppConfig>,
    supabase: &Arc<SupabaseClient>,
    bc_cache: &BcScoreCache,
) -> Result<()> {
    let mint_str = v
        .get("mint")
        .and_then(|m| m.as_str())
        .ok_or_else(|| anyhow::anyhow!("tokenTrade: missing 'mint' field"))?;

    // Only update tokens we're already tracking.
    let entry = match watchlist.get_mut(mint_str) {
        Some(e) => e,
        None => {
            debug!(mint = mint_str, "tokenTrade for unknown mint — skipping");
            return Ok(());
        }
    };

    let sol_amount = v
        .get("solAmount")
        .and_then(|a| a.as_f64())
        .unwrap_or(0.0);

    let tx_type = v
        .get("txType")
        .and_then(|t| t.as_str())
        .unwrap_or("");

    let trader = v
        .get("traderPublicKey")
        .and_then(|t| t.as_str())
        .and_then(|s| Pubkey::from_str(s).ok());

    let now = chrono::Utc::now().timestamp_millis();

    // Skip the creator's first tokenTrade that echoes `initialBuy` (a
    // TOKEN quantity, not SOL).  These values are typically >1000 while
    // real SOL amounts are <500.
    let is_initial_buy_echo = entry.initial_buy_sol > 0.0
        && sol_amount > 1_000.0
        && (sol_amount - entry.initial_buy_sol).abs() < 1.0;
    if is_initial_buy_echo {
        debug!(
            mint = mint_str,
            sol_amount,
            initial_buy = entry.initial_buy_sol,
            "Skipping creator initialBuy echo (token qty, not SOL)"
        );
        return Ok(());
    }

    entry.total_volume_sol += sol_amount;

    // Snapshot live BC reserves from this trade event (PumpPortal payload).
    // These are how we compute bc_progress_pct without calling pump.fun REST.
    if let Some(v) = v.get("vSolInBondingCurve").and_then(|x| x.as_f64()) {
        if v > 0.0 {
            entry.last_v_sol_reserves = v;
        }
    }
    if let Some(v) = v.get("vTokensInBondingCurve").and_then(|x| x.as_f64()) {
        if v > 0.0 {
            entry.last_v_token_reserves = v;
        }
    }
    if let Some(v) = v.get("marketCapSol").and_then(|x| x.as_f64()) {
        if v > 0.0 {
            entry.last_market_cap_sol = v;
        }
    }

    let is_buy = tx_type == "buy";

    match tx_type {
        "buy" => {
            entry.buy_count += 1;

            // Track unique buyers and trade timestamps for wash-trade detection
            if let Some(trader_pubkey) = trader {
                entry.unique_buyers.insert(trader_pubkey);
                entry.trade_timestamps.push((now, trader_pubkey));
            }
        }
        "sell" => {
            entry.sell_count += 1;

            if let Some(trader_pubkey) = trader {
                entry.trade_timestamps.push((now, trader_pubkey));
            }
        }
        other => {
            debug!(mint = mint_str, tx_type = other, "tokenTrade → unrecognized txType — skipping");
        }
    }

    // ── Record per-trade data for bonding curve signal detection ──
    if (is_buy || tx_type == "sell") && entry.trade_log.len() < super::types::MAX_TRADE_LOG_SIZE {
        if let Some(trader_pubkey) = trader {
            entry.trade_log.push((now, sol_amount, is_buy, trader_pubkey));
        }
    }

    // ── Check if we should record a bonding curve signal row ──
    if cfg.strategy.detection.bonding_curve_signals_enabled
        && !entry.signal_recorded
        && entry.total_volume_sol >= cfg.strategy.detection.bc_signal_volume_threshold
    {
        entry.signal_recorded = true;

        // ── Compute and cache BC score for fast-track pipeline ──
        let whale_buy = entry.trade_log.iter().any(|&(_, sol, is_buy, _)| is_buy && sol >= 3.0);
        let cr = entry.trade_log.iter().any(|&(_, _, is_buy, wallet)| {
            is_buy && wallet == entry.creator_wallet
        }) && entry.trade_log.len() > 1;
        let bsr = if entry.sell_count > 0 {
            entry.buy_count as f64 / entry.sell_count as f64
        } else if entry.buy_count > 0 {
            entry.buy_count as f64
        } else {
            0.0
        };
        let bc_score = compute_bc_score(
            entry.unique_buyers.len(),
            bsr,
            cr,
            whale_buy,
            entry.buy_count,
            entry.sell_count,
            entry.total_volume_sol,
        );

        let cache_entry = BcScoreEntry {
            score: bc_score,
            unique_buyers: entry.unique_buyers.len(),
            buy_sell_ratio: bsr,
            creator_rebuy: cr,
            whale_buy,
            buy_count: entry.buy_count,
            sell_count: entry.sell_count,
            total_volume_sol: entry.total_volume_sol,
            recorded_at: chrono::Utc::now().timestamp_millis(),
        };

        {
            let bc_cache = bc_cache.clone();
            let mint_key = mint_str.to_string();
            let score_val = bc_score;
            // Insert into cache (non-blocking — lock is fast)
            let mut map = bc_cache.lock().await;
            map.insert(mint_key, cache_entry);
            drop(map);
            debug!(
                mint = mint_str,
                bc_score = format!("{:.1}", score_val),
                "📊 BC score cached for fast-track pipeline"
            );
        }
        // Prune cache periodically
        prune_bc_score_cache(bc_cache).await;

        let signal_payload = build_signal_payload(entry, mint_str);
        let supabase_clone = Arc::clone(supabase);
        let score_for_paper = bc_score;
        tokio::spawn(async move {
            write_bonding_curve_signal(&supabase_clone, &signal_payload).await;
            write_bc_paper_trade(&supabase_clone, &signal_payload, "volume_50sol", score_for_paper).await;
        });
    }

    // ── Lane-B: bc_progress_pct >= 90% trigger (one-shot per token) ──
    // Captures the same signal payload + computed bc_score at high BC progress
    // so we can backtest "score-filter at 90% → buy on BC → ride graduation".
    // Independent of the 50-SOL signal so a single token can produce both rows.
    if !entry.progress_signal_recorded
        && entry.last_v_sol_reserves > 0.0
    {
        let bc_progress_pct = (((entry.last_v_sol_reserves - 30.0) / 85.0) * 100.0)
            .clamp(0.0, 100.0);
        if bc_progress_pct >= 90.0 {
            entry.progress_signal_recorded = true;

            // Compute the same score we use for the 50-SOL signal so the two
            // rows are directly comparable.
            let whale_buy_b = entry.trade_log.iter().any(|&(_, sol, is_buy, _)| is_buy && sol >= 3.0);
            let cr_b = entry.trade_log.iter().any(|&(_, _, is_buy, wallet)| {
                is_buy && wallet == entry.creator_wallet
            }) && entry.trade_log.len() > 1;
            let bsr_b = if entry.sell_count > 0 {
                entry.buy_count as f64 / entry.sell_count as f64
            } else if entry.buy_count > 0 {
                entry.buy_count as f64
            } else {
                0.0
            };
            let lane_b_score = compute_bc_score(
                entry.unique_buyers.len(),
                bsr_b,
                cr_b,
                whale_buy_b,
                entry.buy_count,
                entry.sell_count,
                entry.total_volume_sol,
            );

            let signal_payload_b = build_signal_payload(entry, mint_str);
            let supabase_b = Arc::clone(supabase);
            let cfg_b = Arc::clone(cfg);
            let mint_for_b = mint_str.to_string();
            let progress_pct_logged = bc_progress_pct;
            tokio::spawn(async move {
                info!(
                    mint = %mint_for_b,
                    progress = format!("{:.1}%", progress_pct_logged),
                    score = format!("{:.1}", lane_b_score),
                    "🎯 LANE-B TRIGGER — bc_progress >= 90%, recording paper trade"
                );

                let row_id = write_bc_paper_trade(
                    &supabase_b,
                    &signal_payload_b,
                    "progress_90pct",
                    lane_b_score,
                ).await;

                // Async API check — runs AFTER the row is written so it doesn't
                // delay the trigger. Result PATCHed onto entry_api_checks.
                if let Some(id) = row_id {
                    run_lane_b_api_check(&supabase_b, &cfg_b, id, &mint_for_b).await;
                }
            });
        }
    }

    Ok(())
}

/// Run a fast safety-check on a Lane-B candidate and PATCH the result onto
/// `bc_paper_trades.entry_api_checks`. Used to test the hypothesis:
///   "filter at 90% with score + API checks → does it beat 50-SOL baseline?"
///
/// Currently calls GoPlus only (fastest single check, ~250-500ms). Stays
/// best-effort: failures just write a `succeeded=false` marker.
async fn run_lane_b_api_check(
    supabase: &Arc<SupabaseClient>,
    cfg: &Arc<AppConfig>,
    row_id: i64,
    mint: &str,
) {
    use crate::filters::goplus::GoPlusFilter;
    let started = std::time::Instant::now();
    let gp = GoPlusFilter::new();
    let result = gp.check(mint, cfg).await;

    let payload = serde_json::json!({
        "succeeded": true,
        "goplus_passed": result.passed,
        "goplus_rejection_reason": result.fail_reason,
        "ms_total": started.elapsed().as_millis() as u64,
    });
    patch_lane_b_api_checks(supabase, row_id, &payload).await;
}

async fn patch_lane_b_api_checks(supabase: &Arc<SupabaseClient>, row_id: i64, value: &serde_json::Value) {
    let url = format!("{}/bc_paper_trades?id=eq.{}", supabase.base_url, row_id);
    let body = serde_json::json!({ "entry_api_checks": value });
    if let Err(e) = supabase.client.patch(&url).json(&body).send().await {
        warn!(row_id, "Lane-B api_checks PATCH error: {}", e);
    }
}

// ─── 3. tokenComplete (GRADUATION) ──────────────────────────────────────────

/// The token has graduated — it is migrating to an AMM (Raydium or pump-amm).
/// Build a [`GraduatedToken`] from the watchlist data and send it downstream.
async fn handle_token_complete(
    v: &serde_json::Value,
    tx: &mpsc::Sender<GraduatedToken>,
    watchlist: &mut HashMap<String, WatchlistEntry>,
    supabase: &Arc<SupabaseClient>,
    rpc_url: &str,
) -> Result<()> {
    let mint_str = v
        .get("mint")
        .and_then(|m| m.as_str())
        .ok_or_else(|| anyhow::anyhow!("tokenComplete: missing 'mint' field"))?;

    // Pool address from the graduation payload — try several field names.
    let mut pool_address: Option<Pubkey> = ["pool", "poolAddress", "pool_address"]
        .iter()
        .find_map(|key| {
            v.get(*key)
                .and_then(|p| p.as_str())
                .and_then(|s| Pubkey::from_str(s).ok())
        });

    // CRITICAL FIX (v10): pump.fun's tokenComplete event often returns the
    // bonding-curve account pubkey (owned by PumpFun program 6EF8...) rather
    // than the actual PumpSwap AMM pool. We must verify the on-chain owner
    // and discard non-PumpSwap/Raydium addresses so the fallback paths below
    // can resolve the real pool.
    if let Some(pk) = pool_address {
        if !verify_pool_owner(rpc_url, &pk).await {
            warn!(
                target: "pool_resolve",
                mint = %mint_str,
                pool = %pk,
                source = "event_field",
                "⚠️ POOL_REJECT — event 'pool' field owned by non-AMM program (likely BC account); falling back"
            );
            pool_address = None;
        } else {
            info!(
                target: "pool_resolve",
                mint = %mint_str,
                pool = %pk,
                source = "event_field",
                "✅ POOL_OK — event 'pool' field verified as AMM"
            );
        }
    }

    // If pool field is not a valid pubkey (e.g. "pump-amm"), resolve from the
    // migration transaction signature via RPC.
    if pool_address.is_none() {
        let sig_str = v.get("signature").and_then(|s| s.as_str());
        if let Some(sig) = sig_str {
            info!(mint = %mint_str, "Pool field is not a pubkey — resolving from migration tx");
            match resolve_pool_from_tx(rpc_url, sig, mint_str).await {
                Ok(Some(pool)) => {
                    info!(
                        target: "pool_resolve",
                        mint = %mint_str,
                        pool = %pool,
                        source = "migration_tx",
                        "✅ POOL_OK — resolved from migration tx"
                    );
                    pool_address = Some(pool);
                }
                Ok(None) => {
                    warn!(
                        target: "pool_resolve",
                        mint = %mint_str,
                        source = "migration_tx",
                        "⚠️ POOL_MISS — no pool address in migration tx accounts"
                    );
                }
                Err(e) => {
                    warn!(
                        target: "pool_resolve",
                        mint = %mint_str,
                        source = "migration_tx",
                        error = %e,
                        "⚠️ POOL_MISS — migration tx fetch failed"
                    );
                }
            }
        }
    }

    // Fallback: resolve pool via DexScreener if migration tx parsing failed.
    // DexScreener indexes pump.fun graduations within seconds.
    if pool_address.is_none() {
        let dex_url = format!(
            "https://api.dexscreener.com/latest/dex/tokens/{}",
            mint_str
        );
        match supabase
            .client
            .get(&dex_url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(pairs) = json.get("pairs").and_then(|p| p.as_array()) {
                        let pool_str = pairs
                            .iter()
                            .find(|p| p.get("chainId").and_then(|c| c.as_str()) == Some("solana"))
                            .or_else(|| pairs.first())
                            .and_then(|p| p.get("pairAddress"))
                            .and_then(|a| a.as_str());
                        if let Some(addr) = pool_str {
                            if let Ok(pubkey) = Pubkey::from_str(addr) {
                                info!(
                                    target: "pool_resolve",
                                    mint = %mint_str,
                                    pool = %pubkey,
                                    source = "dexscreener",
                                    "✅ POOL_OK — resolved via DexScreener fallback"
                                );
                                pool_address = Some(pubkey);
                            }
                        }
                    }
                }
            }
            _ => {
                debug!(mint = %mint_str, "DexScreener pool lookup failed — will use fallback liquidity");
            }
        }
    }

    if pool_address.is_none() {
        warn!(
            target: "pool_resolve",
            mint = %mint_str,
            source = "none",
            "❌ POOL_FAIL — all 3 resolution paths failed (event/migration_tx/dexscreener); LP + tick monitoring will be unavailable"
        );
    }

    let now_ms = chrono::Utc::now().timestamp_millis();

    // ── Fetch historical trades from pump.fun API ────────
    // Skip if the watchlist already has enough trade data from WebSocket monitoring.
    let has_enough_ws_data = watchlist.get(mint_str)
        .map(|e| e.buy_count + e.sell_count >= 20)
        .unwrap_or(false);

    let trades: Vec<serde_json::Value> = if has_enough_ws_data {
        tracing::info!(
            mint = %mint_str,
            "Skipping historical fetch — sufficient WebSocket trade data"
        );
        Vec::new()
    } else {
        let trades_url = format!(
            "https://frontend-api.pump.fun/trades/all/{}?limit=200&offset=0&minimumSize=0",
            mint_str
        );
        match supabase
            .client
            .get(&trades_url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(r) => r.json().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    };

    let mut historical_buys = 0u64;
    let mut historical_sells = 0u64;

    for trade in &trades {
        match trade.get("is_buy").and_then(|v| v.as_bool()) {
            Some(true)  => historical_buys += 1,
            Some(false) => historical_sells += 1,
            None => {}
        }
    }

    if trades.is_empty() && !has_enough_ws_data {
        warn!(
            mint = %mint_str,
            "Could not fetch historical trades — proceeding with empty watchlist"
        );
    } else if !trades.is_empty() {
        info!(
            mint = %mint_str,
            historical_buys,
            historical_sells,
            total = historical_buys + historical_sells,
            "Fetched historical trades for graduated token"
        );
    }

    // Inject historical counts and unique buyers into the watchlist entry before consuming it.
    if let Some(entry) = watchlist.get_mut(mint_str) {
        entry.buy_count += historical_buys;
        entry.sell_count += historical_sells;

        // Merge historical buy traders into unique_buyers
        for trade in &trades {
            if trade.get("is_buy").and_then(|v| v.as_bool()) != Some(true) {
                continue;
            }
            let trader_str = trade
                .get("user")
                .or_else(|| trade.get("traderPublicKey"))
                .or_else(|| trade.get("trader"))
                .or_else(|| trade.get("signer"))
                .and_then(|v| v.as_str());
            if let Some(s) = trader_str {
                if let Ok(pubkey) = Pubkey::from_str(s) {
                    entry.unique_buyers.insert(pubkey);
                }
            }
        }

        tracing::info!(
            mint = %mint_str,
            unique_buyers_after_merge = entry.unique_buyers.len(),
            buy_count = entry.buy_count,
            "Historical trades merged into unique_buyers"
        );
    }

    // Look up the watchlist for enrichment; if we missed the newToken for
    // this mint (e.g. after a reconnect), we still emit with defaults.
    if let Some(entry) = watchlist.get(mint_str) {
        info!(
            mint = %mint_str,
            ws_buy_count = entry.buy_count,
            ws_sell_count = entry.sell_count,
            ws_unique_buyers = entry.unique_buyers.len(),
            ws_volume_sol = format!("{:.4}", entry.total_volume_sol),
            ws_buy_pressure = format!("{:.1}%", entry.buy_pressure_pct()),
            "📊 Watchlist stats at graduation (before historical merge)"
        );
    } else {
        warn!(mint = %mint_str, "📊 Token NOT in watchlist at graduation time");
    }
    let (creator_wallet, bonding_curve_volume_sol, buy_pressure_pct, detected_at, time_to_graduate_seconds, unique_buyer_count, buy_count, sell_count, trade_timestamps, token_name, token_symbol, initial_buy_sol, creator_rebuy, buy_sell_ratio) =
        match watchlist.remove(mint_str) {
            Some(entry) => {
                let elapsed_secs = (now_ms - entry.detected_at) as f64 / 1_000.0;
                // Compute creator_rebuy from trade_log
                let cr = entry.trade_log.iter().any(|&(_, _, is_buy, wallet)| {
                    is_buy && wallet == entry.creator_wallet
                }) && entry.trade_log.len() > 1;
                // Compute buy/sell ratio
                let bsr = if entry.sell_count > 0 {
                    entry.buy_count as f64 / entry.sell_count as f64
                } else if entry.buy_count > 0 {
                    entry.buy_count as f64
                } else {
                    0.0
                };
                (
                    entry.creator_wallet,
                    entry.total_volume_sol,
                    entry.buy_pressure_pct(),
                    entry.detected_at,
                    elapsed_secs,
                    entry.unique_buyers.len(),
                    entry.buy_count,
                    entry.sell_count,
                    entry.trade_timestamps,
                    entry.name,
                    entry.symbol,
                    entry.initial_buy_sol,
                    cr,
                    bsr,
                )
            }
            None => {
                warn!(
                    mint = mint_str,
                    "tokenComplete for mint not in watchlist — using historical data"
                );
                let total_historical = historical_buys + historical_sells;
                let bp_pct = if total_historical > 0 {
                    historical_buys as f64 / total_historical as f64 * 100.0
                } else {
                    0.0
                };
                let mut unique_set = std::collections::HashSet::new();
                let mut ts_vec = vec![];
                for trade in &trades {
                    if trade.get("is_buy").and_then(|v| v.as_bool()) != Some(true) {
                        continue;
                    }
                    let trader_str = trade
                        .get("user")
                        .or_else(|| trade.get("traderPublicKey"))
                        .or_else(|| trade.get("trader"))
                        .or_else(|| trade.get("signer"))
                        .and_then(|v| v.as_str());
                    if let Some(s) = trader_str {
                        if let Ok(pubkey) = Pubkey::from_str(s) {
                            unique_set.insert(pubkey);
                            ts_vec.push((now_ms, pubkey));
                        }
                    }
                }
                let fallback_bsr = if historical_sells > 0 {
                    historical_buys as f64 / historical_sells as f64
                } else if historical_buys > 0 {
                    historical_buys as f64
                } else {
                    0.0
                };
                (
                    Pubkey::default(),
                    0.0,
                    bp_pct,
                    now_ms,
                    0.0,
                    unique_set.len(),
                    historical_buys,
                    historical_sells,
                    ts_vec,
                    String::new(),
                    String::new(),
                    0.0,
                    false,
                    fallback_bsr,
                )
            }
        };

    // ── Fallback: pull name/symbol from the graduation event itself ────
    // The watchlist None branch returns empty strings, which causes the
    // filter engine to silently drop the token. Parse from the raw payload.
    let token_name = if token_name.is_empty() {
        v.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string()
    } else { token_name };
    let token_symbol = if token_symbol.is_empty() {
        v.get("symbol").and_then(|s| s.as_str()).unwrap_or("").to_string()
    } else { token_symbol };

    let mint = Pubkey::from_str(mint_str)
        .map_err(|e| anyhow::anyhow!("tokenComplete: invalid mint '{}': {}", mint_str, e))?;

    // ── Resolve actual pool liquidity from RPC balance ──────────────────
    // bonding_curve_volume_sol is NOT pool liquidity — it's observed trade
    // volume on the bonding curve. The real initial liquidity is the SOL
    // sitting in the Raydium/pump-AMM pool account after graduation.
    let initial_liquidity_sol = if let Some(pool_pubkey) = pool_address {
        let pool_rpc = RpcClient::new_with_timeout(
            rpc_url.to_string(),
            std::time::Duration::from_secs(5),
        );
        match pool_rpc.get_balance(&pool_pubkey).await {
            Ok(lamports) => {
                let sol = lamports as f64 / 1_000_000_000.0;
                // If balance is tiny (< 1 SOL), it's just the rent-exempt
                // minimum — pump-AMM pools store liquidity in a separate
                // vault, not the pool account itself.  Fall back to 85 SOL.
                if sol < 1.0 {
                    info!(
                        mint = %mint_str,
                        pool = %pool_pubkey,
                        sol = format!("{:.4}", sol),
                        "📊 Pool balance is rent-only — using 85 SOL fallback"
                    );
                    85.0
                } else {
                    info!(
                        mint = %mint_str,
                        pool = %pool_pubkey,
                        lamports,
                        sol = format!("{:.4}", sol),
                        "📊 Actual pool SOL balance fetched for initial_liquidity_sol"
                    );
                    sol
                }
            }
            Err(e) => {
                warn!(
                    mint = %mint_str,
                    "Failed to fetch pool balance — falling back to 85 SOL estimate: {}",
                    e
                );
                85.0
            }
        }
    } else {
        85.0
    };

    let graduated = GraduatedToken {
        mint,
        pool_address,
        creator_wallet,
        bonding_curve_volume_sol,
        buy_pressure_pct,
        time_to_graduate_seconds,
        detected_at,
        source: DetectionSource::PumpFun,
        unique_buyer_count,
        buy_count,
        sell_count,
        trade_timestamps,
        name: token_name.clone(),
        symbol: token_symbol.clone(),
        initial_liquidity_sol,
        creator_rebuy,
        buy_sell_ratio,
        candidate_id: None,
        sniper_features: None,
        sniper_score: None,
        pipeline_timing: PipelineTiming::new(detected_at),
    };

    info!(
        mint = %graduated.mint,
        pool = ?graduated.pool_address,
        volume_sol = graduated.bonding_curve_volume_sol,
        buy_pct = format!("{:.1}%", graduated.buy_pressure_pct),
        grad_secs = format!("{:.1}s", graduated.time_to_graduate_seconds),
        "🎓 tokenComplete → GRADUATED — sending to filter engine"
    );

    // ── Write to Supabase tokens_seen (non-blocking) ─────
    let supabase_clone = Arc::clone(supabase);
    let mint_string = mint_str.to_string();
    let initial_liq = graduated.initial_liquidity_sol;
    let name_clone = graduated.name.clone();
    let symbol_clone = graduated.symbol.clone();
    let pool_str = graduated.pool_address.map(|p| p.to_string());
    let creator_str = graduated.creator_wallet.to_string();
    tokio::spawn(async move {
        let payload = serde_json::json!({
            "mint": mint_string,
            "source": "pumpfun_ws",
            "detected_at": chrono::Utc::now().to_rfc3339(),
            "initial_liquidity_sol": initial_liq,
            "name": name_clone,
            "symbol": symbol_clone,
            "pool_address": pool_str,
            "creator_wallet": creator_str,
        });

        let url = format!("{}/tokens_seen", supabase_clone.base_url);
        match supabase_clone.client.post(&url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => { /* ok */ }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::error!("Failed to write tokens_seen: HTTP {} — {}", status, body);
            }
            Err(e) => {
                tracing::error!("Failed to write tokens_seen: {}", e);
            }
        }
    });

    // ── Mark graduated in bonding_curve_signals + bc_paper_trades ──
    {
        let supabase_grad = Arc::clone(supabase);
        let mint_for_grad = mint_str.to_string();
        let grad_liquidity = initial_liquidity_sol;
        tokio::spawn(async move {
            let now_rfc = chrono::Utc::now().to_rfc3339();

            // Update bonding_curve_signals
            let url = format!(
                "{}/bonding_curve_signals?mint=eq.{}",
                supabase_grad.base_url, mint_for_grad
            );
            let payload = serde_json::json!({
                "graduated": true,
                "graduated_at": &now_rfc,
            });
            match supabase_grad.client.patch(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!(mint = %mint_for_grad, "bonding_curve_signals: marked graduated");
                }
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    debug!(mint = %mint_for_grad, "bonding_curve_signals graduation update: {}", body);
                }
                Err(e) => {
                    debug!(mint = %mint_for_grad, "bonding_curve_signals graduation update failed: {}", e);
                }
            }

            // Update bc_paper_trades with graduation data
            let pt_url = format!(
                "{}/bc_paper_trades?mint=eq.{}",
                supabase_grad.base_url, mint_for_grad
            );
            let pt_payload = serde_json::json!({
                "graduated": true,
                "graduated_at": &now_rfc,
                "initial_liquidity_sol": grad_liquidity,
            });
            match supabase_grad.client.patch(&pt_url).json(&pt_payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(mint = %mint_for_grad, "🎓 bc_paper_trades: marked graduated");
                }
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    debug!(mint = %mint_for_grad, "bc_paper_trades graduation update: {}", body);
                }
                Err(e) => {
                    debug!(mint = %mint_for_grad, "bc_paper_trades graduation update failed: {}", e);
                }
            }

            // Schedule post-graduation price tracking (separate spawn so it
            // doesn't block the graduation handler for 1+ hour)
            tokio::spawn(spawn_bc_price_tracker(supabase_grad, mint_for_grad));
        });
    }

    tx.send(graduated)
        .await
        .context("Detection channel closed — receiver dropped")?;

    Ok(())
}

// ─── Pool address resolution from migration transaction ──────────────────────

/// Known AMM program IDs to identify pool accounts in a migration transaction.
const PUMP_AMM_PROGRAM: &str = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj";
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

/// Verify that a pubkey's on-chain owner matches a supported AMM program.
/// Returns true if the owner is PumpSwap or Raydium V4, false otherwise
/// (including RPC errors — fail closed so the caller tries fallback).
async fn verify_pool_owner(rpc_url: &str, pool: &Pubkey) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [pool.to_string(), { "encoding": "base64" }]
    });
    let resp = match client.post(rpc_url).json(&payload).send().await {
        Ok(r) => r,
        Err(_) => return false,
    };
    let body: serde_json::Value = match resp.json().await {
        Ok(b) => b,
        Err(_) => return false,
    };
    let owner = body
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.get("owner"))
        .and_then(|o| o.as_str());
    matches!(owner, Some(PUMP_AMM_PROGRAM) | Some(RAYDIUM_AMM_V4))
}

/// Fetch the migration transaction and extract the pool address from its accounts.
///
/// Strategy: find the instruction that targets a known AMM program (pump-amm or
/// Raydium), then extract the pool account (typically the first writable account
/// in that instruction that is NOT the mint, the payer, or a well-known program).
async fn resolve_pool_from_tx(
    rpc_url: &str,
    signature_str: &str,
    mint_str: &str,
) -> Result<Option<Pubkey>> {
    let rpc = RpcClient::new_with_timeout(
        rpc_url.to_string(),
        std::time::Duration::from_secs(10),
    );

    let sig = Signature::from_str(signature_str)
        .map_err(|e| anyhow::anyhow!("Invalid signature '{}': {}", signature_str, e))?;

    let tx_config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::JsonParsed),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    // The graduation event fires before the tx is confirmed on-chain.
    // Retry a few times with short delays to wait for confirmation.
    let mut tx_response = None;
    for attempt in 0..5 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        }
        match rpc.get_transaction_with_config(&sig, tx_config.clone()).await {
            Ok(resp) => {
                tx_response = Some(resp);
                break;
            }
            Err(e) => {
                if attempt < 4 {
                    debug!("getTransaction attempt {} failed (not confirmed yet), retrying...", attempt + 1);
                } else {
                    return Err(anyhow::anyhow!("getTransaction failed after 5 attempts: {}", e));
                }
            }
        }
    }

    let tx_response = tx_response.ok_or_else(|| anyhow::anyhow!("getTransaction: no response after retries"))?;

    // Extract account keys from the transaction
    let tx_value = serde_json::to_value(&tx_response.transaction)
        .map_err(|e| anyhow::anyhow!("Failed to serialize tx: {}", e))?;

    // Navigate: transaction.message.accountKeys
    let account_keys: Vec<String> = tx_value
        .get("transaction")
        .and_then(|t| t.get("message"))
        .and_then(|m| m.get("accountKeys"))
        .and_then(|a| a.as_array())
        .map(|keys| {
            keys.iter()
                .filter_map(|k| {
                    // accountKeys can be strings or objects with a "pubkey" field
                    k.as_str()
                        .map(|s| s.to_string())
                        .or_else(|| {
                            k.get("pubkey")
                                .and_then(|p| p.as_str())
                                .map(|s| s.to_string())
                        })
                })
                .collect()
        })
        .unwrap_or_default();

    if account_keys.is_empty() {
        return Ok(None);
    }

    let mint_pubkey = Pubkey::from_str(mint_str).ok();

    // Well-known program IDs and system accounts to skip
    let skip_set: std::collections::HashSet<&str> = [
        PUMP_AMM_PROGRAM,
        RAYDIUM_AMM_V4,
        "11111111111111111111111111111111",           // System Program
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // Token Program
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", // Token-2022 Program
        "SysvarRent111111111111111111111111111111111",
        "So11111111111111111111111111111111111111112", // Wrapped SOL
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", // Associated Token Program
        "ComputeBudget111111111111111111111111111111",
        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", // PumpFun program
    ]
    .iter()
    .copied()
    .collect();

    // Look for instructions targeting a known AMM program
    let instructions = tx_value
        .get("transaction")
        .and_then(|t| t.get("message"))
        .and_then(|m| m.get("instructions"))
        .and_then(|i| i.as_array());

    if let Some(ixs) = instructions {
        for ix in ixs {
            let program_id = ix
                .get("programId")
                .and_then(|p| p.as_str())
                .unwrap_or("");

            if program_id == PUMP_AMM_PROGRAM || program_id == RAYDIUM_AMM_V4 {
                // The accounts in this instruction — pool is typically one of
                // the first few writable accounts that isn't mint/program/system.
                if let Some(accs) = ix.get("accounts").and_then(|a| a.as_array()) {
                    for acc_val in accs {
                        let acc_str = acc_val.as_str().unwrap_or("");
                        if acc_str.is_empty() { continue; }
                        if skip_set.contains(acc_str) { continue; }
                        if let Some(ref mp) = mint_pubkey {
                            if acc_str == mp.to_string() { continue; }
                        }
                        // First non-trivial account in the AMM instruction = pool
                        if let Ok(pool) = Pubkey::from_str(acc_str) {
                            return Ok(Some(pool));
                        }
                    }
                }
            }
        }
    }

    // Fallback: also check inner instructions (for versioned transactions)
    let inner = tx_value
        .get("meta")
        .and_then(|m| m.get("innerInstructions"))
        .and_then(|i| i.as_array());

    if let Some(inner_groups) = inner {
        for group in inner_groups {
            if let Some(ixs) = group.get("instructions").and_then(|i| i.as_array()) {
                for ix in ixs {
                    let program_id = ix
                        .get("programId")
                        .and_then(|p| p.as_str())
                        .unwrap_or("");

                    if program_id == PUMP_AMM_PROGRAM || program_id == RAYDIUM_AMM_V4 {
                        if let Some(accs) = ix.get("accounts").and_then(|a| a.as_array()) {
                            for acc_val in accs {
                                let acc_str = acc_val.as_str().unwrap_or("");
                                if acc_str.is_empty() { continue; }
                                if skip_set.contains(acc_str) { continue; }
                                if let Some(ref mp) = mint_pubkey {
                                    if acc_str == mp.to_string() { continue; }
                                }
                                if let Ok(pool) = Pubkey::from_str(acc_str) {
                                    return Ok(Some(pool));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(None)
}

// ─── Bonding curve signal detection ──────────────────────────────────────────

/// Build the JSONB payload for a bonding_curve_signals INSERT.
/// Called when a token's cumulative volume crosses the threshold.
fn build_signal_payload(entry: &WatchlistEntry, mint_str: &str) -> serde_json::Value {
    let now = chrono::Utc::now().timestamp_millis();
    let token_age_secs = (now - entry.detected_at) as f64 / 1_000.0;

    // ── Compute signal flags from trade_log ──
    let mut whale_buy_max_sol: f64 = 0.0;
    let mut whale_buy_count: u32 = 0;
    let mut max_single_trade_sol: f64 = 0.0;
    let mut total_buy_sol: f64 = 0.0;
    let mut total_sell_sol: f64 = 0.0;
    let mut creator_rebuy = false;

    for &(_, sol, is_buy, wallet) in &entry.trade_log {
        if sol > max_single_trade_sol {
            max_single_trade_sol = sol;
        }
        if is_buy {
            total_buy_sol += sol;
            if sol >= 3.0 {
                whale_buy_count += 1;
                if sol > whale_buy_max_sol {
                    whale_buy_max_sol = sol;
                }
            }
            // Creator rebuy: creator wallet buying again (not the initial buy)
            if wallet == entry.creator_wallet && entry.trade_log.len() > 1 {
                creator_rebuy = true;
            }
        } else {
            total_sell_sol += sol;
        }
    }

    // Buy velocity: max buys in any 30-second window
    let buy_velocity_30s = compute_buy_velocity(&entry.trade_log, 30_000);

    // Zero sells pattern: lots of buys with zero sells
    let zero_sells = entry.buy_count >= 50 && entry.sell_count == 0;

    // Sell-then-buy flip: was there a period of mostly sells followed by buy burst?
    let sell_then_buy_flip = detect_sell_buy_flip(&entry.trade_log);

    // Fast volume: 50+ SOL in under 3 minutes
    let fast_volume = entry.total_volume_sol >= 50.0 && token_age_secs < 180.0;

    let buy_sell_ratio = if entry.sell_count > 0 {
        entry.buy_count as f64 / entry.sell_count as f64
    } else if entry.buy_count > 0 {
        entry.buy_count as f64 // infinity-like
    } else {
        0.0
    };

    let avg_trade_sol = if !entry.trade_log.is_empty() {
        entry.total_volume_sol / entry.trade_log.len() as f64
    } else {
        0.0
    };

    let signals = serde_json::json!({
        "whale_buy": whale_buy_count > 0,
        "whale_buy_max_sol": whale_buy_max_sol,
        "whale_buy_count": whale_buy_count,
        "buy_velocity_30s": buy_velocity_30s,
        "zero_sells": zero_sells,
        "creator_rebuy": creator_rebuy,
        "sell_then_buy_flip": sell_then_buy_flip,
        "fast_volume": fast_volume,
        "max_single_trade_sol": max_single_trade_sol,
        "avg_trade_sol": avg_trade_sol,
        "buy_sell_ratio": buy_sell_ratio,
        "total_buy_sol": total_buy_sol,
        "total_sell_sol": total_sell_sol,
    });

    // Build compact trade log JSONB (truncate wallet to first 8 chars for space)
    let trades_json: Vec<serde_json::Value> = entry.trade_log.iter().map(|(t, sol, is_buy, wallet)| {
        serde_json::json!({
            "t": t,
            "sol": sol,
            "side": if *is_buy { "buy" } else { "sell" },
            "w": &wallet.to_string()[..8],
        })
    }).collect();

    // Compute bonding-curve progress from live WS reserves.
    // Pump.fun starts with ~30 SOL virtual reserves and graduates when
    // ~85 SOL of real SOL has entered → virtual_sol ≈ 115 at graduation.
    // progress_pct = ((v_sol - 30) / 85) * 100, clamped to [0, 100].
    let v_sol = entry.last_v_sol_reserves;
    let v_tok = entry.last_v_token_reserves;
    let bc_progress_pct = if v_sol > 0.0 {
        (((v_sol - 30.0) / 85.0) * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };
    // Spot price in SOL/token from the live curve (unitless ratio of reserves).
    let bc_price_sol_per_token = if v_tok > 0.0 { v_sol / v_tok } else { 0.0 };

    serde_json::json!({
        "mint": mint_str,
        "name": entry.name,
        "symbol": entry.symbol,
        "creator_wallet": entry.creator_wallet.to_string(),
        "token_created_at": entry.detected_at,
        "signal_recorded_at": now,
        "token_age_secs": token_age_secs,
        "total_volume_sol": entry.total_volume_sol,
        "buy_count": entry.buy_count,
        "sell_count": entry.sell_count,
        "unique_buyers": entry.unique_buyers.len(),
        "initial_buy_sol": entry.initial_buy_sol,
        "trades": trades_json,
        "signals": signals,
        // Live bonding-curve state at signal time (from WS, not REST).
        "bc_progress_pct": bc_progress_pct,
        "bc_virtual_sol_reserves": v_sol,
        "bc_virtual_token_reserves": v_tok,
        "bc_market_cap_sol": entry.last_market_cap_sol,
        "bc_price_sol_per_token": bc_price_sol_per_token,
    })
}

/// Max number of buy trades in any rolling window of `window_ms` milliseconds.
fn compute_buy_velocity(trade_log: &[(i64, f64, bool, Pubkey)], window_ms: i64) -> u32 {
    let buys: Vec<i64> = trade_log.iter()
        .filter(|(_, _, is_buy, _)| *is_buy)
        .map(|(t, _, _, _)| *t)
        .collect();

    if buys.is_empty() {
        return 0;
    }

    let mut max_count: u32 = 0;
    let mut start = 0;
    for end in 0..buys.len() {
        while buys[end] - buys[start] > window_ms {
            start += 1;
        }
        let count = (end - start + 1) as u32;
        if count > max_count {
            max_count = count;
        }
    }
    max_count
}

/// Detect if there was a sell-dominated period followed by a buy burst.
/// Simple heuristic: split trade log in half, first half has more sells,
/// second half has more buys.
fn detect_sell_buy_flip(trade_log: &[(i64, f64, bool, Pubkey)]) -> bool {
    if trade_log.len() < 20 {
        return false;
    }
    let mid = trade_log.len() / 2;
    let first_half_sells = trade_log[..mid].iter().filter(|(_, _, is_buy, _)| !is_buy).count();
    let first_half_buys = trade_log[..mid].iter().filter(|(_, _, is_buy, _)| *is_buy).count();
    let second_half_buys = trade_log[mid..].iter().filter(|(_, _, is_buy, _)| *is_buy).count();
    let second_half_sells = trade_log[mid..].iter().filter(|(_, _, is_buy, _)| !is_buy).count();

    first_half_sells > first_half_buys && second_half_buys > second_half_sells * 2
}

/// Fire-and-forget INSERT to bonding_curve_signals.
async fn write_bonding_curve_signal(supabase: &SupabaseClient, payload: &serde_json::Value) {
    let mint = payload.get("mint").and_then(|m| m.as_str()).unwrap_or("unknown");
    let url = format!("{}/bonding_curve_signals", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            info!(
                mint = %mint,
                volume = payload.get("total_volume_sol").and_then(|v| v.as_f64()).unwrap_or(0.0),
                buys = payload.get("buy_count").and_then(|v| v.as_u64()).unwrap_or(0),
                "📊 Bonding curve signal recorded"
            );
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "bonding_curve_signals INSERT failed: {}", body);
        }
        Err(e) => {
            warn!(mint = %mint, "bonding_curve_signals INSERT error: {}", e);
        }
    }
}

/// Fire-and-forget INSERT to bc_paper_trades — simulated pre-graduation buy.
/// Records BC reserves from the live WS stream (always populated when reserves
/// have been observed) plus an optional pump.fun REST enrichment.
///
/// `trigger` distinguishes which signal fired this row:
///   - "volume_50sol"   : default 50 SOL volume threshold (≈23% progress)
///   - "progress_90pct" : Lane-B trigger when bc_progress_pct crosses 90%
///
/// Returns the inserted row id when available, so callers can PATCH async
/// API-check results onto the row (used by Lane B for entry_api_checks).
async fn write_bc_paper_trade(
    supabase: &SupabaseClient,
    signal: &serde_json::Value,
    trigger: &str,
    entry_score: f64,
) -> Option<i64> {
    let mint = signal.get("mint").and_then(|m| m.as_str()).unwrap_or("unknown");

    let bsr = signal.get("signals")
        .and_then(|s| s.get("buy_sell_ratio"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let creator_rebuy = signal.get("signals")
        .and_then(|s| s.get("creator_rebuy"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Fetch bonding curve state from pump.fun API
    let coin_data = fetch_pumpfun_coin(mint).await;

    // ── WS-derived BC state (always populated when we have any trade data) ──
    // These are computed in build_signal_payload from the live WS reserves
    // and are reliable; the pump.fun REST API is Cloudflare-protected and
    // often returns nothing, so prefer WS values as the source of truth.
    let ws_bc_progress = signal.get("bc_progress_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ws_v_sol = signal.get("bc_virtual_sol_reserves").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ws_v_tok = signal.get("bc_virtual_token_reserves").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ws_mc_sol = signal.get("bc_market_cap_sol").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ws_price_sol_per_tok = signal.get("bc_price_sol_per_token").and_then(|v| v.as_f64()).unwrap_or(0.0);

    let mut payload = serde_json::json!({
        "mint": mint,
        "symbol": signal.get("symbol"),
        "name": signal.get("name"),
        "creator_wallet": signal.get("creator_wallet"),
        "entry_volume_sol": signal.get("total_volume_sol"),
        "entry_buy_count": signal.get("buy_count"),
        "entry_sell_count": signal.get("sell_count"),
        "entry_unique_buyers": signal.get("unique_buyers"),
        "entry_buy_sell_ratio": bsr,
        "entry_creator_rebuy": creator_rebuy,
        "entry_token_age_secs": signal.get("token_age_secs"),
        "entry_signals": signal.get("signals"),
        "sim_buy_sol": 0.05,
        // Migration 021: Lane-B distinguishing fields
        "entry_trigger": trigger,
        "entry_score": entry_score,
        // BC state from WS (primary source — always written if reserves seen)
        "bc_progress_pct": if ws_bc_progress > 0.0 { Some(ws_bc_progress) } else { None },
        "bc_virtual_sol_reserves": if ws_v_sol > 0.0 { Some(ws_v_sol) } else { None },
        "bc_virtual_token_reserves": if ws_v_tok > 0.0 { Some(ws_v_tok) } else { None },
        "bc_price_usd": if ws_price_sol_per_tok > 0.0 { Some(ws_price_sol_per_tok) } else { None },
        // bc_market_cap_usd: convert SOL→USD with rough $150/SOL fallback
        "bc_market_cap_usd": if ws_mc_sol > 0.0 { Some(ws_mc_sol * 150.0) } else { None },
    });

    // Merge pump.fun coin data into payload
    if let Some(ref coin) = coin_data {
        let obj = payload.as_object_mut().unwrap();

        // Price: virtual_sol_reserves / virtual_token_reserves * 1e6 (adjusted for decimals)
        let v_sol = coin.get("virtual_sol_reserves").and_then(|v| v.as_f64());
        let v_tok = coin.get("virtual_token_reserves").and_then(|v| v.as_f64());
        if let (Some(vs), Some(vt)) = (v_sol, v_tok) {
            if vt > 0.0 {
                // reserves are in lamports/raw units — convert to SOL price per token
                let price_sol = vs / vt;
                // Rough USD estimate: ~150 USD/SOL (will be approximate)
                obj.insert("bc_price_usd".to_string(), serde_json::json!(price_sol));
            }
        }

        // Market cap
        if let Some(mc) = coin.get("usd_market_cap").and_then(|v| v.as_f64()) {
            obj.insert("bc_market_cap_usd".to_string(), serde_json::json!(mc));
        }

        // Bonding curve progress %
        if let Some(progress) = coin.get("bonding_curve_progress") {
            if let Some(p) = progress.as_f64() {
                obj.insert("bc_progress_pct".to_string(), serde_json::json!(p));
            } else if let Some(s) = progress.as_str() {
                if let Ok(p) = s.parse::<f64>() {
                    obj.insert("bc_progress_pct".to_string(), serde_json::json!(p));
                }
            }
        }

        // Virtual reserves
        if let Some(vs) = v_sol {
            obj.insert("bc_virtual_sol_reserves".to_string(), serde_json::json!(vs));
        }
        if let Some(vt) = v_tok {
            obj.insert("bc_virtual_token_reserves".to_string(), serde_json::json!(vt));
        }

        // Social / metadata
        if let Some(rc) = coin.get("reply_count").and_then(|v| v.as_i64()) {
            obj.insert("bc_reply_count".to_string(), serde_json::json!(rc));
        }
        if let Some(lr) = coin.get("last_reply").and_then(|v| v.as_str()) {
            obj.insert("bc_last_reply_at".to_string(), serde_json::json!(lr));
        }
        if let Some(w) = coin.get("website").and_then(|v| v.as_str()) {
            if !w.is_empty() {
                obj.insert("bc_website".to_string(), serde_json::json!(w));
            }
        }
        if let Some(t) = coin.get("twitter").and_then(|v| v.as_str()) {
            if !t.is_empty() {
                obj.insert("bc_twitter".to_string(), serde_json::json!(t));
            }
        }
        if let Some(tg) = coin.get("telegram").and_then(|v| v.as_str()) {
            if !tg.is_empty() {
                obj.insert("bc_telegram".to_string(), serde_json::json!(tg));
            }
        }
        if let Some(koth) = coin.get("king_of_the_hill_timestamp").and_then(|v| v.as_i64()) {
            if koth > 0 {
                let dt = chrono::DateTime::from_timestamp_millis(koth);
                if let Some(d) = dt {
                    obj.insert("bc_king_of_hill_at".to_string(), serde_json::json!(d.to_rfc3339()));
                }
            }
        }

        // Store full raw response for future analysis
        obj.insert("bc_raw_response".to_string(), coin.clone());

        info!(
            mint = %mint,
            mc_usd = coin.get("usd_market_cap").and_then(|v| v.as_f64()).unwrap_or(0.0),
            reply_count = coin.get("reply_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "📡 pump.fun coin data fetched for BC paper trade"
        );
    } else {
        warn!(mint = %mint, "pump.fun coin fetch failed — recording paper trade without BC state");
    }

    let url = format!("{}/bc_paper_trades", supabase.base_url);
    // Use Prefer: return=representation so we get the inserted row back
    // (we need its id to PATCH async API-check results onto Lane-B rows).
    match supabase.client
        .post(&url)
        .header("Prefer", "return=representation")
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
            let id = rows.first().and_then(|r| r.get("id")).and_then(|v| v.as_i64());
            info!(
                mint = %mint,
                trigger,
                bsr = format!("{:.2}", bsr),
                score = format!("{:.1}", entry_score),
                row_id = ?id,
                "🧪 BC paper trade recorded (pre-grad buy simulated)"
            );
            id
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "bc_paper_trades INSERT failed: {}", body);
            None
        }
        Err(e) => {
            warn!(mint = %mint, "bc_paper_trades INSERT error: {}", e);
            None
        }
    }
}

/// Fetch bonding curve coin data from pump.fun REST API.
/// Returns the parsed JSON response or None on failure.
async fn fetch_pumpfun_coin(mint: &str) -> Option<serde_json::Value> {
    let url = format!("https://frontend-api.pump.fun/coins/{}", mint);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")
        .build()
        .ok()?;

    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json().await.ok()
}

/// Post-graduation price tracker: records price at T+1m, 5m, 15m, 1h
/// and updates the bonding_curve_signals row with multipliers.
async fn spawn_bc_price_tracker(supabase: Arc<SupabaseClient>, mint: String) {
    let intervals: &[(u64, &str)] = &[
        (60, "price_1m_multiplier"),
        (300, "price_5m_multiplier"),
        (900, "price_15m_multiplier"),
        (3600, "price_1h_multiplier"),
    ];

    // Wait before first price fetch — DexScreener needs time to index
    // newly graduated tokens. Retry up to 4 times with 10s spacing.
    let mut baseline = 0.0_f64;
    for attempt in 0..4 {
        tokio::time::sleep(std::time::Duration::from_secs(if attempt == 0 { 15 } else { 10 })).await;
        if let Some(p) = fetch_bc_price(&mint).await {
            if p > 0.0 {
                baseline = p;
                debug!(mint = %mint, attempt, price = p, "BC price tracker: baseline acquired");
                break;
            }
        }
        debug!(mint = %mint, attempt, "BC price tracker: baseline not yet available, retrying");
    }
    if baseline <= 0.0 {
        warn!(mint = %mint, "BC price tracker: no baseline after 4 attempts — skipping");
        return;
    }

    // Write baseline (graduation price) to bc_paper_trades
    {
        let pt_url = format!("{}/bc_paper_trades?mint=eq.{}", supabase.base_url, mint);
        let pt_payload = serde_json::json!({ "price_at_graduation": baseline });
        let _ = supabase.client.patch(&pt_url).json(&pt_payload).send().await;
    }

    let mut peak_price: f64 = baseline;

    // Map signal column names to bc_paper_trades column names
    let pt_columns: &[&str] = &["price_1m", "price_5m", "price_15m", "price_1h"];

    for (i, (delay_secs, column)) in intervals.iter().enumerate() {
        tokio::time::sleep(std::time::Duration::from_secs(*delay_secs)).await;

        let price = match fetch_bc_price(&mint).await {
            Some(p) if p > 0.0 => p,
            _ => continue,
        };

        if price > peak_price {
            peak_price = price;
        }

        let multiplier = price / baseline;

        // Update bonding_curve_signals
        let url = format!(
            "{}/bonding_curve_signals?mint=eq.{}",
            supabase.base_url, mint
        );
        let mut payload = serde_json::Map::new();
        payload.insert(column.to_string(), serde_json::json!(multiplier));
        payload.insert("peak_multiplier".to_string(), serde_json::json!(peak_price / baseline));
        let _ = supabase.client.patch(&url).json(&serde_json::Value::Object(payload)).send().await;

        // Update bc_paper_trades with actual prices
        let pt_url = format!("{}/bc_paper_trades?mint=eq.{}", supabase.base_url, mint);
        let mut pt_payload = serde_json::Map::new();
        pt_payload.insert(pt_columns[i].to_string(), serde_json::json!(price));
        pt_payload.insert("peak_price".to_string(), serde_json::json!(peak_price));
        pt_payload.insert("peak_multiplier".to_string(), serde_json::json!(peak_price / baseline));
        let _ = supabase.client.patch(&pt_url).json(&serde_json::Value::Object(pt_payload)).send().await;

        debug!(
            mint = %mint,
            column = column,
            multiplier = format!("{:.2}x", multiplier),
            price = format!("{:.8}", price),
            "BC signal price update"
        );
    }
}

/// Simple DexScreener price fetch for bonding curve signal tracking.
async fn fetch_bc_price(mint: &str) -> Option<f64> {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", mint);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
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
