use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use super::tick_stream::{Tick, TickDirection, TickWindow};
use super::types::{ExitReason, ExitSignal};

/// Maximum back-off duration between reconnection attempts.
const MAX_BACKOFF_SECS: u64 = 10;
/// Initial back-off duration after a disconnect.
const INITIAL_BACKOFF_SECS: u64 = 1;

/// Metadata needed to construct an ExitSignal when a watcher fires.
#[derive(Debug, Clone)]
pub struct WatchContext {
    pub position_id: i64,
    pub mint: String,
    pub current_price: f64,
    pub entry_price_usd: f64,
    pub sol_spent: f64,
    pub token_amount: f64,
    pub is_paper_trade: bool,
}

/// Real-time account watcher using Helius enhanced WebSocket.
///
/// Spawns `accountSubscribe` on the given accounts and sends emergency
/// exit signals when thresholds are breached.  Falls back gracefully
/// to the polling-based monitoring if the WebSocket is unavailable.
pub struct HeliusWatcher {
    ws_url: String,
}

impl HeliusWatcher {
    pub fn new(ws_url: String) -> Self {
        Self { ws_url }
    }

    // ── Dev wallet token account watcher ──────────────────

    /// Subscribe to the dev wallet's token account.  Fires an emergency
    /// exit when the token balance drops by more than `threshold_pct`
    /// from `initial_balance`.
    ///
    /// Runs until `shutdown` is cancelled or the exit_tx channel closes.
    pub async fn watch_dev_wallet(
        &self,
        dev_token_account: Pubkey,
        initial_balance: u64,
        threshold_pct: f64,
        exit_tx: mpsc::Sender<ExitSignal>,
        ctx: WatchContext,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        if initial_balance == 0 {
            debug!(
                mint = %ctx.mint,
                "Helius dev-wallet watcher skipped — initial balance is 0"
            );
            return;
        }

        info!(
            mint = %ctx.mint,
            dev_token_account = %dev_token_account,
            initial_balance,
            "🔌 Starting Helius dev-wallet watcher"
        );

        let mut backoff = INITIAL_BACKOFF_SECS;

        loop {
            match self
                .subscribe_and_watch_token_balance(
                    &dev_token_account,
                    initial_balance,
                    threshold_pct,
                    &exit_tx,
                    &ctx,
                    ExitReason::DevWalletDumping,
                    &mut shutdown,
                )
                .await
            {
                Ok(true) => {
                    // Threshold breached — exit signal sent, stop watching
                    return;
                }
                Ok(false) => {
                    // Shutdown requested
                    debug!(mint = %ctx.mint, "Helius dev-wallet watcher shutting down");
                    return;
                }
                Err(e) => {
                    warn!(
                        mint = %ctx.mint,
                        "Helius dev-wallet WS error: {:#}. Reconnecting in {}s",
                        e, backoff
                    );
                }
            }

            // Check shutdown before sleeping
            if *shutdown.borrow() {
                return;
            }

            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
        }
    }

    // ── LP vault watcher ─────────────────────────────────

    /// Subscribe to the LP vault's SOL account.  Fires an emergency
    /// exit when the lamport balance drops by more than `threshold_pct`
    /// from `initial_lamports`.
    pub async fn watch_lp_vault(
        &self,
        lp_vault_account: Pubkey,
        initial_lamports: u64,
        threshold_pct: f64,
        exit_tx: mpsc::Sender<ExitSignal>,
        ctx: WatchContext,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        if initial_lamports == 0 {
            debug!(
                mint = %ctx.mint,
                "Helius LP-vault watcher skipped — initial lamports is 0"
            );
            return;
        }

        info!(
            mint = %ctx.mint,
            lp_vault_account = %lp_vault_account,
            initial_lamports,
            "🔌 Starting Helius LP-vault watcher"
        );

        let mut backoff = INITIAL_BACKOFF_SECS;

        loop {
            match self
                .subscribe_and_watch_lamports(
                    &lp_vault_account,
                    initial_lamports,
                    threshold_pct,
                    &exit_tx,
                    &ctx,
                    ExitReason::LiquidityRemoved,
                    &mut shutdown,
                )
                .await
            {
                Ok(true) => return,
                Ok(false) => {
                    debug!(mint = %ctx.mint, "Helius LP-vault watcher shutting down");
                    return;
                }
                Err(e) => {
                    warn!(
                        mint = %ctx.mint,
                        "Helius LP-vault WS error: {:#}. Reconnecting in {}s",
                        e, backoff
                    );
                }
            }

            if *shutdown.borrow() {
                return;
            }

            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
        }
    }

    // ── Pool token vault watcher (tick stream) ─────────

    /// Subscribe to the pool's token vault account to derive a real-time
    /// tick stream.  Every balance change = a trade.
    ///
    /// - Balance decreases → someone bought tokens (tokens left the vault).
    /// - Balance increases → someone sold tokens (tokens entered the vault).
    ///
    /// Each tick is pushed into the shared `tick_window` for momentum analysis.
    /// Also updates `sol_vault_balance` by subscribing to the SOL vault so
    /// we can estimate SOL amounts per trade.
    pub async fn watch_pool_trades(
        &self,
        token_vault_account: Pubkey,
        sol_vault_account: Pubkey,
        initial_token_balance: u64,
        initial_sol_lamports: u64,
        tick_window: Arc<Mutex<TickWindow>>,
        mint_for_log: String,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        sol_vault_is_token_account: bool,
    ) {
        info!(
            mint = %mint_for_log,
            token_vault = %token_vault_account,
            sol_vault = %sol_vault_account,
            initial_token_balance,
            initial_sol_lamports,
            sol_vault_is_token_account,
            "🔌 Starting pool trade tick watcher"
        );

        let mut backoff = INITIAL_BACKOFF_SECS;

        loop {
            match self
                .subscribe_pool_vaults(
                    &token_vault_account,
                    &sol_vault_account,
                    initial_token_balance,
                    initial_sol_lamports,
                    &tick_window,
                    &mint_for_log,
                    &mut shutdown,
                    sol_vault_is_token_account,
                )
                .await
            {
                Ok(false) => {
                    debug!(mint = %mint_for_log, "Pool trade watcher shutting down");
                    return;
                }
                Ok(true) => {
                    // Shouldn't happen for this watcher, but handle gracefully
                    return;
                }
                Err(e) => {
                    warn!(
                        mint = %mint_for_log,
                        "Pool trade WS error: {:#}. Reconnecting in {}s",
                        e, backoff
                    );
                }
            }

            if *shutdown.borrow() {
                return;
            }

            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
        }
    }

    /// Internal: subscribe to both vault accounts on a single WS connection
    /// and emit ticks from balance changes.
    async fn subscribe_pool_vaults(
        &self,
        token_vault: &Pubkey,
        sol_vault: &Pubkey,
        initial_token_balance: u64,
        initial_sol_lamports: u64,
        tick_window: &Arc<Mutex<TickWindow>>,
        mint: &str,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
        sol_vault_is_token_account: bool,
    ) -> Result<bool> {
        let (ws, _) = connect_async(&self.ws_url)
            .await
            .context("Failed to connect to Helius WebSocket (pool vaults)")?;

        let (mut write, mut read) = ws.split();

        // Subscribe to token vault (jsonParsed for token balance)
        let sub_token = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "accountSubscribe",
            "params": [
                token_vault.to_string(),
                { "encoding": "jsonParsed", "commitment": "confirmed" }
            ]
        });
        write
            .send(Message::Text(sub_token.to_string()))
            .await
            .context("Failed to subscribe token vault")?;

        // Subscribe to SOL vault.
        // For PumpSwap: WSOL is a token account, use jsonParsed.
        // For Raydium: raw SOL, use base64 to read lamports.
        let sol_encoding = if sol_vault_is_token_account {
            "jsonParsed"
        } else {
            "base64"
        };
        let sub_sol = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "accountSubscribe",
            "params": [
                sol_vault.to_string(),
                { "encoding": sol_encoding, "commitment": "confirmed" }
            ]
        });
        write
            .send(Message::Text(sub_sol.to_string()))
            .await
            .context("Failed to subscribe SOL vault")?;

        debug!(mint = %mint, "accountSubscribe sent for both pool vaults");

        let mut prev_token_balance: u64 = initial_token_balance;
        let mut prev_sol_lamports: u64 = initial_sol_lamports;
        // Track subscription IDs to route notifications
        let mut token_sub_id: Option<u64> = None;
        let mut sol_sub_id: Option<u64> = None;

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            let v: serde_json::Value = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };

                            // Handle subscription confirmations
                            if let (Some(id), Some(result)) = (v.get("id").and_then(|i| i.as_u64()), v.get("result").and_then(|r| r.as_u64())) {
                                if id == 10 { token_sub_id = Some(result); }
                                if id == 11 { sol_sub_id = Some(result); }
                                continue;
                            }

                            // Route notifications by subscription ID
                            let notif_sub = v.get("params")
                                .and_then(|p| p.get("subscription"))
                                .and_then(|s| s.as_u64());

                            if notif_sub == token_sub_id && token_sub_id.is_some() {
                                // Token vault balance update
                                if let Some(balance) = parse_token_balance(&text) {
                                    if balance != prev_token_balance {
                                        let (direction, token_delta) = if balance < prev_token_balance {
                                            (TickDirection::Buy, prev_token_balance - balance)
                                        } else {
                                            (TickDirection::Sell, balance - prev_token_balance)
                                        };

                                        // Estimate SOL value from concurrent SOL vault change
                                        let sol_amount = {
                                            // Simple estimate: use the ratio of token delta to total supply
                                            // to approximate SOL amount. This is rough but fast.
                                            // More accurate: derived from SOL vault lamport changes.
                                            0.0 // Will be updated when SOL vault notification arrives
                                        };

                                        let tick = Tick {
                                            direction,
                                            token_delta,
                                            sol_amount,
                                            timestamp: Instant::now(),
                                        };

                                        debug!(
                                            mint = %mint,
                                            direction = ?direction,
                                            token_delta,
                                            prev = prev_token_balance,
                                            new = balance,
                                            "📊 Pool tick"
                                        );

                                        prev_token_balance = balance;
                                        tick_window.lock().await.push(tick);
                                    }
                                }
                            } else if notif_sub == sol_sub_id && sol_sub_id.is_some() {
                                // SOL vault update — parse as token balance (PumpSwap WSOL)
                                // or raw lamports (Raydium)
                                let new_balance = if sol_vault_is_token_account {
                                    parse_token_balance(&text)
                                } else {
                                    parse_lamports(&text)
                                };
                                if let Some(lamports) = new_balance {
                                    if lamports != prev_sol_lamports {
                                        let sol_delta = if lamports > prev_sol_lamports {
                                            // More SOL in vault = someone bought (sent SOL)
                                            (lamports - prev_sol_lamports) as f64 / 1_000_000_000.0
                                        } else {
                                            // Less SOL in vault = someone sold (took SOL)
                                            (prev_sol_lamports - lamports) as f64 / 1_000_000_000.0
                                        };

                                        prev_sol_lamports = lamports;

                                        // Update the most recent tick's sol_amount if it doesn't have one
                                        let mut window = tick_window.lock().await;
                                        // Find the last tick within 500ms that has sol_amount == 0
                                        // (the paired token vault update)
                                        let _now = Instant::now();
                                        let _ticks_ref = &mut *window;
                                        // We need direct access to the deque — use the tick count
                                        // The most recent tick is the one we want to update
                                        // Since we can't access ticks directly, we'll push
                                        // a synthetic amend by noting the SOL delta.
                                        // For simplicity, we always set sol_amount on the
                                        // most recent tick via a dedicated method.
                                        drop(window);
                                        // Re-acquire to call a method that updates last tick
                                        tick_window.lock().await.update_last_tick_sol(sol_delta);
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            return Err(anyhow::anyhow!("WebSocket closed by server"));
                        }
                        Some(Err(e)) => {
                            return Err(e.into());
                        }
                        None => {
                            return Err(anyhow::anyhow!("WebSocket stream ended"));
                        }
                        _ => {}
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(false);
                    }
                }
            }
        }
    }

    // ── Internal: token account balance watcher ──────────

    /// Connect, subscribe to an SPL token account, and watch for balance drops.
    /// Returns Ok(true) if threshold was breached, Ok(false) if shutdown.
    async fn subscribe_and_watch_token_balance(
        &self,
        account: &Pubkey,
        initial_balance: u64,
        threshold_pct: f64,
        exit_tx: &mpsc::Sender<ExitSignal>,
        ctx: &WatchContext,
        reason: ExitReason,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<bool> {
        let (ws, _) = connect_async(&self.ws_url)
            .await
            .context("Failed to connect to Helius WebSocket")?;

        let (mut write, mut read) = ws.split();

        // Subscribe to the account with jsonParsed encoding so we get
        // structured token balance data in notifications.
        let sub_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                account.to_string(),
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed"
                }
            ]
        });

        write
            .send(Message::Text(sub_msg.to_string()))
            .await
            .context("Failed to send accountSubscribe")?;

        debug!(account = %account, "accountSubscribe sent (token balance)");

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(balance) = parse_token_balance(&text) {
                                let drop_pct = if balance < initial_balance {
                                    (initial_balance - balance) as f64
                                        / initial_balance as f64
                                        * 100.0
                                } else {
                                    0.0
                                };

                                debug!(
                                    mint = %ctx.mint,
                                    account = %account,
                                    initial_balance,
                                    current_balance = balance,
                                    drop_pct = format!("{:.1}%", drop_pct),
                                    "Helius token balance update"
                                );

                                if drop_pct >= threshold_pct {
                                    warn!(
                                        mint = %ctx.mint,
                                        drop_pct = format!("{:.1}%", drop_pct),
                                        "🚨 Helius detected dev wallet dump — emergency exit"
                                    );

                                    let signal = ExitSignal {
                                        position_id: ctx.position_id,
                                        mint: ctx.mint.clone(),
                                        pct_to_sell: 100,
                                        reason,
                                        current_price: ctx.current_price,
                                        entry_price_usd: ctx.entry_price_usd,
                                        sol_spent: ctx.sol_spent,
                                        token_amount: ctx.token_amount,
                                        is_paper_trade: ctx.is_paper_trade,
                                        sub_reason: None,
                                    };

                                    let _ = exit_tx.send(signal).await;
                                    return Ok(true);
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            return Err(anyhow::anyhow!("WebSocket closed by server"));
                        }
                        Some(Err(e)) => {
                            return Err(e.into());
                        }
                        None => {
                            return Err(anyhow::anyhow!("WebSocket stream ended"));
                        }
                        _ => {}
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(false);
                    }
                }
            }
        }
    }

    // ── Internal: lamport balance watcher (LP vault) ─────

    /// Connect, subscribe to a system account, and watch for lamport drops.
    /// Returns Ok(true) if threshold was breached, Ok(false) if shutdown.
    async fn subscribe_and_watch_lamports(
        &self,
        account: &Pubkey,
        initial_lamports: u64,
        threshold_pct: f64,
        exit_tx: &mpsc::Sender<ExitSignal>,
        ctx: &WatchContext,
        reason: ExitReason,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<bool> {
        let (ws, _) = connect_async(&self.ws_url)
            .await
            .context("Failed to connect to Helius WebSocket")?;

        let (mut write, mut read) = ws.split();

        // For the LP vault we only need the lamports field, so plain
        // encoding is fine (smaller payloads).
        let sub_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "accountSubscribe",
            "params": [
                account.to_string(),
                {
                    "encoding": "base64",
                    "commitment": "confirmed"
                }
            ]
        });

        write
            .send(Message::Text(sub_msg.to_string()))
            .await
            .context("Failed to send accountSubscribe")?;

        debug!(account = %account, "accountSubscribe sent (lamports)");

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(lamports) = parse_lamports(&text) {
                                let drop_pct = if lamports < initial_lamports {
                                    (initial_lamports - lamports) as f64
                                        / initial_lamports as f64
                                        * 100.0
                                } else {
                                    0.0
                                };

                                debug!(
                                    mint = %ctx.mint,
                                    account = %account,
                                    initial_lamports,
                                    current_lamports = lamports,
                                    drop_pct = format!("{:.1}%", drop_pct),
                                    "Helius LP vault balance update"
                                );

                                if drop_pct >= threshold_pct {
                                    warn!(
                                        mint = %ctx.mint,
                                        drop_pct = format!("{:.1}%", drop_pct),
                                        "🚨 Helius detected LP removal — emergency exit"
                                    );

                                    let signal = ExitSignal {
                                        position_id: ctx.position_id,
                                        mint: ctx.mint.clone(),
                                        pct_to_sell: 100,
                                        reason,
                                        current_price: ctx.current_price,
                                        entry_price_usd: ctx.entry_price_usd,
                                        sol_spent: ctx.sol_spent,
                                        token_amount: ctx.token_amount,
                                        is_paper_trade: ctx.is_paper_trade,
                                        sub_reason: None,
                                    };

                                    let _ = exit_tx.send(signal).await;
                                    return Ok(true);
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            return Err(anyhow::anyhow!("WebSocket closed by server"));
                        }
                        Some(Err(e)) => {
                            return Err(e.into());
                        }
                        None => {
                            return Err(anyhow::anyhow!("WebSocket stream ended"));
                        }
                        _ => {}
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(false);
                    }
                }
            }
        }
    }
}

// ── JSON parsing helpers ─────────────────────────────────────

/// Extract the token amount from a Helius `accountNotification` with
/// jsonParsed encoding.
///
/// Expected path: params.result.value.data.parsed.info.tokenAmount.amount
fn parse_token_balance(text: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Skip subscription confirmation messages (they have "result" at top level)
    if v.get("result").is_some() && v.get("method").is_none() {
        return None;
    }

    v.get("params")?
        .get("result")?
        .get("value")?
        .get("data")?
        .get("parsed")?
        .get("info")?
        .get("tokenAmount")?
        .get("amount")?
        .as_str()?
        .parse::<u64>()
        .ok()
}

/// Extract the lamports field from a Helius `accountNotification`.
///
/// Expected path: params.result.value.lamports
fn parse_lamports(text: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Skip subscription confirmation messages
    if v.get("result").is_some() && v.get("method").is_none() {
        return None;
    }

    v.get("params")?
        .get("result")?
        .get("value")?
        .get("lamports")?
        .as_u64()
}

/// Derive the associated token account (ATA) for a wallet + mint.
///
/// This is the standard SPL ATA derivation used by all Solana wallets.
pub fn derive_ata(wallet: &Pubkey, mint: &Pubkey) -> Pubkey {
    let spl_token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        .expect("hardcoded SPL Token program ID is valid");
    let ata_program = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
        .expect("hardcoded ATA program ID is valid");

    Pubkey::find_program_address(
        &[wallet.as_ref(), spl_token_program.as_ref(), mint.as_ref()],
        &ata_program,
    )
    .0
}

/// PumpSwap AMM program ID.
const PUMP_AMM_PROGRAM_ID: &str = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj";

/// Resolved vault addresses for a pool.
pub struct PoolVaults {
    /// Token vault (meme token) — always an SPL token account.
    pub token_vault: Pubkey,
    /// SOL/quote vault — for PumpSwap this is a WSOL token account,
    /// for Raydium this is a raw SOL account.
    pub sol_vault: Pubkey,
    /// Whether the SOL vault is an SPL token account (WSOL) rather than
    /// a raw SOL account.  True for PumpSwap, false for Raydium.
    pub sol_vault_is_token_account: bool,
}

/// Resolve the actual vault token accounts for a pool by fetching its
/// on-chain data.  Currently supports PumpSwap; returns `None` for
/// other AMMs so callers can fall back to legacy derivation.
///
/// PumpSwap Pool layout (Anchor/Borsh, after 8-byte discriminator):
///   [8]     pool_bump        u8
///   [9..11] index            u16
///   [11..43]  creator        Pubkey
///   ... (additional fields) ...
///   [205..237] base_mint      Pubkey   (meme token)
///   [237..269] quote_mint     Pubkey   (WSOL)
///   [269..301] pool_base_token_account  Pubkey  ← meme token vault
///   [301..333] pool_quote_token_account Pubkey  ← WSOL vault
pub async fn resolve_pool_vaults(
    rpc: &RpcClient,
    pool_pubkey: &Pubkey,
    mint: &Pubkey,
) -> Option<PoolVaults> {
    let account = match rpc.get_account(pool_pubkey).await {
        Ok(a) => a,
        Err(e) => {
            debug!(pool = %pool_pubkey, "Could not fetch pool account for vault resolution: {}", e);
            return None;
        }
    };

    let pump_amm =
        Pubkey::from_str(PUMP_AMM_PROGRAM_ID).expect("hardcoded PumpSwap program ID is valid");
    if account.owner != pump_amm {
        debug!(
            pool = %pool_pubkey,
            owner = %account.owner,
            "Pool is not PumpSwap — falling back to legacy vault derivation"
        );
        return None;
    }

    let data = &account.data;
    if data.len() < 333 {
        warn!(
            pool = %pool_pubkey,
            data_len = data.len(),
            "PumpSwap pool account data too short for vault parsing"
        );
        return None;
    }

    let base_mint = Pubkey::try_from(&data[205..237]).ok()?;
    let quote_mint = Pubkey::try_from(&data[237..269]).ok()?;
    let pool_base_vault = Pubkey::try_from(&data[269..301]).ok()?;
    let pool_quote_vault = Pubkey::try_from(&data[301..333]).ok()?;

    // Validate: one of the mints should match the token we're tracking.
    let wsol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
        .expect("hardcoded WSOL mint is valid");

    if base_mint == *mint && quote_mint == wsol_mint {
        // base = meme token, quote = WSOL  (standard PumpSwap layout)
        info!(
            pool = %pool_pubkey,
            token_vault = %pool_base_vault,
            sol_vault = %pool_quote_vault,
            "🔍 Resolved PumpSwap pool vaults from on-chain data"
        );
        Some(PoolVaults {
            token_vault: pool_base_vault,
            sol_vault: pool_quote_vault,
            sol_vault_is_token_account: true,
        })
    } else if quote_mint == *mint && base_mint == wsol_mint {
        // Reverse layout (unlikely but handle gracefully)
        info!(
            pool = %pool_pubkey,
            token_vault = %pool_quote_vault,
            sol_vault = %pool_base_vault,
            "🔍 Resolved PumpSwap pool vaults (reversed layout)"
        );
        Some(PoolVaults {
            token_vault: pool_quote_vault,
            sol_vault: pool_base_vault,
            sol_vault_is_token_account: true,
        })
    } else {
        warn!(
            pool = %pool_pubkey,
            base_mint = %base_mint,
            quote_mint = %quote_mint,
            expected_mint = %mint,
            "PumpSwap pool mints don't match expected token — vault resolution failed"
        );
        None
    }
}
