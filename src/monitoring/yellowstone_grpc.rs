//! Chainstack Yellowstone gRPC Geyser client — pump.fun bonding-curve price stream.
//!
//! ── Architecture ──────────────────────────────────────────────────────────
//! One long-lived gRPC stream subscribes to the pump.fun program with an
//! `accounts` filter that matches only the active positions' bonding-curve
//! PDAs. Chainstack filters server-side, so we receive only the accounts we
//! care about — zero wasted bytes.
//!
//! The mux owns the subscription state and reissues the `SubscribeRequest`
//! whenever the active-position set changes (add/remove). Per-position
//! monitor tasks interact through a `PriceStreamHandle` identical in shape
//! to `helius_price_ws::PriceStreamHandle` so the call sites in
//! `monitoring/mod.rs` work with either backend.
//!
//! ── Why gRPC over Helius WS ──────────────────────────────────────────────
//! - Helius Enhanced WS `accountSubscribe` silently drops notifications on
//!   the Developer plan (verified empirically: 0 notifications received
//!   across 4 confirmed subs in 37s of logs).
//! - Chainstack Yellowstone streams events straight from validator memory
//!   with Jito ShredStream enabled by default. $49/mo with unlimited events.
//! - Up to 50 accounts per stream; we cap at 6 concurrent positions, so
//!   we sit well under the limit.
//!
//! ── Reconnect / replay ───────────────────────────────────────────────────
//! On any stream error the mux reconnects with exponential backoff and
//! resubscribes with the current active set. The cache is *not* cleared —
//! consumers hit `get_fresh` with a 3s freshness window, so briefly stale
//! prices degrade to Jupiter automatically.
//!
//! ── Observability ────────────────────────────────────────────────────────
//! - `helius_ws_subscribed` / `helius_ws_graduated` (reusing the existing
//!   event_type strings so Supabase queries continue to work)
//! - `yellowstone_grpc_reconnect` on reconnect
//! - `helius_ws_metrics` every 60s with the usual hit/miss/active counters

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions, SubscribeUpdate,
};

use crate::detection::types::{DetectionSource, GraduatedToken, PipelineTiming};
use crate::logger::SupabaseClient;
use crate::monitoring::helius_price_ws::{BondingCurveSnapshot, HeliusPriceCache};

const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const RAYDIUM_INITIALIZE2_TAG: u8 = 1;
const RAYDIUM_INIT2_POOL_ACCOUNT_POS: usize = 4;
const RAYDIUM_INIT2_COIN_MINT_POS: usize = 8;
const RAYDIUM_INIT2_PC_MINT_POS: usize = 9;

const MAX_BACKOFF_SECS: u64 = 10;
const INITIAL_BACKOFF_SECS: u64 = 1;

// ── Control protocol ──────────────────────────────────────────

#[derive(Debug)]
enum MuxCommand {
    Subscribe {
        mint: String,
    },
    Unsubscribe {
        mint: String,
    },
    /// Watch a dev wallet's token account for balance drops (rug detection).
    WatchDevWallet {
        mint: String,
        dev_token_account: String,
    },
}

/// Cheap-to-clone handle. API-compatible with `helius_price_ws::PriceStreamHandle`.
#[derive(Clone)]
pub struct YellowstonePriceHandle {
    tx: mpsc::UnboundedSender<MuxCommand>,
}

impl YellowstonePriceHandle {
    pub fn subscribe(&self, mint: String) {
        if mint == SOL_MINT {
            return;
        }
        if self.tx.send(MuxCommand::Subscribe { mint }).is_err() {
            warn!("yellowstone_grpc: mux channel closed — cannot subscribe");
        }
    }

    pub fn unsubscribe(&self, mint: String) {
        let _ = self.tx.send(MuxCommand::Unsubscribe { mint });
    }

    /// Subscribe to dev wallet token account changes for rug detection.
    pub fn watch_dev_wallet(&self, mint: String, dev_token_account: String) {
        let _ = self.tx.send(MuxCommand::WatchDevWallet {
            mint,
            dev_token_account,
        });
    }
}

// ── Mux state ────────────────────────────────────────────────

struct MintState {
    pda: Pubkey,
    pda_str: String,
    /// Dev wallet token account to watch for rug-dump (if known).
    /// Stored as raw bytes for fast comparison against gRPC account updates.
    dev_ata_bytes: Option<Vec<u8>>,
}

fn derive_bonding_curve_pda(mint: &str) -> Option<(Pubkey, String)> {
    let mint_pk = Pubkey::from_str(mint).ok()?;
    let program = Pubkey::from_str(PUMPFUN_PROGRAM).ok()?;
    let (pda, _) = Pubkey::find_program_address(&[b"bonding-curve", mint_pk.as_ref()], &program);
    let pda_str = pda.to_string();
    Some((pda, pda_str))
}

pub struct YellowstoneConfig {
    /// gRPC endpoint, e.g. `https://yellowstone-solana-mainnet.core.chainstack.com`.
    pub endpoint: String,
    /// Primary auth: x-token header.
    pub x_token: Option<String>,
    /// Fallback auth: HTTP Basic. Used only if x_token is None.
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Start the gRPC mux task plus the metrics flusher from
/// `helius_price_ws::flush_metrics_loop` (already running — we share the cache).
///
/// - `graduation_tx`: if provided, Raydium pool creation events are sent here
///   to feed back into the detection → filter pipeline as a backup source.
/// - `bot_wallet`: if provided, subscribes to the bot's transactions for
///   instant confirmation (avoids polling `getSignatureStatuses`).
///
/// Returns a handle API-compatible with the Helius one.
pub fn start_mux(
    config: YellowstoneConfig,
    cache: Arc<HeliusPriceCache>,
    supabase: Arc<SupabaseClient>,
    graduation_tx: Option<mpsc::Sender<GraduatedToken>>,
    bot_wallet: Option<String>,
) -> YellowstonePriceHandle {
    let (tx, rx) = mpsc::unbounded_channel::<MuxCommand>();

    tokio::spawn(async move {
        run_mux(config, cache, supabase, rx, graduation_tx, bot_wallet).await;
    });

    info!("📡 yellowstone_grpc mux started (Chainstack, single stream, server-side filtered)");
    YellowstonePriceHandle { tx }
}

async fn run_mux(
    config: YellowstoneConfig,
    cache: Arc<HeliusPriceCache>,
    supabase: Arc<SupabaseClient>,
    mut rx: mpsc::UnboundedReceiver<MuxCommand>,
    graduation_tx: Option<mpsc::Sender<GraduatedToken>>,
    bot_wallet: Option<String>,
) {
    let mut active: HashMap<String, MintState> = HashMap::new();
    let mut backoff = INITIAL_BACKOFF_SECS;

    loop {
        match run_session(
            &config,
            &cache,
            &supabase,
            &mut active,
            &mut rx,
            &mut backoff,
            &graduation_tx,
            &bot_wallet,
        )
        .await
        {
            Ok(()) => {
                info!("yellowstone_grpc mux: control channel closed, exiting");
                return;
            }
            Err(e) => {
                cache.record_reconnect();
                warn!(
                    error = %e,
                    active_subs = active.len(),
                    backoff_secs = backoff,
                    "yellowstone_grpc mux: session ended, will reconnect"
                );
                log_event(
                    &supabase,
                    "yellowstone_grpc_reconnect",
                    &format!("Reason: {} | active_subs: {}", e, active.len()),
                )
                .await;
            }
        }
        sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
    }
}

async fn run_session(
    config: &YellowstoneConfig,
    cache: &HeliusPriceCache,
    supabase: &SupabaseClient,
    active: &mut HashMap<String, MintState>,
    rx: &mut mpsc::UnboundedReceiver<MuxCommand>,
    backoff: &mut u64,
    graduation_tx: &Option<mpsc::Sender<GraduatedToken>>,
    bot_wallet: &Option<String>,
) -> Result<()> {
    // ── 1. Connect ──────────────────────────────────────────────
    // v1.14 builder: build_from_shared -> x_token -> tls_config -> connect.
    // Basic auth (if ever needed) is done by embedding credentials in the
    // endpoint URL as https://user:pass@host — Tonic forwards it.
    let endpoint = if config.x_token.is_none() {
        if let (Some(u), Some(p)) = (&config.username, &config.password) {
            if !p.is_empty() {
                inject_basic_auth(&config.endpoint, u, p)?
            } else {
                config.endpoint.clone()
            }
        } else {
            config.endpoint.clone()
        }
    } else {
        config.endpoint.clone()
    };

    let mut client = GeyserGrpcClient::build_from_shared(endpoint)
        .context("yellowstone_grpc: build endpoint")?
        .x_token(config.x_token.clone())
        .context("yellowstone_grpc: attach x-token")?
        .tls_config(tonic::transport::ClientTlsConfig::new())
        .context("yellowstone_grpc: tls config")?
        .connect()
        .await
        .context("yellowstone_grpc: connect")?;

    info!(
        existing_subs = active.len(),
        "yellowstone_grpc mux: stream connected"
    );
    // Reset backoff on successful connect so next disconnect starts fresh.
    *backoff = INITIAL_BACKOFF_SECS;

    let (mut sink, mut stream) = client
        .subscribe()
        .await
        .context("yellowstone_grpc: open subscribe stream")?;

    // Dedup graduation events per-mint across the session. A single
    // Raydium AMM pool creation can emit many CPI inner instructions in
    // the same slot/tx, and the stream may deliver multiple txs for the
    // same mint across consecutive slots. Only the first matters.
    let mut seen_graduation_mints: HashSet<String> = HashSet::new();

    // ── 2. Issue initial request with the current active set ────
    send_subscribe_request(&mut sink, active, bot_wallet).await?;

    // ── 3. Multiplex commands + stream updates ──────────────────
    loop {
        tokio::select! {
            cmd = rx.recv() => {
                let cmd = match cmd { Some(c) => c, None => return Ok(()) };
                match cmd {
                    MuxCommand::Subscribe { mint } => {
                        if active.contains_key(&mint) {
                            debug!(mint = %mint, "yellowstone_grpc mux: already subscribed");
                            continue;
                        }
                        let (pda, pda_str) = match derive_bonding_curve_pda(&mint) {
                            Some(p) => p,
                            None => {
                                warn!(mint = %mint, "yellowstone_grpc mux: invalid mint");
                                continue;
                            }
                        };
                        active.insert(mint.clone(), MintState { pda, pda_str: pda_str.clone(), dev_ata_bytes: None });
                        send_subscribe_request(&mut sink, active, bot_wallet).await?;
                        log_event(
                            supabase,
                            "helius_ws_subscribed",
                            &format!("mint: {} | bonding_curve: {} | active: {} | backend: yellowstone_grpc", mint, pda_str, active.len()),
                        ).await;
                    }
                    MuxCommand::Unsubscribe { mint } => {
                        if active.remove(&mint).is_some() {
                            cache.remove(&mint);
                            send_subscribe_request(&mut sink, active, bot_wallet).await?;
                            debug!(mint = %mint, active = active.len(), "yellowstone_grpc mux: unsubscribed");
                        }
                    }
                    MuxCommand::WatchDevWallet { mint, dev_token_account } => {
                        if let Some(state) = active.get_mut(&mint) {
                            let ata_bytes = bs58::decode(&dev_token_account).into_vec();
                            match ata_bytes {
                                Ok(bytes) => {
                                    info!(mint = %mint, dev_ata = %dev_token_account, "yellowstone_grpc mux: watching dev wallet");
                                    state.dev_ata_bytes = Some(bytes);
                                }
                                Err(e) => {
                                    warn!(mint = %mint, dev_ata = %dev_token_account, "yellowstone_grpc mux: invalid dev ATA address: {}", e);
                                    continue;
                                }
                            }
                            send_subscribe_request(&mut sink, active, bot_wallet).await?;
                        } else {
                            warn!(mint = %mint, "yellowstone_grpc mux: WatchDevWallet for unknown mint");
                        }
                    }
                }
            }
            msg = stream.next() => {
                let update = match msg {
                    Some(Ok(u)) => u,
                    Some(Err(e)) => return Err(anyhow::anyhow!("stream error: {}", e)),
                    None => return Err(anyhow::anyhow!("stream closed")),
                };
                handle_update(update, cache, supabase, active, graduation_tx, &mut seen_graduation_mints).await;
            }
        }
    }
}

/// Build and send a `SubscribeRequest` covering every currently-active PDA,
/// dev wallet token accounts, and the bot wallet transaction filter.
///
/// Yellowstone treats each `SubscribeRequest` as a full-state replacement
/// for the named filter, so we always rebuild from scratch.
async fn send_subscribe_request<S>(
    sink: &mut S,
    active: &HashMap<String, MintState>,
    bot_wallet: &Option<String>,
) -> Result<()>
where
    S: SinkExt<SubscribeRequest> + Unpin,
    <S as futures_util::Sink<SubscribeRequest>>::Error: std::fmt::Display,
{
    let account_list: Vec<String> = active.values().map(|s| s.pda_str.clone()).collect();

    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    if !account_list.is_empty() {
        accounts.insert(
            "pumpfun_bonding_curves".to_string(),
            SubscribeRequestFilterAccounts {
                account: account_list,
                owner: vec![PUMPFUN_PROGRAM.to_string()],
                filters: vec![],
            },
        );
    }

    // Filter #2: Dev wallet token accounts — detect rug-dumps in real time
    let dev_ata_list: Vec<String> = active
        .values()
        .filter_map(|s| {
            s.dev_ata_bytes
                .as_ref()
                .map(|b| bs58::encode(b).into_string())
        })
        .collect();
    if !dev_ata_list.is_empty() {
        accounts.insert(
            "dev_wallet_accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: dev_ata_list,
                owner: vec![], // Token accounts owned by Token Program, but we match by address
                filters: vec![],
            },
        );
    }

    // Transactions filter: Raydium graduation + bot wallet confirmations
    let mut tx_filters = build_tx_filters();

    // Filter #2 (transactions): Bot wallet tx confirmation — instant confirmation
    // instead of polling getSignatureStatuses
    if let Some(wallet) = bot_wallet {
        tx_filters.insert(
            "bot_wallet_txs".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![wallet.clone()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
    }

    let req = SubscribeRequest {
        accounts,
        slots: HashMap::new(),
        transactions: tx_filters,
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
    };

    sink.send(req)
        .await
        .map_err(|e| anyhow::anyhow!("send subscribe: {}", e))?;
    debug!(
        active = active.len(),
        "yellowstone_grpc mux: subscribe request sent"
    );
    Ok(())
}

async fn handle_update(
    update: SubscribeUpdate,
    cache: &HeliusPriceCache,
    supabase: &SupabaseClient,
    active: &mut HashMap<String, MintState>,
    graduation_tx: &Option<mpsc::Sender<GraduatedToken>>,
    seen_graduation_mints: &mut HashSet<String>,
) {
    let Some(oneof) = update.update_oneof else {
        return;
    };

    let account_update = match oneof {
        UpdateOneof::Account(a) => a,
        UpdateOneof::Transaction(tx_update) => {
            handle_transaction_update(
                tx_update,
                cache,
                supabase,
                active,
                graduation_tx,
                seen_graduation_mints,
            )
            .await;
            return;
        }
        UpdateOneof::Ping(_) => {
            debug!("yellowstone_grpc mux: ping");
            return;
        }
        UpdateOneof::Pong(_) => return,
        _ => {
            return;
        }
    };

    let Some(acc) = account_update.account else {
        return;
    };

    // Map pubkey (bytes) back to mint by scanning active state.
    // With 6 max positions this is trivially fast.
    let pubkey_bytes = acc.pubkey.as_slice();

    // Check if this is a dev wallet token account update (rug detection).
    let dev_match = active
        .iter()
        .find(|(_, st)| {
            st.dev_ata_bytes
                .as_ref()
                .map_or(false, |bytes| bytes.as_slice() == pubkey_bytes)
        })
        .map(|(m, _)| m.clone());
    if let Some(dev_mint) = dev_match {
        // Dev wallet token account changed — could be selling.
        // SPL Token account data: offset 64 = amount (u64 LE, 8 bytes)
        if acc.data.len() >= 72 {
            let amount = u64::from_le_bytes(acc.data[64..72].try_into().unwrap_or([0u8; 8]));
            warn!(
                mint = %dev_mint,
                dev_token_balance = amount,
                slot = account_update.slot,
                "yellowstone_grpc mux: ⚠️ dev wallet token account changed"
            );
        }
        return;
    }

    // Map pubkey (bytes) back to mint by scanning active state.
    // With 6 max positions this is trivially fast.
    let mint = active
        .iter()
        .find(|(_, st)| st.pda.as_ref() == pubkey_bytes)
        .map(|(m, _)| m.clone());
    let Some(mint) = mint else {
        debug!("yellowstone_grpc mux: update for untracked pubkey (expected briefly after unsubscribe)");
        return;
    };

    let snap = match BondingCurveSnapshot::decode(&acc.data) {
        Some(s) => s,
        None => {
            warn!(mint = %mint, len = acc.data.len(), "yellowstone_grpc mux: bonding curve decode failed");
            return;
        }
    };
    if snap.complete {
        info!(mint = %mint, "yellowstone_grpc mux: bonding curve complete — handing off to Jupiter");
        log_event(
            supabase,
            "helius_ws_graduated",
            &format!("mint: {} | backend: yellowstone_grpc", mint),
        )
        .await;
        active.remove(&mint);
        cache.remove(&mint);
        return;
    }
    let sol_usd = cache.sol_usd();
    if sol_usd <= 0.0 {
        warn!(mint = %mint, "yellowstone_grpc mux: sol_usd not seeded — dropping tick");
        return;
    }
    let price_usd = snap.price_sol() * sol_usd;
    cache.set(mint.clone(), price_usd);
    info!(
        mint = %mint,
        price_usd,
        slot = account_update.slot,
        vsr = snap.virtual_sol_reserves,
        vtr = snap.virtual_token_reserves,
        "yellowstone_grpc mux: tick"
    );
}

async fn log_event(supabase: &SupabaseClient, event_type: &str, message: &str) {
    let payload = serde_json::json!({
        "event_type": event_type,
        "message": message,
    });
    let url = format!("{}/system_events", supabase.base_url);
    let _ = supabase.client.post(&url).json(&payload).send().await;
}

// ── Raydium graduation detection via transactions filter ────

/// Build the static `transactions` filter map. Always subscribes to
/// Raydium AMM V4 to detect pool creation (graduation) in real time.
fn build_tx_filters() -> HashMap<String, SubscribeRequestFilterTransactions> {
    let mut txs = HashMap::new();
    txs.insert(
        "raydium_graduation".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![RAYDIUM_AMM_V4.to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );
    txs
}

fn account_key_at(account_keys: &[Vec<u8>], ix_accounts: &[u8], ix_pos: usize) -> Option<Vec<u8>> {
    let account_idx = *ix_accounts.get(ix_pos)? as usize;
    account_keys.get(account_idx).cloned()
}

fn extract_raydium_initialize2_pool_and_mint(
    account_keys: &[Vec<u8>],
    ix_accounts: &[u8],
    ix_data: &[u8],
) -> Option<(String, String)> {
    if ix_data.first().copied() != Some(RAYDIUM_INITIALIZE2_TAG) {
        return None;
    }

    // Raydium AMM V4 initialize2 layout as observed in migration txs:
    //   [0] SPL Token Program
    //   [1] Associated Token Program
    //   [2] System Program
    //   [3] Rent Sysvar
    //   [4] amm_id (pool)
    //   [8] coin_mint
    //   [9] pc_mint (WSOL for pump.fun graduations; side order can vary)
    let pool = account_key_at(account_keys, ix_accounts, RAYDIUM_INIT2_POOL_ACCOUNT_POS)?;
    let coin_mint = account_key_at(account_keys, ix_accounts, RAYDIUM_INIT2_COIN_MINT_POS)?;
    let pc_mint = account_key_at(account_keys, ix_accounts, RAYDIUM_INIT2_PC_MINT_POS)?;
    let sol_mint = bs58_to_bytes(SOL_MINT);

    let mint = match (coin_mint == sol_mint, pc_mint == sol_mint) {
        (false, true) => coin_mint,
        (true, false) => pc_mint,
        _ => return None,
    };

    Some((
        bs58::encode(mint).into_string(),
        bs58::encode(pool).into_string(),
    ))
}

/// Handle a transaction update from the Raydium graduation filter.
///
/// Extracts the pool address and mint from Raydium AMM V4 `initialize2`
/// transactions, logs the graduation event, and (in future) can feed the
/// pool address to LP safety watchers.
async fn handle_transaction_update(
    tx_update: yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction,
    _cache: &HeliusPriceCache,
    supabase: &SupabaseClient,
    _active: &mut HashMap<String, MintState>,
    graduation_tx: &Option<mpsc::Sender<GraduatedToken>>,
    seen_graduation_mints: &mut HashSet<String>,
) {
    let Some(tx_info) = tx_update.transaction else {
        return;
    };
    let Some(tx) = &tx_info.transaction else {
        return;
    };
    let Some(msg) = &tx.message else { return };
    let Some(meta) = &tx_info.meta else { return };

    // Skip failed transactions.
    if meta.err.is_some() {
        return;
    }

    // Build the full account keys list (static + loaded).
    let mut account_keys: Vec<Vec<u8>> = msg.account_keys.clone();
    account_keys.extend(meta.loaded_writable_addresses.iter().cloned());
    account_keys.extend(meta.loaded_readonly_addresses.iter().cloned());

    let raydium_bytes = bs58_to_bytes(RAYDIUM_AMM_V4);

    // Track pools we've already logged in this tx to prevent duplicates
    // (same IX can appear in both top-level and inner instructions).
    let mut seen_pools: Vec<String> = Vec::new();

    for ix in &msg.instructions {
        let prog_idx = ix.program_id_index as usize;
        if prog_idx >= account_keys.len() {
            continue;
        }
        if account_keys[prog_idx] != raydium_bytes {
            continue;
        }
        let Some((mint_str, pool_str)) =
            extract_raydium_initialize2_pool_and_mint(&account_keys, &ix.accounts, &ix.data)
        else {
            continue;
        };

        // Dedup: skip if we already logged this pool from a top-level IX.
        if seen_pools.contains(&pool_str) {
            continue;
        }
        seen_pools.push(pool_str.clone());

        // Cross-tx dedup: skip if we already processed this mint in this session.
        if !seen_graduation_mints.insert(mint_str.clone()) {
            continue;
        }

        let sig_str = if !tx.signatures.is_empty() {
            bs58::encode(&tx.signatures[0]).into_string()
        } else {
            "unknown".to_string()
        };

        info!(
            mint = %mint_str,
            pool = %pool_str,
            slot = tx_update.slot,
            sig = %sig_str,
            "yellowstone_grpc mux: 🎓 Raydium pool created (graduation detected)"
        );

        // Feed graduation into detection pipeline as a backup source.
        send_graduation_event(graduation_tx, &mint_str, &pool_str).await;

        // Don't break — one tx could theoretically create multiple pools.
    }

    // Also scan inner instructions for CPI-invoked pool creation.
    for inner_ix_group in &meta.inner_instructions {
        for inner_ix in &inner_ix_group.instructions {
            let prog_idx = inner_ix.program_id_index as usize;
            if prog_idx >= account_keys.len() {
                continue;
            }
            if account_keys[prog_idx] != raydium_bytes {
                continue;
            }
            let Some((mint_str, pool_str)) = extract_raydium_initialize2_pool_and_mint(
                &account_keys,
                &inner_ix.accounts,
                &inner_ix.data,
            ) else {
                continue;
            };

            // Dedup: skip if already logged from top-level instructions.
            if seen_pools.contains(&pool_str) {
                continue;
            }
            seen_pools.push(pool_str.clone());

            // Cross-tx dedup: skip if we already processed this mint in this session.
            if !seen_graduation_mints.insert(mint_str.clone()) {
                continue;
            }

            let sig_str = if !tx.signatures.is_empty() {
                bs58::encode(&tx.signatures[0]).into_string()
            } else {
                "unknown".to_string()
            };

            info!(
                mint = %mint_str,
                pool = %pool_str,
                slot = tx_update.slot,
                sig = %sig_str,
                "yellowstone_grpc mux: 🎓 Raydium pool created via CPI (graduation detected)"
            );

            send_graduation_event(graduation_tx, &mint_str, &pool_str).await;
        }
    }
}

/// Send a graduation event into the detection pipeline.
async fn send_graduation_event(
    graduation_tx: &Option<mpsc::Sender<GraduatedToken>>,
    mint: &str,
    pool: &str,
) {
    let Some(tx) = graduation_tx else { return };
    let mint_pk = match Pubkey::from_str(mint) {
        Ok(pk) => pk,
        Err(e) => {
            warn!(mint = %mint, "graduation event: invalid mint address: {}", e);
            return;
        }
    };
    let pool_pk = Pubkey::from_str(pool).ok();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let token = GraduatedToken {
        mint: mint_pk,
        pool_address: pool_pk,
        detected_at: now_ms,
        source: DetectionSource::Geyser,
        creator_wallet: Pubkey::default(),
        bonding_curve_volume_sol: 0.0,
        buy_pressure_pct: 0.0,
        time_to_graduate_seconds: 0.0,
        unique_buyer_count: 0,
        buy_count: 0,
        sell_count: 0,
        trade_timestamps: vec![],
        name: String::new(),
        symbol: String::new(),
        initial_liquidity_sol: 0.0,
        creator_rebuy: false,
        buy_sell_ratio: 0.0,
        narrative_cluster: None,
        candidate_id: None,
        sniper_features: None,
        sniper_score: None,
        pipeline_timing: PipelineTiming::new(now_ms),
    };
    if tx.send(token).await.is_err() {
        warn!("yellowstone_grpc mux: graduation channel full/closed");
    }
}

fn bs58_to_bytes(s: &str) -> Vec<u8> {
    bs58::decode(s).into_vec().unwrap_or_default()
}

/// Inject basic-auth credentials into an https URL (`https://host` ->
/// `https://user:pass@host`). URL-encodes both components.
fn inject_basic_auth(endpoint: &str, user: &str, pass: &str) -> Result<String> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("endpoint missing scheme: {}", endpoint))?;
    let u = urlencode(user);
    let p = urlencode(pass);
    Ok(format!("{}://{}:{}@{}", scheme, u, p, rest))
}

fn urlencode(s: &str) -> String {
    // Minimal URL-encoder for username/password: percent-encode non-alnum
    // and non-safe chars. Adequate for Chainstack auto-generated creds.
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}
