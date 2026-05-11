//! Yellowstone gRPC source for pump.fun event stream.
//!
//! Replaces PumpPortal WebSocket with a direct on-chain feed. Subscribes to
//! pump.fun program transactions via Yellowstone gRPC, parses Anchor event
//! logs from `meta.log_messages`, decodes `CreateEvent` / `TradeEvent` /
//! `CompleteEvent`, synthesizes the same JSON shape PumpPortal sends, and
//! feeds it into [`crate::detection::pumpfun_ws::handle_text_message`].
//!
//! ## Why
//! PumpPortal's API-key WebSocket charges ~0.01 SOL per WS session — its
//! Lightning Wallet drains every ~10 minutes, costing >1 SOL/day to keep
//! `subscribeTokenTrade` alive. Yellowstone gRPC delivers the same events
//! direct from the validator with no per-event fees.
//!
//! ## Anchor event encoding
//! Every emitted event appears as a `Program data: <base64>` line in the
//! transaction's log messages. The decoded bytes start with an 8-byte
//! Anchor discriminator: `sha256("event:<EventName>")[0..8]`. The body is
//! borsh-encoded.
//!
//! ## Output schema parity
//! Synthesizes exactly the field names that
//! [`super::pumpfun_ws::handle_new_token`] / `handle_token_trade` /
//! `handle_token_complete` read, so 100% of the existing watchlist,
//! `bc_score` cache, and downstream filter pipeline is reused unchanged.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

use super::pumpfun_ws::{
    self, ProvenWalletInfo, RecentCreatorObservation, RecentLabelObservation,
    COMPLETE_DEDUP_MAX_ENTRIES, COMPLETE_DEDUP_WINDOW_MS,
};
use super::types::{BcScoreCache, GraduatedToken, WatchlistEntry};
use crate::config::AppConfig;
use crate::logger::SupabaseClient;

const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

const MAX_BACKOFF_SECS: u64 = 30;
const INITIAL_BACKOFF_SECS: u64 = 1;

// ── Anchor event discriminators = sha256("event:<Name>")[0..8] ──
// Verified against pump.fun mainnet transactions; do not change without
// re-running the discriminator unit tests at the bottom of this file.

const TRADE_EVENT_DISCRIM: [u8; 8] = [0xbd, 0xdb, 0x7f, 0xd3, 0x4e, 0xe6, 0x61, 0xee];
const CREATE_EVENT_DISCRIM: [u8; 8] = [0x1b, 0x72, 0xa9, 0x4d, 0xde, 0xeb, 0x63, 0x76];
const COMPLETE_EVENT_DISCRIM: [u8; 8] = [0x5f, 0x72, 0x61, 0x9c, 0xd4, 0x2e, 0x98, 0x08];

// ── Decoded event payloads ──

#[derive(Debug, Clone)]
struct TradeEvent {
    mint: [u8; 32],
    sol_amount: u64,
    token_amount: u64,
    is_buy: bool,
    user: [u8; 32],
    #[allow(dead_code)]
    timestamp: i64,
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    #[allow(dead_code)]
    real_sol_reserves: u64,
    #[allow(dead_code)]
    real_token_reserves: u64,
}

#[derive(Debug, Clone)]
struct CreateEvent {
    name: String,
    symbol: String,
    #[allow(dead_code)]
    uri: String,
    mint: [u8; 32],
    #[allow(dead_code)]
    bonding_curve: [u8; 32],
    user: [u8; 32],
}

#[derive(Debug, Clone)]
struct CompleteEvent {
    #[allow(dead_code)]
    user: [u8; 32],
    mint: [u8; 32],
    #[allow(dead_code)]
    bonding_curve: [u8; 32],
    #[allow(dead_code)]
    timestamp: i64,
}

enum PumpEvent {
    Create(CreateEvent),
    Trade(TradeEvent),
    Complete(CompleteEvent),
}

// ── Borsh decoders (hand-rolled for zero-allocation hot path) ──

fn read_pubkey(data: &[u8], o: &mut usize) -> Option<[u8; 32]> {
    if *o + 32 > data.len() {
        return None;
    }
    let arr: [u8; 32] = data[*o..*o + 32].try_into().ok()?;
    *o += 32;
    Some(arr)
}

fn read_u64(data: &[u8], o: &mut usize) -> Option<u64> {
    if *o + 8 > data.len() {
        return None;
    }
    let v = u64::from_le_bytes(data[*o..*o + 8].try_into().ok()?);
    *o += 8;
    Some(v)
}

fn read_i64(data: &[u8], o: &mut usize) -> Option<i64> {
    if *o + 8 > data.len() {
        return None;
    }
    let v = i64::from_le_bytes(data[*o..*o + 8].try_into().ok()?);
    *o += 8;
    Some(v)
}

fn read_bool(data: &[u8], o: &mut usize) -> Option<bool> {
    if *o + 1 > data.len() {
        return None;
    }
    let v = data[*o] != 0;
    *o += 1;
    Some(v)
}

fn read_string(data: &[u8], o: &mut usize) -> Option<String> {
    if *o + 4 > data.len() {
        return None;
    }
    let len = u32::from_le_bytes(data[*o..*o + 4].try_into().ok()?) as usize;
    *o += 4;
    if *o + len > data.len() {
        return None;
    }
    let s = String::from_utf8(data[*o..*o + len].to_vec()).ok()?;
    *o += len;
    Some(s)
}

fn decode_trade_event(data: &[u8]) -> Option<TradeEvent> {
    let mut o = 0usize;
    let mint = read_pubkey(data, &mut o)?;
    let sol_amount = read_u64(data, &mut o)?;
    let token_amount = read_u64(data, &mut o)?;
    let is_buy = read_bool(data, &mut o)?;
    let user = read_pubkey(data, &mut o)?;
    let timestamp = read_i64(data, &mut o)?;
    let virtual_sol_reserves = read_u64(data, &mut o)?;
    let virtual_token_reserves = read_u64(data, &mut o)?;
    let real_sol_reserves = read_u64(data, &mut o)?;
    let real_token_reserves = read_u64(data, &mut o)?;
    Some(TradeEvent {
        mint,
        sol_amount,
        token_amount,
        is_buy,
        user,
        timestamp,
        virtual_sol_reserves,
        virtual_token_reserves,
        real_sol_reserves,
        real_token_reserves,
    })
}

fn decode_create_event(data: &[u8]) -> Option<CreateEvent> {
    let mut o = 0usize;
    let name = read_string(data, &mut o)?;
    let symbol = read_string(data, &mut o)?;
    let uri = read_string(data, &mut o)?;
    let mint = read_pubkey(data, &mut o)?;
    let bonding_curve = read_pubkey(data, &mut o)?;
    let user = read_pubkey(data, &mut o)?;
    Some(CreateEvent {
        name,
        symbol,
        uri,
        mint,
        bonding_curve,
        user,
    })
}

fn decode_complete_event(data: &[u8]) -> Option<CompleteEvent> {
    let mut o = 0usize;
    let user = read_pubkey(data, &mut o)?;
    let mint = read_pubkey(data, &mut o)?;
    let bonding_curve = read_pubkey(data, &mut o)?;
    let timestamp = read_i64(data, &mut o)?;
    Some(CompleteEvent {
        user,
        mint,
        bonding_curve,
        timestamp,
    })
}

/// Scan transaction log messages for `Program data: <base64>` lines and
/// decode any pump.fun events found.
fn extract_events(log_messages: &[String]) -> Vec<PumpEvent> {
    let mut out = Vec::new();
    for line in log_messages {
        let Some(b64) = line.strip_prefix("Program data: ") else {
            continue;
        };
        let bytes = match base64::engine::general_purpose::STANDARD.decode(b64.trim()) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bytes.len() < 8 {
            continue;
        }
        let disc: [u8; 8] = match bytes[0..8].try_into() {
            Ok(d) => d,
            Err(_) => continue,
        };
        let body = &bytes[8..];
        if disc == TRADE_EVENT_DISCRIM {
            if let Some(e) = decode_trade_event(body) {
                out.push(PumpEvent::Trade(e));
            }
        } else if disc == CREATE_EVENT_DISCRIM {
            if let Some(e) = decode_create_event(body) {
                out.push(PumpEvent::Create(e));
            }
        } else if disc == COMPLETE_EVENT_DISCRIM {
            if let Some(e) = decode_complete_event(body) {
                out.push(PumpEvent::Complete(e));
            }
        }
    }
    out
}

// ── PumpPortal-shape JSON synthesis ──

/// pump.fun launches a fixed 1B token supply with 6 decimals.
const PUMPFUN_TOTAL_SUPPLY_TOKENS: f64 = 1_000_000_000.0;
const TOKEN_DECIMALS: f64 = 1_000_000.0; // 6 decimals

fn create_to_json(e: &CreateEvent) -> String {
    serde_json::json!({
        "txType": "create",
        "mint": bs58::encode(&e.mint).into_string(),
        "name": e.name,
        "symbol": e.symbol,
        "traderPublicKey": bs58::encode(&e.user).into_string(),
    })
    .to_string()
}

fn trade_to_json(e: &TradeEvent) -> String {
    let v_sol_sol = e.virtual_sol_reserves as f64 / 1e9;
    let v_tokens_tokens = e.virtual_token_reserves as f64 / TOKEN_DECIMALS;
    let market_cap_sol = if v_tokens_tokens > 0.0 {
        PUMPFUN_TOTAL_SUPPLY_TOKENS * v_sol_sol / v_tokens_tokens
    } else {
        0.0
    };
    serde_json::json!({
        "txType": if e.is_buy { "buy" } else { "sell" },
        "mint": bs58::encode(&e.mint).into_string(),
        "solAmount": e.sol_amount as f64 / 1e9,
        "tokenAmount": e.token_amount as f64 / TOKEN_DECIMALS,
        "traderPublicKey": bs58::encode(&e.user).into_string(),
        "vSolInBondingCurve": v_sol_sol,
        "vTokensInBondingCurve": v_tokens_tokens,
        "marketCapSol": market_cap_sol,
    })
    .to_string()
}

fn complete_to_json(e: &CompleteEvent, sig: &str) -> String {
    // Pool address is intentionally omitted — pump.fun's CompleteEvent fires
    // when the bonding curve fills, but the Raydium pool is created in a
    // separate transaction. `handle_token_complete` has fallback logic that
    // resolves the pool from the migration tx signature, then DexScreener.
    serde_json::json!({
        "txType": "migrate",
        "mint": bs58::encode(&e.mint).into_string(),
        "signature": sig,
    })
    .to_string()
}

// ── gRPC connection ──

/// Configuration for the pump.fun Yellowstone gRPC stream.
#[derive(Debug, Clone)]
pub struct YellowstonePumpfunConfig {
    /// gRPC endpoint, e.g. `https://yellowstone-solana-mainnet.core.chainstack.com`.
    pub endpoint: String,
    /// Primary auth: x-token header.
    pub x_token: Option<String>,
    /// Fallback auth: HTTP Basic. Used only if x_token is None.
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Run the gRPC source loop with reconnect-on-error.
pub async fn run(
    config: YellowstonePumpfunConfig,
    tx: mpsc::Sender<GraduatedToken>,
    supabase: Arc<SupabaseClient>,
    rpc_url: String,
    cfg: Arc<AppConfig>,
    bc_cache: BcScoreCache,
) {
    let mut backoff_secs = INITIAL_BACKOFF_SECS;
    loop {
        info!("Connecting to Yellowstone gRPC for pump.fun event stream …");
        match connect_and_listen(&config, &tx, &supabase, &rpc_url, &cfg, &bc_cache).await {
            Ok(()) => {
                warn!("Yellowstone pump.fun stream closed cleanly — reconnecting");
                backoff_secs = INITIAL_BACKOFF_SECS;
            }
            Err(e) => {
                error!(
                    "Yellowstone pump.fun stream error: {:#}. Reconnecting in {}s …",
                    e, backoff_secs
                );
            }
        }
        sleep(Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
    }
}

async fn connect_and_listen(
    config: &YellowstonePumpfunConfig,
    tx: &mpsc::Sender<GraduatedToken>,
    supabase: &Arc<SupabaseClient>,
    rpc_url: &str,
    cfg: &Arc<AppConfig>,
    bc_cache: &BcScoreCache,
) -> Result<()> {
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
        .context("yellowstone_pumpfun: build endpoint")?
        .x_token(config.x_token.clone())
        .context("yellowstone_pumpfun: attach x-token")?
        .tls_config(tonic::transport::ClientTlsConfig::new())
        .context("yellowstone_pumpfun: tls config")?
        .connect()
        .await
        .context("yellowstone_pumpfun: connect")?;

    info!("Yellowstone pump.fun stream connected");

    let (mut sink, mut stream) = client
        .subscribe()
        .await
        .context("yellowstone_pumpfun: open subscribe stream")?;

    sink.send(build_subscribe_request())
        .await
        .map_err(|e| anyhow::anyhow!("send subscribe: {}", e))?;

    info!(
        program = PUMPFUN_PROGRAM,
        "Yellowstone pump.fun: subscribed to program transactions"
    );

    // ── Drain channel used by handle_new_token's per-token subscribe path.
    // We pass `token_trade_subs_enabled = false` so this channel never
    // receives anything; it exists only to satisfy the function signature.
    let (ws_write_tx, mut ws_write_rx) = mpsc::channel::<Message>(16);
    tokio::spawn(async move {
        while let Some(_) = ws_write_rx.recv().await {
            // Discarded; gRPC source receives every trade by default.
        }
    });

    // State owned by this task — same shape as pumpfun_ws::connect_and_listen.
    let mut watchlist: HashMap<String, WatchlistEntry> = HashMap::new();
    let mut recent_labels: HashMap<String, Vec<RecentLabelObservation>> = HashMap::new();
    let mut recent_creators: HashMap<Pubkey, Vec<RecentCreatorObservation>> = HashMap::new();
    let mut emitted_complete: HashMap<String, i64> = HashMap::new();
    let proven_wallet_roster: HashMap<Pubkey, ProvenWalletInfo> =
        pumpfun_ws::fetch_proven_wallet_roster(supabase).await;

    let mut events_processed: u64 = 0;
    let mut last_log = std::time::Instant::now();

    while let Some(msg) = stream.next().await {
        let update = match msg {
            Ok(u) => u,
            Err(e) => return Err(anyhow::anyhow!("stream error: {}", e)),
        };

        let oneof = match update.update_oneof {
            Some(o) => o,
            None => continue,
        };

        let tx_update = match oneof {
            UpdateOneof::Transaction(t) => t,
            UpdateOneof::Ping(_) => {
                debug!("yellowstone_pumpfun: ping");
                continue;
            }
            UpdateOneof::Pong(_) => continue,
            _ => continue,
        };

        let Some(tx_info) = tx_update.transaction else {
            continue;
        };
        let Some(meta) = &tx_info.meta else {
            continue;
        };
        if meta.err.is_some() {
            continue;
        }

        let sig = if let Some(t) = tx_info.transaction.as_ref() {
            if !t.signatures.is_empty() {
                bs58::encode(&t.signatures[0]).into_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let events = extract_events(&meta.log_messages);
        for evt in events {
            let json_text = match &evt {
                PumpEvent::Create(e) => create_to_json(e),
                PumpEvent::Trade(e) => trade_to_json(e),
                PumpEvent::Complete(e) => complete_to_json(e, &sig),
            };
            events_processed += 1;
            if let Err(e) = pumpfun_ws::handle_text_message(
                &json_text,
                tx,
                &mut watchlist,
                &mut recent_labels,
                &mut recent_creators,
                &mut emitted_complete,
                COMPLETE_DEDUP_WINDOW_MS,
                COMPLETE_DEDUP_MAX_ENTRIES,
                supabase,
                &ws_write_tx,
                false, // token_trade_subs_enabled — gRPC delivers all trades, no per-token subs needed
                rpc_url,
                cfg,
                bc_cache,
                &proven_wallet_roster,
            )
            .await
            {
                warn!(
                    "yellowstone_pumpfun: failed to process synthesized event: {:#}",
                    e
                );
            }
        }

        if last_log.elapsed().as_secs() >= 60 {
            info!(
                events_processed,
                watchlist_size = watchlist.len(),
                "yellowstone_pumpfun: 60s heartbeat"
            );
            last_log = std::time::Instant::now();
        }
    }

    Err(anyhow::anyhow!("stream ended"))
}

fn build_subscribe_request() -> SubscribeRequest {
    let mut txs: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    txs.insert(
        "pumpfun_program".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![PUMPFUN_PROGRAM.to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );
    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: txs,
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
    }
}

fn inject_basic_auth(endpoint: &str, user: &str, pass: &str) -> Result<String> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("endpoint missing scheme: {}", endpoint))?;
    let u = urlencode(user);
    let p = urlencode(pass);
    Ok(format!("{}://{}:{}@{}", scheme, u, p, rest))
}

fn urlencode(s: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the hardcoded discriminators match `sha256("event:<Name>")[0..8]`.
    /// If pump.fun ever renames an event this test will fail and we'll know
    /// to recompute.
    #[test]
    fn discriminators_match_sha256() {
        use sha2::{Digest, Sha256};
        let cases: &[(&str, [u8; 8])] = &[
            ("event:TradeEvent", TRADE_EVENT_DISCRIM),
            ("event:CreateEvent", CREATE_EVENT_DISCRIM),
            ("event:CompleteEvent", COMPLETE_EVENT_DISCRIM),
        ];
        for (name, expected) in cases {
            let mut h = Sha256::new();
            h.update(name.as_bytes());
            let digest = h.finalize();
            let mut got = [0u8; 8];
            got.copy_from_slice(&digest[..8]);
            assert_eq!(
                got, *expected,
                "discriminator mismatch for {}: expected {:02x?} got {:02x?}",
                name, expected, got
            );
        }
    }

    #[test]
    fn trade_event_decoder_roundtrip() {
        // Hand-crafted minimal valid TradeEvent body (121 bytes after disc).
        let mut body = Vec::with_capacity(121);
        body.extend_from_slice(&[1u8; 32]); // mint
        body.extend_from_slice(&100_000_000u64.to_le_bytes()); // 0.1 SOL
        body.extend_from_slice(&5_000_000u64.to_le_bytes()); // 5.0 tokens
        body.push(1u8); // is_buy=true
        body.extend_from_slice(&[2u8; 32]); // user
        body.extend_from_slice(&1_700_000_000i64.to_le_bytes()); // ts
        body.extend_from_slice(&30_000_000_000u64.to_le_bytes()); // 30 SOL vSol
        body.extend_from_slice(&1_073_000_000_000_000u64.to_le_bytes()); // vTokens
        body.extend_from_slice(&5_000_000_000u64.to_le_bytes()); // realSol
        body.extend_from_slice(&500_000_000_000_000u64.to_le_bytes()); // realTokens

        let e = decode_trade_event(&body).expect("decode");
        assert_eq!(e.sol_amount, 100_000_000);
        assert_eq!(e.token_amount, 5_000_000);
        assert!(e.is_buy);
        assert_eq!(e.virtual_sol_reserves, 30_000_000_000);
    }

    #[test]
    fn create_event_decoder_handles_strings() {
        let name = "Test Coin";
        let symbol = "TEST";
        let uri = "https://x.com/y";
        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u32).to_le_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&(symbol.len() as u32).to_le_bytes());
        body.extend_from_slice(symbol.as_bytes());
        body.extend_from_slice(&(uri.len() as u32).to_le_bytes());
        body.extend_from_slice(uri.as_bytes());
        body.extend_from_slice(&[7u8; 32]); // mint
        body.extend_from_slice(&[8u8; 32]); // bonding_curve
        body.extend_from_slice(&[9u8; 32]); // user

        let e = decode_create_event(&body).expect("decode");
        assert_eq!(e.name, name);
        assert_eq!(e.symbol, symbol);
        assert_eq!(e.uri, uri);
    }
}
