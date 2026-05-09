//! Helius WebSocket multiplexed price stream for pump.fun bonding curves.
//!
//! ── Architecture ──────────────────────────────────────────────────────────
//! A single long-lived task (`PriceStreamMux`) owns ONE Helius Enhanced WS
//! connection and multiplexes `accountSubscribe` requests for every open
//! position. This keeps us inside the Helius Developer plan's 3-connection
//! budget regardless of how many positions are active.
//!
//! Per-position monitor tasks talk to the mux through an `mpsc` control
//! channel via `PriceStreamHandle::{subscribe, unsubscribe}`.
//!
//! ── Why a single connection ──────────────────────────────────────────────
//! Original PR-C spawned one WS per mint. With 6 max positions plus the
//! pre-existing dev-wallet & LP-vault watchers, we'd routinely blow past
//! 3 concurrent connections — Helius would start rejecting the safety-
//! critical rug watchers. This file is the fix.
//!
//! ── Reconnect semantics ──────────────────────────────────────────────────
//! On any WS error the mux reconnects with exponential backoff and replays
//! every active subscription from its in-memory state map. Cache entries
//! older than `HELIUS_CACHE_MAX_AGE` (3s) are treated as stale by the
//! consumer side, so a brief reconnect pause silently degrades to Jupiter.
//!
//! ── Observability ────────────────────────────────────────────────────────
//! - Tracing logs on every state transition (subscribe / graduate / drop).
//! - Atomic hit/miss counters on `HeliusPriceCache` updated by the consumer.
//! - Periodic flush task writes a `helius_ws_metrics` row to Supabase
//!   `system_events` every 60s including hit_rate, active_subs, and
//!   reconnect counts.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::logger::SupabaseClient;

const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

const MAX_BACKOFF_SECS: u64 = 10;
const INITIAL_BACKOFF_SECS: u64 = 1;

/// Metrics flush cadence (seconds).
const METRICS_INTERVAL_SECS: u64 = 60;

// ── Shared price cache ────────────────────────────────────────

/// Shared in-memory cache of mint → (USD price, write timestamp), plus
/// atomic hit/miss counters used by the metrics flusher.
///
/// Cloned via `Arc` between the consumer (`PriceFetcher`) and the producer
/// (`PriceStreamMux`). The lock is only held for the few microseconds
/// needed to swap a `HashMap` slot.
pub struct HeliusPriceCache {
    prices: Mutex<HashMap<String, (f64, Instant)>>,
    /// SOL/USD rate as f64-bits, refreshed periodically by monitoring loop.
    /// 0.0 (default) disables cache writes — subscriber waits until set.
    sol_usd_bits: AtomicU64,
    /// Number of `get_fresh` calls that returned `Some` (Jupiter skipped).
    cache_hits: AtomicU64,
    /// Number of `get_fresh` calls that returned `None` (Jupiter fallback).
    cache_misses: AtomicU64,
    /// Number of times the mux had to reconnect.
    reconnects: AtomicU64,
}

impl HeliusPriceCache {
    pub fn new() -> Self {
        Self {
            prices: Mutex::new(HashMap::new()),
            sol_usd_bits: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
        }
    }

    /// Return cached USD price if it was written within `max_age`.
    /// Increments the hit/miss counter as a side effect.
    pub fn get_fresh(&self, mint: &str, max_age: Duration) -> Option<f64> {
        let result = {
            let guard = self.prices.lock().ok()?;
            guard.get(mint).copied()
        };
        match result {
            Some((price, ts)) if ts.elapsed() <= max_age && price > 0.0 => {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(price)
            }
            _ => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Return cached USD price regardless of age. Used by the monitoring
    /// loop to avoid Jupiter HTTP calls entirely — Yellowstone keeps the
    /// cache warm in real time.
    pub fn get(&self, mint: &str) -> Option<f64> {
        let guard = self.prices.lock().ok()?;
        guard
            .get(mint)
            .map(|(price, _)| *price)
            .filter(|p| *p > 0.0)
    }

    pub fn set(&self, mint: String, price_usd: f64) {
        if price_usd <= 0.0 {
            return;
        }
        if let Ok(mut guard) = self.prices.lock() {
            guard.insert(mint, (price_usd, Instant::now()));
        }
    }

    pub fn remove(&self, mint: &str) {
        if let Ok(mut guard) = self.prices.lock() {
            guard.remove(mint);
        }
    }

    pub fn set_sol_usd(&self, p: f64) {
        if p > 0.0 && p.is_finite() {
            self.sol_usd_bits.store(p.to_bits(), Ordering::Relaxed);
        }
    }

    pub fn sol_usd(&self) -> f64 {
        f64::from_bits(self.sol_usd_bits.load(Ordering::Relaxed))
    }

    pub fn active_count(&self) -> usize {
        self.prices.lock().map(|g| g.len()).unwrap_or(0)
    }

    /// Atomically read-and-reset the hit/miss/reconnect counters.
    pub fn snapshot_metrics(&self) -> (u64, u64, u64) {
        (
            self.cache_hits.swap(0, Ordering::Relaxed),
            self.cache_misses.swap(0, Ordering::Relaxed),
            self.reconnects.swap(0, Ordering::Relaxed),
        )
    }

    pub(crate) fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for HeliusPriceCache {
    fn default() -> Self {
        Self::new()
    }
}

// ── Bonding curve decoding ────────────────────────────────────

/// Mirrors the layout in `src/sniper/enrichment.rs::fetch_bonding_curve`.
///
/// Bytes 0-7   : 8-byte anchor discriminator (ignored)
/// Bytes 8-15  : virtual_token_reserves (u64, 6 decimals)
/// Bytes 16-23 : virtual_sol_reserves   (u64, 9 decimals)
/// Byte 48     : complete (bool — true once graduated to Raydium)
#[derive(Debug, Clone, Copy)]
pub struct BondingCurveSnapshot {
    pub virtual_token_reserves: f64,
    pub virtual_sol_reserves: f64,
    pub complete: bool,
}

impl BondingCurveSnapshot {
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 49 {
            return None;
        }
        let virtual_token_reserves =
            u64::from_le_bytes(data[8..16].try_into().ok()?) as f64 / 1_000_000.0;
        let virtual_sol_reserves =
            u64::from_le_bytes(data[16..24].try_into().ok()?) as f64 / 1_000_000_000.0;
        let complete = data[48] != 0;
        if virtual_token_reserves <= 0.0 {
            return None;
        }
        Some(Self {
            virtual_token_reserves,
            virtual_sol_reserves,
            complete,
        })
    }

    pub fn price_sol(&self) -> f64 {
        self.virtual_sol_reserves / self.virtual_token_reserves
    }
}

fn derive_bonding_curve_pda(mint: &str) -> Option<Pubkey> {
    let mint_pk = Pubkey::from_str(mint).ok()?;
    let program = Pubkey::from_str(PUMPFUN_PROGRAM).ok()?;
    let (pda, _) = Pubkey::find_program_address(&[b"bonding-curve", mint_pk.as_ref()], &program);
    Some(pda)
}

// ── Multiplexer control protocol ──────────────────────────────

#[derive(Debug)]
enum MuxCommand {
    Subscribe { mint: String },
    Unsubscribe { mint: String },
}

/// Cheap-to-clone handle handed to per-position monitor tasks.
#[derive(Clone)]
pub struct PriceStreamHandle {
    tx: mpsc::UnboundedSender<MuxCommand>,
}

impl PriceStreamHandle {
    pub fn subscribe(&self, mint: String) {
        if mint == SOL_MINT {
            return;
        }
        if self.tx.send(MuxCommand::Subscribe { mint }).is_err() {
            warn!("helius_price_ws: mux channel closed — cannot subscribe");
        }
    }

    pub fn unsubscribe(&self, mint: String) {
        let _ = self.tx.send(MuxCommand::Unsubscribe { mint });
    }
}

// ── Multiplexer task ──────────────────────────────────────────

/// Per-mint state inside the mux.
struct MintState {
    pda: Pubkey,
    /// Set after the WS confirms the subscription.
    sub_id: Option<u64>,
    /// JSON-RPC request id used at subscribe time, so we can route the
    /// confirmation back to this mint.
    pending_id: u64,
}

/// Spawn the multiplexer task plus the metrics flusher and return a handle.
///
/// `ws_url` should be the Helius Enhanced WS URL (with API key).
/// `cache` is the same `Arc` shared with `PriceFetcher`.
/// `supabase` is used by the metrics flusher only.
pub fn start_mux(
    ws_url: String,
    cache: Arc<HeliusPriceCache>,
    supabase: Arc<SupabaseClient>,
) -> PriceStreamHandle {
    let (tx, rx) = mpsc::unbounded_channel::<MuxCommand>();

    // Mux task — owns the WS, owns `state`, runs forever.
    {
        let cache = Arc::clone(&cache);
        let supabase = Arc::clone(&supabase);
        let ws_url = ws_url.clone();
        tokio::spawn(async move {
            run_mux(ws_url, cache, supabase, rx).await;
        });
    }

    // Metrics flush task.
    {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            flush_metrics_loop(cache, supabase).await;
        });
    }

    info!("📡 helius_price_ws mux started (single-connection, multiplexed)");
    PriceStreamHandle { tx }
}

async fn run_mux(
    ws_url: String,
    cache: Arc<HeliusPriceCache>,
    supabase: Arc<SupabaseClient>,
    mut rx: mpsc::UnboundedReceiver<MuxCommand>,
) {
    let mut state: HashMap<String, MintState> = HashMap::new();
    let mut sub_id_to_mint: HashMap<u64, String> = HashMap::new();
    let mut backoff = INITIAL_BACKOFF_SECS;
    let mut next_req_id: u64 = 100;

    loop {
        match run_session(
            &ws_url,
            &cache,
            &supabase,
            &mut state,
            &mut sub_id_to_mint,
            &mut next_req_id,
            &mut rx,
        )
        .await
        {
            Ok(()) => {
                info!("helius_price_ws mux: control channel closed, exiting");
                return;
            }
            Err(e) => {
                cache.record_reconnect();
                warn!(
                    error = %e,
                    active_subs = state.len(),
                    backoff_secs = backoff,
                    "helius_price_ws mux: session ended, will reconnect"
                );
                log_event(
                    &supabase,
                    "helius_ws_reconnect",
                    &format!("Reason: {} | active_subs: {}", e, state.len()),
                )
                .await;
                // Drop confirmed sub_ids — they're invalid on a new connection.
                for s in state.values_mut() {
                    s.sub_id = None;
                }
                sub_id_to_mint.clear();
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
    }
}

/// Single WS session lifetime. Returns `Ok(())` only when the control
/// channel is closed (graceful shutdown). Any other terminator returns
/// `Err` so the outer loop reconnects.
async fn run_session(
    ws_url: &str,
    cache: &HeliusPriceCache,
    supabase: &SupabaseClient,
    state: &mut HashMap<String, MintState>,
    sub_id_to_mint: &mut HashMap<u64, String>,
    next_req_id: &mut u64,
    rx: &mut mpsc::UnboundedReceiver<MuxCommand>,
) -> Result<()> {
    let (ws, _) = connect_async(ws_url)
        .await
        .context("helius_price_ws mux: connect")?;
    let (mut write, mut read) = ws.split();

    info!(
        existing_subs = state.len(),
        "helius_price_ws mux: WS connected, replaying subs"
    );

    // Replay every existing subscription on the new connection.
    let mints: Vec<String> = state.keys().cloned().collect();
    for mint in mints {
        if let Some(s) = state.get_mut(&mint) {
            *next_req_id += 1;
            s.pending_id = *next_req_id;
            s.sub_id = None;
            send_subscribe(&mut write, s.pending_id, &s.pda).await?;
        }
    }

    loop {
        tokio::select! {
            // Control commands from per-position monitor tasks.
            cmd = rx.recv() => {
                let cmd = match cmd {
                    Some(c) => c,
                    None => return Ok(()),
                };
                match cmd {
                    MuxCommand::Subscribe { mint } => {
                        if state.contains_key(&mint) {
                            debug!(mint = %mint, "helius_price_ws mux: already subscribed");
                            continue;
                        }
                        let pda = match derive_bonding_curve_pda(&mint) {
                            Some(p) => p,
                            None => {
                                warn!(mint = %mint, "helius_price_ws mux: invalid mint");
                                continue;
                            }
                        };
                        *next_req_id += 1;
                        let req_id = *next_req_id;
                        state.insert(mint.clone(), MintState { pda, sub_id: None, pending_id: req_id });
                        if let Err(e) = send_subscribe(&mut write, req_id, &pda).await {
                            return Err(e);
                        }
                        log_event(
                            supabase,
                            "helius_ws_subscribed",
                            &format!("mint: {} | bonding_curve: {} | active: {}", mint, pda, state.len()),
                        ).await;
                    }
                    MuxCommand::Unsubscribe { mint } => {
                        if let Some(s) = state.remove(&mint) {
                            if let Some(sub_id) = s.sub_id {
                                sub_id_to_mint.remove(&sub_id);
                                *next_req_id += 1;
                                let _ = send_unsubscribe(&mut write, *next_req_id, sub_id).await;
                            }
                            cache.remove(&mint);
                            debug!(mint = %mint, active = state.len(), "helius_price_ws mux: unsubscribed");
                        }
                    }
                }
            }
            // Notifications from Helius.
            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(anyhow::anyhow!("ws read: {}", e)),
                    None => return Err(anyhow::anyhow!("ws stream closed")),
                };
                let text = match msg {
                    Message::Text(t) => t,
                    Message::Ping(p) => {
                        let _ = write.send(Message::Pong(p)).await;
                        continue;
                    }
                    Message::Close(_) => return Err(anyhow::anyhow!("ws close frame")),
                    _ => continue,
                };
                handle_ws_message(&text, cache, supabase, state, sub_id_to_mint).await;
            }
        }
    }
}

async fn send_subscribe<W>(write: &mut W, req_id: u64, pda: &Pubkey) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    <W as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "accountSubscribe",
        "params": [
            pda.to_string(),
            { "encoding": "base64", "commitment": "processed" }
        ]
    });
    info!(req_id, pda = %pda, "helius_price_ws mux: sending accountSubscribe");
    write
        .send(Message::Text(payload.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("send subscribe: {}", e))?;
    Ok(())
}

async fn send_unsubscribe<W>(write: &mut W, req_id: u64, sub_id: u64) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    <W as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "accountUnsubscribe",
        "params": [sub_id]
    });
    write
        .send(Message::Text(payload.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("send unsubscribe: {}", e))?;
    Ok(())
}

async fn handle_ws_message(
    text: &str,
    cache: &HeliusPriceCache,
    supabase: &SupabaseClient,
    state: &mut HashMap<String, MintState>,
    sub_id_to_mint: &mut HashMap<u64, String>,
) {
    // PR-E DIAG: log every raw frame at debug so we can see what Helius actually sends.
    let preview: String = text.chars().take(300).collect();
    debug!(len = text.len(), preview = %preview, "helius_price_ws mux: raw frame");

    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, preview = %preview, "helius_price_ws mux: json parse failed");
            return;
        }
    };

    // Surface JSON-RPC error responses (e.g. -32602 invalid params, -32601 method not found).
    if let Some(err) = v.get("error") {
        let id = v.get("id").and_then(|i| i.as_u64()).unwrap_or(0);
        warn!(req_id = id, error = %err, "helius_price_ws mux: JSON-RPC error from Helius");
        return;
    }

    // Subscription confirmation: {"jsonrpc":"2.0","id":<req_id>,"result":<sub_id>}
    if let (Some(req_id), Some(result)) = (
        v.get("id").and_then(|i| i.as_u64()),
        v.get("result").and_then(|r| r.as_u64()),
    ) {
        let mint_owner = state
            .iter_mut()
            .find(|(_, s)| s.pending_id == req_id)
            .map(|(m, s)| {
                s.sub_id = Some(result);
                m.clone()
            });
        if let Some(mint) = mint_owner {
            sub_id_to_mint.insert(result, mint.clone());
            info!(mint = %mint, sub_id = result, req_id, "helius_price_ws mux: sub confirmed");
        } else {
            warn!(
                req_id,
                sub_id = result,
                "helius_price_ws mux: orphan sub confirmation (no pending state match)"
            );
        }
        return;
    }

    let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("");
    if method != "accountNotification" {
        if !method.is_empty() {
            debug!(method = %method, preview = %preview, "helius_price_ws mux: unhandled method");
        } else {
            debug!(preview = %preview, "helius_price_ws mux: frame has no method/result");
        }
        return;
    }

    info!(preview = %preview, "helius_price_ws mux: accountNotification received");

    let sub_id = match v.pointer("/params/subscription").and_then(|s| s.as_u64()) {
        Some(s) => s,
        None => {
            warn!(preview = %preview, "helius_price_ws mux: notification missing subscription id");
            return;
        }
    };
    let mint = match sub_id_to_mint.get(&sub_id) {
        Some(m) => m.clone(),
        None => {
            warn!(
                sub_id,
                known_subs = sub_id_to_mint.len(),
                "helius_price_ws mux: unknown sub_id"
            );
            return;
        }
    };

    let data_field = match v
        .pointer("/params/result/value/data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|s| s.as_str())
    {
        Some(d) => d,
        None => {
            warn!(mint = %mint, preview = %preview, "helius_price_ws mux: notification missing data[0] string");
            return;
        }
    };
    let bytes = match base64::engine::general_purpose::STANDARD.decode(data_field) {
        Ok(b) => b,
        Err(e) => {
            warn!(mint = %mint, error = %e, "helius_price_ws mux: base64 decode failed");
            return;
        }
    };
    let snap = match BondingCurveSnapshot::decode(&bytes) {
        Some(s) => s,
        None => {
            warn!(mint = %mint, len = bytes.len(), "helius_price_ws mux: bonding curve decode failed");
            return;
        }
    };
    if snap.complete {
        info!(mint = %mint, "helius_price_ws mux: bonding curve complete — handing off to Jupiter");
        log_event(supabase, "helius_ws_graduated", &format!("mint: {}", mint)).await;
        state.remove(&mint);
        sub_id_to_mint.remove(&sub_id);
        cache.remove(&mint);
        return;
    }
    let sol_usd = cache.sol_usd();
    if sol_usd <= 0.0 {
        warn!(mint = %mint, "helius_price_ws mux: sol_usd not seeded yet — dropping tick");
        return;
    }
    let price_usd = snap.price_sol() * sol_usd;
    cache.set(mint.clone(), price_usd);
    info!(
        mint = %mint,
        price_usd,
        vsr = snap.virtual_sol_reserves,
        vtr = snap.virtual_token_reserves,
        "helius_price_ws mux: tick"
    );
}

// ── Metrics flusher ───────────────────────────────────────────

pub async fn flush_metrics_loop(cache: Arc<HeliusPriceCache>, supabase: Arc<SupabaseClient>) {
    let mut tick = tokio::time::interval(Duration::from_secs(METRICS_INTERVAL_SECS));
    tick.tick().await;
    loop {
        tick.tick().await;
        let (hits, misses, reconnects) = cache.snapshot_metrics();
        let total = hits + misses;
        if total == 0 && reconnects == 0 {
            continue; // no traffic this window — don't spam Supabase
        }
        let hit_rate = if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        let active = cache.active_count();
        let payload = serde_json::json!({
            "event_type": "helius_ws_metrics",
            "message": format!(
                "hits: {} | misses: {} | hit_rate_pct: {:.1} | active_subs: {} | reconnects: {} | window_secs: {}",
                hits, misses, hit_rate, active, reconnects, METRICS_INTERVAL_SECS
            ),
        });
        let url = format!("{}/system_events", supabase.base_url);
        let _ = supabase.client.post(&url).json(&payload).send().await;
        info!(
            hits,
            misses, hit_rate, active, reconnects, "📊 helius_price_ws metrics flushed"
        );
    }
}

async fn log_event(supabase: &SupabaseClient, event_type: &str, message: &str) {
    let payload = serde_json::json!({
        "event_type": event_type,
        "message": message,
    });
    let url = format!("{}/system_events", supabase.base_url);
    let _ = supabase.client.post(&url).json(&payload).send().await;
}

// ── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn build_curve_bytes(vtr: u64, vsr: u64, complete: bool) -> Vec<u8> {
        let mut buf = vec![0u8; 49];
        buf[8..16].copy_from_slice(&vtr.to_le_bytes());
        buf[16..24].copy_from_slice(&vsr.to_le_bytes());
        buf[48] = if complete { 1 } else { 0 };
        buf
    }

    #[test]
    fn decode_round_trips_reserves() {
        let bytes = build_curve_bytes(800_000_000_000_000, 30_000_000_000, false);
        let s = BondingCurveSnapshot::decode(&bytes).expect("decode");
        assert!((s.virtual_token_reserves - 800_000_000.0).abs() < 1.0);
        assert!((s.virtual_sol_reserves - 30.0).abs() < 1e-9);
        assert!(!s.complete);
        let expected = 30.0 / 800_000_000.0;
        assert!((s.price_sol() - expected).abs() / expected < 1e-9);
    }

    #[test]
    fn decode_marks_complete() {
        let bytes = build_curve_bytes(1, 1, true);
        let s = BondingCurveSnapshot::decode(&bytes).expect("decode");
        assert!(s.complete);
    }

    #[test]
    fn decode_rejects_short_data() {
        assert!(BondingCurveSnapshot::decode(&[0u8; 10]).is_none());
    }

    #[test]
    fn cache_fresh_window() {
        let c = HeliusPriceCache::new();
        c.set("mint1".into(), 0.0001);
        assert_eq!(c.get_fresh("mint1", Duration::from_secs(1)), Some(0.0001));
        assert_eq!(c.get_fresh("mint2", Duration::from_secs(1)), None);
    }

    #[test]
    fn cache_rejects_zero() {
        let c = HeliusPriceCache::new();
        c.set("mint1".into(), 0.0);
        assert!(c.get_fresh("mint1", Duration::from_secs(1)).is_none());
    }

    #[test]
    fn sol_usd_round_trip() {
        let c = HeliusPriceCache::new();
        assert_eq!(c.sol_usd(), 0.0);
        c.set_sol_usd(180.42);
        assert!((c.sol_usd() - 180.42).abs() < 1e-9);
    }

    #[test]
    fn pda_derivation_deterministic() {
        let mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
        let pda1 = derive_bonding_curve_pda(mint).unwrap();
        let pda2 = derive_bonding_curve_pda(mint).unwrap();
        assert_eq!(pda1, pda2);
    }

    #[test]
    fn metrics_counters_track_hit_miss() {
        let c = HeliusPriceCache::new();
        c.set("m".into(), 0.001);
        let _ = c.get_fresh("m", Duration::from_secs(1)); // hit
        let _ = c.get_fresh("nope", Duration::from_secs(1)); // miss
        let _ = c.get_fresh("nope", Duration::from_secs(1)); // miss
        let (h, m, _) = c.snapshot_metrics();
        assert_eq!(h, 1);
        assert_eq!(m, 2);
        // After snapshot, counters reset.
        let (h2, m2, _) = c.snapshot_metrics();
        assert_eq!(h2, 0);
        assert_eq!(m2, 0);
    }

    #[test]
    fn handle_subscribe_skips_sol_mint() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let h = PriceStreamHandle { tx };
        h.subscribe(SOL_MINT.into());
        assert!(rx.try_recv().is_err());
        h.subscribe("RealMint".into());
        match rx.try_recv() {
            Ok(MuxCommand::Subscribe { mint }) => assert_eq!(mint, "RealMint"),
            other => panic!("expected Subscribe, got {:?}", other),
        }
    }
}
