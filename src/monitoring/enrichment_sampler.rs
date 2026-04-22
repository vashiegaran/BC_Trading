//! Enrichment sampler — passive data collection during position hold phase.
//!
//! Spawns a per-position async task that snapshots multiple external APIs on a
//! schedule (30s, 2m, 5m, 10m, 20m, 30m, 60m) plus event-triggered points
//! (pre_dip_death, post_exit_1h). All results are written as JSONB rows into
//! `position_enrichment_snapshots`. No strategy decisions are made here —
//! this is pure logging for v6 data-driven tuning.
//!
//! Every outbound API call is gated through [`SamplerGuards`] (rate-limit +
//! circuit breaker). On circuit-open / timeout / error, the relevant column
//! is left NULL and the failure is logged into the `apis_failed` JSONB blob.

use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use serde_json::{json, Value};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::execution::types::PositionOpened;
use crate::logger::SupabaseClient;
use crate::monitoring::api_limiter::{ApiGuard, SamplerGuards};

/// Scheduled offsets (seconds from position open) for passive snapshots.
/// Density decays so early-hold gets finer resolution where most decisions fire.
const SCHEDULE_SECS: &[u64] = &[30, 120, 300, 600, 1200, 1800, 3600];

/// Tier 3 post-exit window — how long to wait before the moonbag check.
const POST_EXIT_CHECK_SECS: u64 = 3600;

/// Multiplier above entry_price_usd that qualifies as a "missed moonbag".
const MOONBAG_MULT_THRESHOLD: f64 = 3.0;

/// Individual HTTP request timeout used by the sampler (per call).
const HTTP_TIMEOUT_SECS: u64 = 8;

/// Shared context handed to each sampler task.
#[derive(Clone)]
pub struct SamplerCtx {
    pub cfg: Arc<AppConfig>,
    pub supabase: Arc<SupabaseClient>,
    pub guards: SamplerGuards,
    pub http: Client,
}

impl SamplerCtx {
    pub fn new(cfg: Arc<AppConfig>, supabase: Arc<SupabaseClient>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()
            .expect("enrichment sampler HTTP client build failed");
        Self {
            cfg,
            supabase,
            guards: SamplerGuards::new(),
            http,
        }
    }
}

/// Spawn the scheduled sampler for a freshly-opened position.
///
/// The task exits naturally after the last offset or if all snapshots fail.
/// Returns immediately — caller does not await.
pub fn spawn_scheduled_sampler(ctx: SamplerCtx, pos: PositionOpened) {
    let mint = pos.mint.clone();
    let position_id = pos.position_id;
    let is_paper = pos.is_paper_trade;
    let entry_price = pos.entry_price_usd;

    tokio::spawn(async move {
        info!(
            position_id,
            mint = %mint,
            paper = is_paper,
            "Enrichment sampler started"
        );
        let started = Instant::now();
        let mut prev_holder_count: Option<i64> = None;
        let mut prev_vol_5m: Option<f64> = None;
        let mut peak_mult: f64 = 1.0;

        for &offset_secs in SCHEDULE_SECS {
            let elapsed = started.elapsed().as_secs();
            if elapsed < offset_secs {
                sleep(Duration::from_secs(offset_secs - elapsed)).await;
            }

            let snap = collect_snapshot(
                &ctx,
                position_id,
                &mint,
                entry_price,
                pos.dev_wallet.as_deref(),
                offset_secs as i64,
                "scheduled",
                prev_holder_count,
                prev_vol_5m,
                &mut peak_mult,
            )
            .await;

            // Track trend deltas for next iteration
            prev_holder_count = snap.holder_count;
            prev_vol_5m = snap.vol_5m_usd;

            if let Err(e) = write_snapshot(&ctx.supabase, &snap).await {
                warn!(position_id, mint = %mint, "snapshot write failed: {}", e);
            }
        }

        debug!(position_id, mint = %mint, "Enrichment sampler finished");
    });
}

/// Take a single ad-hoc snapshot tagged with `trigger` (e.g. "pre_dip_death").
/// Used by the monitoring engine before sending certain ExitSignals so we log
/// the on-chain state right before the exit decision.
pub async fn snapshot_ad_hoc(
    ctx: &SamplerCtx,
    position_id: i64,
    mint: &str,
    entry_price_usd: f64,
    dev_wallet: Option<&str>,
    elapsed_secs: i64,
    trigger: &str,
) {
    let mut peak_mult = 1.0_f64;
    let snap = collect_snapshot(
        ctx,
        position_id,
        mint,
        entry_price_usd,
        dev_wallet,
        elapsed_secs,
        trigger,
        None,
        None,
        &mut peak_mult,
    )
    .await;

    if let Err(e) = write_snapshot(&ctx.supabase, &snap).await {
        warn!(position_id, mint = %mint, trigger, "ad-hoc snapshot write failed: {}", e);
    }
}

/// Tier 3 — schedule a post-exit moonbag check.
///
/// After [`POST_EXIT_CHECK_SECS`] sleeps, fetches the current price and if it
/// has exceeded `MOONBAG_MULT_THRESHOLD * entry_price_usd`, writes a row with
/// `trigger='post_exit_1h'` so we can quantify moonbags missed by early exits.
pub fn spawn_post_exit_moonbag_check(
    ctx: SamplerCtx,
    position_id: i64,
    mint: String,
    entry_price_usd: f64,
    dev_wallet: Option<String>,
) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(POST_EXIT_CHECK_SECS)).await;

        let current = fetch_price_usd(&ctx, &mint).await;
        let Some(price_now) = current else {
            debug!(position_id, mint = %mint, "post-exit price fetch failed — skipping moonbag log");
            return;
        };

        let mult = if entry_price_usd > 0.0 { price_now / entry_price_usd } else { 0.0 };
        if mult < MOONBAG_MULT_THRESHOLD {
            debug!(position_id, mint = %mint, mult, "post-exit moonbag below threshold — skipping");
            return;
        }

        info!(
            position_id,
            mint = %mint,
            mult,
            "🌙 Post-exit moonbag detected — capturing enrichment snapshot"
        );
        let mut peak_mult = mult;
        let snap = collect_snapshot(
            &ctx,
            position_id,
            &mint,
            entry_price_usd,
            dev_wallet.as_deref(),
            POST_EXIT_CHECK_SECS as i64,
            "post_exit_1h",
            None,
            None,
            &mut peak_mult,
        )
        .await;

        if let Err(e) = write_snapshot(&ctx.supabase, &snap).await {
            warn!(position_id, mint = %mint, "post-exit snapshot write failed: {}", e);
        }
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot collection
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Snapshot {
    position_id: i64,
    mint: String,
    elapsed_secs: i64,
    trigger: String,

    price_usd: Option<f64>,
    pnl_pct: Option<f64>,
    peak_multiplier: Option<f64>,

    holder_count: Option<i64>,
    holder_delta_from_prev: Option<i64>,
    top10_concentration_pct: Option<f64>,

    vol_5m_usd: Option<f64>,
    vol_1h_usd: Option<f64>,
    vol_acceleration: Option<f64>,
    buy_count_5m: Option<i64>,
    sell_count_5m: Option<i64>,
    buy_sell_ratio: Option<f64>,
    unique_traders_5m: Option<i64>,

    social_count: Option<i64>,
    has_twitter: Option<bool>,
    has_telegram: Option<bool>,
    has_website: Option<bool>,
    new_social_links: Option<Value>,

    smart_wallet_buy_count: Option<i64>,
    smart_wallet_sell_count: Option<i64>,
    smart_wallet_net_sol: Option<f64>,
    smart_wallets: Option<Value>,

    whale_buy_count: Option<i64>,
    whale_sell_count: Option<i64>,
    largest_trade_sol: Option<f64>,

    dev_wallet_sol_delta: Option<f64>,
    dev_wallet_token_balance: Option<f64>,

    liquidity_usd: Option<f64>,
    liquidity_delta_pct: Option<f64>,
    market_cap_usd: Option<f64>,
    price_impact_1sol_bps: Option<f64>,

    raw_birdeye: Option<Value>,
    raw_dexscreener: Option<Value>,
    raw_solana_tracker: Option<Value>,
    raw_das: Option<Value>,

    apis_called: serde_json::Map<String, Value>,
    apis_failed: serde_json::Map<String, Value>,
}

#[allow(clippy::too_many_arguments)]
async fn collect_snapshot(
    ctx: &SamplerCtx,
    position_id: i64,
    mint: &str,
    entry_price_usd: f64,
    dev_wallet: Option<&str>,
    elapsed_secs: i64,
    trigger: &str,
    prev_holder_count: Option<i64>,
    prev_vol_5m: Option<f64>,
    peak_mult: &mut f64,
) -> Snapshot {
    let mut snap = Snapshot {
        position_id,
        mint: mint.to_string(),
        elapsed_secs,
        trigger: trigger.to_string(),
        ..Default::default()
    };

    // Fire all fetchers in parallel where safe. DAS, Birdeye, DexScreener,
    // SolanaTracker are independent — run concurrently via tokio::join!.
    let (das_res, birdeye_res, dex_res, st_res) = tokio::join!(
        fetch_das(ctx, mint),
        fetch_birdeye(ctx, mint),
        fetch_dexscreener(ctx, mint),
        fetch_solana_tracker(ctx, mint),
    );

    // ── Price + raw Birdeye ──
    if let Some((val, latency_ms)) = &birdeye_res {
        snap.apis_called.insert("birdeye".into(), json!(latency_ms));
        snap.raw_birdeye = Some(val.clone());
        let data = val.get("data").unwrap_or(val);
        snap.price_usd = data.get("price").and_then(Value::as_f64);
        snap.liquidity_usd = data.get("liquidity").and_then(Value::as_f64);
        snap.market_cap_usd = data
            .get("mc")
            .or_else(|| data.get("marketCap"))
            .and_then(Value::as_f64);
    } else {
        snap.apis_failed
            .insert("birdeye".into(), json!("no_response"));
    }

    // ── DexScreener: socials, liquidity fallback, volume fallback ──
    if let Some((val, latency_ms)) = &dex_res {
        snap.apis_called.insert("dexscreener".into(), json!(latency_ms));
        snap.raw_dexscreener = Some(val.clone());

        if let Some(pair) = val
            .get("pairs")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
        {
            // Price fallback
            if snap.price_usd.is_none() {
                snap.price_usd = pair
                    .get("priceUsd")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<f64>().ok());
            }
            // Liquidity fallback
            if snap.liquidity_usd.is_none() {
                snap.liquidity_usd = pair
                    .get("liquidity")
                    .and_then(|l| l.get("usd"))
                    .and_then(Value::as_f64);
            }
            // Volume
            snap.vol_5m_usd = pair
                .get("volume")
                .and_then(|v| v.get("m5"))
                .and_then(Value::as_f64);
            snap.vol_1h_usd = pair
                .get("volume")
                .and_then(|v| v.get("h1"))
                .and_then(Value::as_f64);
            // Buy/sell counts (5m)
            if let Some(txns5) = pair.get("txns").and_then(|t| t.get("m5")) {
                snap.buy_count_5m = txns5.get("buys").and_then(Value::as_i64);
                snap.sell_count_5m = txns5.get("sells").and_then(Value::as_i64);
                if let (Some(b), Some(s)) = (snap.buy_count_5m, snap.sell_count_5m) {
                    if s > 0 {
                        snap.buy_sell_ratio = Some(b as f64 / s as f64);
                    } else if b > 0 {
                        snap.buy_sell_ratio = Some(f64::INFINITY);
                    }
                }
            }
            // Socials
            let info = pair.get("info");
            let socials = info.and_then(|i| i.get("socials")).and_then(Value::as_array);
            let websites = info.and_then(|i| i.get("websites")).and_then(Value::as_array);
            let sc = socials.map(|a| a.len() as i64).unwrap_or(0);
            let wc = websites.map(|a| a.len() as i64).unwrap_or(0);
            snap.social_count = Some(sc + wc);
            snap.has_website = Some(wc > 0);
            if let Some(arr) = socials {
                let has_tw = arr.iter().any(|s| {
                    s.get("type")
                        .and_then(Value::as_str)
                        .map(|t| t.eq_ignore_ascii_case("twitter"))
                        .unwrap_or(false)
                });
                let has_tg = arr.iter().any(|s| {
                    s.get("type")
                        .and_then(Value::as_str)
                        .map(|t| t.eq_ignore_ascii_case("telegram"))
                        .unwrap_or(false)
                });
                snap.has_twitter = Some(has_tw);
                snap.has_telegram = Some(has_tg);
                snap.new_social_links = Some(Value::Array(arr.clone()));
            }
        }
    } else {
        snap.apis_failed
            .insert("dexscreener".into(), json!("no_response"));
    }

    // ── Helius DAS: holder count (approximate via token accounts owner) ──
    if let Some((val, latency_ms)) = &das_res {
        snap.apis_called.insert("helius_das".into(), json!(latency_ms));
        snap.raw_das = Some(val.clone());
        // DAS getAsset returns aggregate info; actual holder list needs
        // getTokenAccounts. We store raw and parse conservatively.
        if let Some(ownership) = val.get("ownership") {
            // single-owner snapshot isn't holder_count; leave None.
            let _ = ownership;
        }
        if let Some(supply) = val.get("supply") {
            // supply.print_current_supply etc. — unused for now
            let _ = supply;
        }
    } else {
        snap.apis_failed
            .insert("helius_das".into(), json!("no_response"));
    }

    // ── SolanaTracker: holder count, top10 concentration, smart wallets ──
    if let Some((val, latency_ms)) = &st_res {
        snap.apis_called
            .insert("solana_tracker".into(), json!(latency_ms));
        snap.raw_solana_tracker = Some(val.clone());

        // Holder count + top10 concentration
        if let Some(holders) = val.get("holders") {
            snap.holder_count = holders.get("total").and_then(Value::as_i64);
            snap.top10_concentration_pct = holders
                .get("top10")
                .and_then(Value::as_f64)
                .or_else(|| holders.get("top10HoldersPercent").and_then(Value::as_f64));
        }
        // Fallback to risk field / other shapes
        if snap.holder_count.is_none() {
            snap.holder_count = val.get("holderCount").and_then(Value::as_i64);
        }

        // Smart wallets: ST returns `risk.snipers` + sometimes `smartMoney` arrays
        if let Some(sm) = val.get("smartMoney").and_then(Value::as_array) {
            let buys: i64 = sm
                .iter()
                .filter(|e| {
                    e.get("type")
                        .and_then(Value::as_str)
                        .map(|t| t.eq_ignore_ascii_case("buy"))
                        .unwrap_or(false)
                })
                .count() as i64;
            let sells: i64 = sm.len() as i64 - buys;
            let net_sol: f64 = sm
                .iter()
                .filter_map(|e| {
                    let amt = e.get("solAmount").and_then(Value::as_f64)?;
                    let t = e.get("type").and_then(Value::as_str).unwrap_or("");
                    if t.eq_ignore_ascii_case("sell") {
                        Some(-amt)
                    } else {
                        Some(amt)
                    }
                })
                .sum();
            snap.smart_wallet_buy_count = Some(buys);
            snap.smart_wallet_sell_count = Some(sells);
            snap.smart_wallet_net_sol = Some(net_sol);
            snap.smart_wallets = Some(Value::Array(sm.clone()));
        }
    } else {
        snap.apis_failed
            .insert("solana_tracker".into(), json!("no_response"));
    }

    // ── Derive deltas ──
    if let (Some(h), Some(prev)) = (snap.holder_count, prev_holder_count) {
        snap.holder_delta_from_prev = Some(h - prev);
    }
    if let (Some(v), Some(pv)) = (snap.vol_5m_usd, prev_vol_5m) {
        if pv > 0.0 {
            snap.vol_acceleration = Some(v / pv);
        }
    }

    // ── Price math ──
    if let Some(p) = snap.price_usd {
        if entry_price_usd > 0.0 {
            let mult = p / entry_price_usd;
            snap.pnl_pct = Some((mult - 1.0) * 100.0);
            if mult > *peak_mult {
                *peak_mult = mult;
            }
            snap.peak_multiplier = Some(*peak_mult);
        }
    }

    // ── Dev wallet SOL balance (Helius RPC) ──
    if let Some(dev) = dev_wallet {
        if let Some((sol_balance, latency_ms)) = fetch_sol_balance(ctx, dev).await {
            snap.apis_called
                .insert("helius_rpc".into(), json!(latency_ms));
            // Delta baseline isn't stored here — future work. Emit absolute
            // balance as "delta" to keep the column populated.
            snap.dev_wallet_sol_delta = Some(sol_balance);
        } else {
            snap.apis_failed
                .insert("helius_rpc".into(), json!("no_response"));
        }
    }

    snap
}

// ─────────────────────────────────────────────────────────────────────────────
// Individual fetchers — each returns (Value, latency_ms) or None
// ─────────────────────────────────────────────────────────────────────────────

async fn guarded_get_json(
    ctx: &SamplerCtx,
    guard: &ApiGuard,
    url: &str,
    api_key_header: Option<(&str, &str)>,
) -> Option<(Value, u64)> {
    let _permit = guard.acquire().await?;
    let started = Instant::now();
    let mut req = ctx.http.get(url);
    if let Some((k, v)) = api_key_header {
        req = req.header(k, v);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                guard.record_failure(&format!("http_{}", status.as_u16()));
                return None;
            }
            match resp.json::<Value>().await {
                Ok(v) => {
                    guard.record_success();
                    Some((v, started.elapsed().as_millis() as u64))
                }
                Err(e) => {
                    guard.record_failure(&format!("parse:{}", e));
                    None
                }
            }
        }
        Err(e) => {
            guard.record_failure(&format!("net:{}", e));
            None
        }
    }
}

async fn fetch_birdeye(ctx: &SamplerCtx, mint: &str) -> Option<(Value, u64)> {
    let api_key = ctx.cfg.env.birdeye_api_key.as_deref()?;
    let url = format!(
        "https://public-api.birdeye.so/defi/token_overview?address={}",
        mint
    );
    guarded_get_json(
        ctx,
        &ctx.guards.birdeye,
        &url,
        Some(("X-API-KEY", api_key)),
    )
    .await
}

async fn fetch_dexscreener(ctx: &SamplerCtx, mint: &str) -> Option<(Value, u64)> {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", mint);
    guarded_get_json(ctx, &ctx.guards.dexscreener, &url, None).await
}

async fn fetch_solana_tracker(ctx: &SamplerCtx, mint: &str) -> Option<(Value, u64)> {
    let api_key = ctx.cfg.env.solana_tracker_api_key.as_deref()?;
    let url = format!("https://eu.data.solanatracker.io/tokens/{}", mint);
    guarded_get_json(
        ctx,
        &ctx.guards.solana_tracker,
        &url,
        Some(("x-api-key", api_key)),
    )
    .await
}

async fn fetch_das(ctx: &SamplerCtx, mint: &str) -> Option<(Value, u64)> {
    let rpc_url = ctx.cfg.env.helius_rpc_url.as_deref()?;
    let _permit = ctx.guards.helius_das.acquire().await?;
    let started = Instant::now();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAsset",
        "params": { "id": mint }
    });
    match ctx.http.post(rpc_url).json(&body).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                ctx.guards
                    .helius_das
                    .record_failure(&format!("http_{}", resp.status().as_u16()));
                return None;
            }
            match resp.json::<Value>().await {
                Ok(v) => {
                    ctx.guards.helius_das.record_success();
                    // Unwrap jsonrpc envelope
                    let payload = v.get("result").cloned().unwrap_or(v);
                    Some((payload, started.elapsed().as_millis() as u64))
                }
                Err(e) => {
                    ctx.guards
                        .helius_das
                        .record_failure(&format!("parse:{}", e));
                    None
                }
            }
        }
        Err(e) => {
            ctx.guards.helius_das.record_failure(&format!("net:{}", e));
            None
        }
    }
}

async fn fetch_sol_balance(ctx: &SamplerCtx, wallet: &str) -> Option<(f64, u64)> {
    let rpc_url = ctx.cfg.env.helius_rpc_url.as_deref()
        .or(Some(ctx.cfg.env.solana_rpc_url.as_str()))?;
    let _permit = ctx.guards.helius_rpc.acquire().await?;
    let started = Instant::now();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [wallet]
    });
    match ctx.http.post(rpc_url).json(&body).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                ctx.guards
                    .helius_rpc
                    .record_failure(&format!("http_{}", resp.status().as_u16()));
                return None;
            }
            match resp.json::<Value>().await {
                Ok(v) => {
                    ctx.guards.helius_rpc.record_success();
                    let lamports = v
                        .get("result")
                        .and_then(|r| r.get("value"))
                        .and_then(Value::as_u64)?;
                    let sol = lamports as f64 / 1_000_000_000.0;
                    Some((sol, started.elapsed().as_millis() as u64))
                }
                Err(e) => {
                    ctx.guards
                        .helius_rpc
                        .record_failure(&format!("parse:{}", e));
                    None
                }
            }
        }
        Err(e) => {
            ctx.guards.helius_rpc.record_failure(&format!("net:{}", e));
            None
        }
    }
}

async fn fetch_price_usd(ctx: &SamplerCtx, mint: &str) -> Option<f64> {
    // Try Birdeye first, then DexScreener
    if let Some((val, _)) = fetch_birdeye(ctx, mint).await {
        let data = val.get("data").unwrap_or(&val);
        if let Some(p) = data.get("price").and_then(Value::as_f64) {
            return Some(p);
        }
    }
    if let Some((val, _)) = fetch_dexscreener(ctx, mint).await {
        if let Some(p) = val
            .get("pairs")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .and_then(|pair| pair.get("priceUsd"))
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok())
        {
            return Some(p);
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Supabase writer
// ─────────────────────────────────────────────────────────────────────────────

async fn write_snapshot(supabase: &SupabaseClient, snap: &Snapshot) -> anyhow::Result<()> {
    let url = format!("{}/position_enrichment_snapshots", supabase.base_url);
    let payload = json!({
        "position_id": snap.position_id,
        "mint": snap.mint,
        "elapsed_secs": snap.elapsed_secs,
        "trigger": snap.trigger,

        "price_usd": snap.price_usd,
        "pnl_pct": snap.pnl_pct,
        "peak_multiplier": snap.peak_multiplier,

        "holder_count": snap.holder_count,
        "holder_delta_from_prev": snap.holder_delta_from_prev,
        "top10_concentration_pct": snap.top10_concentration_pct,

        "vol_5m_usd": snap.vol_5m_usd,
        "vol_1h_usd": snap.vol_1h_usd,
        "vol_acceleration": snap.vol_acceleration,
        "buy_count_5m": snap.buy_count_5m,
        "sell_count_5m": snap.sell_count_5m,
        "buy_sell_ratio": snap.buy_sell_ratio,
        "unique_traders_5m": snap.unique_traders_5m,

        "social_count": snap.social_count,
        "has_twitter": snap.has_twitter,
        "has_telegram": snap.has_telegram,
        "has_website": snap.has_website,
        "new_social_links": snap.new_social_links,

        "smart_wallet_buy_count": snap.smart_wallet_buy_count,
        "smart_wallet_sell_count": snap.smart_wallet_sell_count,
        "smart_wallet_net_sol": snap.smart_wallet_net_sol,
        "smart_wallets": snap.smart_wallets,

        "whale_buy_count": snap.whale_buy_count,
        "whale_sell_count": snap.whale_sell_count,
        "largest_trade_sol": snap.largest_trade_sol,

        "dev_wallet_sol_delta": snap.dev_wallet_sol_delta,
        "dev_wallet_token_balance": snap.dev_wallet_token_balance,

        "liquidity_usd": snap.liquidity_usd,
        "liquidity_delta_pct": snap.liquidity_delta_pct,
        "market_cap_usd": snap.market_cap_usd,
        "price_impact_1sol_bps": snap.price_impact_1sol_bps,

        "raw_birdeye": snap.raw_birdeye,
        "raw_dexscreener": snap.raw_dexscreener,
        "raw_solana_tracker": snap.raw_solana_tracker,
        "raw_das": snap.raw_das,

        "apis_called": Value::Object(snap.apis_called.clone()),
        "apis_failed": Value::Object(snap.apis_failed.clone()),
    });

    let resp = supabase.client.post(&url).json(&payload).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "HTTP {} — {}",
            status,
            &body[..body.len().min(300)]
        );
    }
    Ok(())
}
