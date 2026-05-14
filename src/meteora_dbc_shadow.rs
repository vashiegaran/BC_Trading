//! Shadow-only Meteora DBC collector.
//!
//! Runs inside the existing bot process so PM2 manages it together with the
//! Rust binary, but it is deliberately isolated from live trading:
//! - no gRPC / Yellowstone stream usage;
//! - no wallet access or transaction signing;
//! - no `GraduatedToken`, filter, execution, or exit channel writes;
//! - writes only to the `meteora_dbc_shadow` research table.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{SecondsFormat, TimeZone, Utc};
use reqwest::Client;
use serde_json::{json, Map, Value};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::logger::SupabaseClient;

const DEX_SEARCH_URL: &str = "https://api.dexscreener.com/latest/dex/search";
const DEX_TOKEN_URL_PREFIX: &str = "https://api.dexscreener.com/latest/dex/tokens";
const USER_AGENT: &str = "BC-Trading-Meteora-DBC-Shadow-Rust/1.0";
const TABLE_RETRY_SECONDS: u64 = 300;
const MIN_INTERVAL_SECONDS: u64 = 30;

/// Start the background Meteora DBC shadow collector if enabled in config.
pub fn start(cfg: Arc<AppConfig>, supabase: Arc<SupabaseClient>) {
    if !cfg.strategy.detection.meteora_dbc_shadow_enabled {
        debug!("meteora_dbc_shadow: disabled");
        return;
    }

    tokio::spawn(async move {
        run_loop(cfg, supabase).await;
    });

    info!(
        "meteora_dbc_shadow: background DexScreener-only shadow collector spawned (no gRPC, no live execution)"
    );
}

async fn run_loop(cfg: Arc<AppConfig>, supabase: Arc<SupabaseClient>) {
    let client = match Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            warn!("meteora_dbc_shadow: failed to build HTTP client: {}", e);
            return;
        }
    };

    let interval = cfg
        .strategy
        .detection
        .meteora_dbc_shadow_interval_seconds
        .max(MIN_INTERVAL_SECONDS);
    let mut table_ready_logged = false;

    loop {
        if !shadow_table_ready(&supabase).await {
            warn!(
                retry_seconds = TABLE_RETRY_SECONDS,
                "meteora_dbc_shadow: table is not ready; apply migrations/038_meteora_dbc_shadow.sql. Live trading continues unaffected."
            );
            sleep(Duration::from_secs(TABLE_RETRY_SECONDS)).await;
            continue;
        }

        if !table_ready_logged {
            table_ready_logged = true;
            info!("meteora_dbc_shadow: Supabase table ready; starting collection");
        }

        if let Err(e) = run_once(&client, &cfg, &supabase).await {
            warn!("meteora_dbc_shadow: poll failed: {:#}", e);
        }

        sleep(Duration::from_secs(interval)).await;
    }
}

async fn shadow_table_ready(supabase: &SupabaseClient) -> bool {
    let url = format!(
        "{}/meteora_dbc_shadow?select=mint&limit=1",
        supabase.base_url
    );
    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => true,
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            debug!(
                status = %status,
                body = %body,
                "meteora_dbc_shadow: table readiness check failed"
            );
            false
        }
        Err(e) => {
            debug!("meteora_dbc_shadow: table readiness request failed: {}", e);
            false
        }
    }
}

async fn run_once(client: &Client, cfg: &AppConfig, supabase: &SupabaseClient) -> Result<()> {
    let query = cfg.strategy.detection.meteora_dbc_shadow_query.as_str();
    let limit = cfg.strategy.detection.meteora_dbc_shadow_limit;
    let min_score = cfg.strategy.detection.meteora_dbc_shadow_min_score;

    let mints = discover_meteoradbc_mints(client, query, limit).await?;
    info!(
        count = mints.len(),
        query = query,
        "meteora_dbc_shadow: discovered DBC mints"
    );

    for mint in mints {
        match process_mint(client, supabase, &mint, min_score).await {
            Ok(status) => debug!(mint = %mint, status = status, "meteora_dbc_shadow: row updated"),
            Err(e) => warn!(mint = %mint, "meteora_dbc_shadow: mint update failed: {:#}", e),
        }
    }

    Ok(())
}

async fn process_mint(
    client: &Client,
    supabase: &SupabaseClient,
    mint: &str,
    min_score: f64,
) -> Result<&'static str> {
    let pairs = fetch_pairs_for_mint(client, mint).await?;
    let payload = build_payload(mint, &pairs, min_score)
        .ok_or_else(|| anyhow!("no meteoradbc pair found in token pair set"))?;
    update_shadow_row(supabase, payload).await
}

async fn discover_meteoradbc_mints(
    client: &Client,
    query: &str,
    limit: usize,
) -> Result<Vec<String>> {
    let url = format!("{}?q={}", DEX_SEARCH_URL, percent_encode(query));
    let data: Value = client
        .get(url)
        .send()
        .await
        .context("DexScreener search request failed")?
        .error_for_status()
        .context("DexScreener search returned non-success")?
        .json()
        .await
        .context("DexScreener search JSON parse failed")?;

    let mut seen = HashSet::new();
    let mut mints = Vec::new();

    for pair in data.get("pairs").and_then(Value::as_array).into_iter().flatten() {
        if str_field(pair, "chainId") != Some("solana")
            || str_field(pair, "dexId") != Some("meteoradbc")
        {
            continue;
        }
        let Some(mint) = nested(pair, &["baseToken", "address"]).and_then(Value::as_str) else {
            continue;
        };
        if seen.insert(mint.to_string()) {
            mints.push(mint.to_string());
        }
        if mints.len() >= limit {
            break;
        }
    }

    Ok(mints)
}

async fn fetch_pairs_for_mint(client: &Client, mint: &str) -> Result<Vec<Value>> {
    let url = format!("{}/{}", DEX_TOKEN_URL_PREFIX, percent_encode(mint));
    let data: Value = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("DexScreener token request failed for {mint}"))?
        .error_for_status()
        .with_context(|| format!("DexScreener token request returned non-success for {mint}"))?
        .json()
        .await
        .with_context(|| format!("DexScreener token JSON parse failed for {mint}"))?;

    Ok(data
        .get("pairs")
        .and_then(Value::as_array)
        .map(|pairs| {
            pairs
                .iter()
                .filter(|p| str_field(p, "chainId") == Some("solana"))
                .cloned()
                .collect()
        })
        .unwrap_or_default())
}

fn build_payload(mint: &str, all_pairs: &[Value], min_score: f64) -> Option<Value> {
    let dbc_pair = choose_best_pair(all_pairs, "meteoradbc")?;
    let meteora_pair = choose_best_pair(all_pairs, "meteora");
    let active_pair = meteora_pair.unwrap_or(dbc_pair);
    let base = active_pair
        .get("baseToken")
        .or_else(|| dbc_pair.get("baseToken"));

    let (score, reasons, penalties) = compute_meteora_dbc_score(dbc_pair, meteora_pair);

    let h1_buys = txns(active_pair, "h1", "buys").or_else(|| txns(active_pair, "h24", "buys"));
    let h1_sells =
        txns(active_pair, "h1", "sells").or_else(|| txns(active_pair, "h24", "sells"));
    let m5_buys = txns(active_pair, "m5", "buys").unwrap_or(0);
    let m5_sells = txns(active_pair, "m5", "sells").unwrap_or(0);
    let h1_buys_i = h1_buys.unwrap_or(0);
    let h1_sells_i = h1_sells.unwrap_or(0);

    Some(json!({
        "mint": mint,
        "symbol": base.and_then(|v| v.get("symbol")).and_then(Value::as_str),
        "name": base.and_then(|v| v.get("name")).and_then(Value::as_str),
        "source": "dexscreener_search_rust",
        "chain_id": str_field(active_pair, "chainId").unwrap_or("solana"),
        "dbc_dex_id": "meteoradbc",
        "dbc_pair_address": str_field(dbc_pair, "pairAddress"),
        "dbc_pair_url": str_field(dbc_pair, "url"),
        "dbc_pair_created_at": ms_to_iso(field_f64(dbc_pair, "pairCreatedAt")),
        "meteora_pair_address": meteora_pair.and_then(|p| str_field(p, "pairAddress")),
        "meteora_pair_url": meteora_pair.and_then(|p| str_field(p, "url")),
        "meteora_pair_created_at": meteora_pair.and_then(|p| ms_to_iso(field_f64(p, "pairCreatedAt"))),
        "price_usd": field_f64(active_pair, "priceUsd"),
        "market_cap_usd": field_f64(active_pair, "marketCap"),
        "fdv_usd": field_f64(active_pair, "fdv"),
        "liquidity_usd": nested_f64(active_pair, &["liquidity", "usd"]),
        "liquidity_base": nested_f64(active_pair, &["liquidity", "base"]),
        "liquidity_quote": nested_f64(active_pair, &["liquidity", "quote"]),
        "volume_m5_usd": volume(active_pair, "m5"),
        "volume_h1_usd": volume(active_pair, "h1"),
        "volume_h6_usd": volume(active_pair, "h6"),
        "volume_h24_usd": volume(active_pair, "h24"),
        "txns_m5_buys": m5_buys,
        "txns_m5_sells": m5_sells,
        "txns_h1_buys": h1_buys_i,
        "txns_h1_sells": h1_sells_i,
        "txns_h6_buys": txns(active_pair, "h6", "buys").unwrap_or(0),
        "txns_h6_sells": txns(active_pair, "h6", "sells").unwrap_or(0),
        "txns_h24_buys": txns(active_pair, "h24", "buys").unwrap_or(0),
        "txns_h24_sells": txns(active_pair, "h24", "sells").unwrap_or(0),
        "buy_pressure_h1_pct": buy_pressure_pct(h1_buys_i, h1_sells_i),
        "buy_sell_ratio_h1": buy_sell_ratio(h1_buys_i, h1_sells_i),
        "price_change_m5_pct": price_change(active_pair, "m5"),
        "price_change_h1_pct": price_change(active_pair, "h1"),
        "price_change_h6_pct": price_change(active_pair, "h6"),
        "price_change_h24_pct": price_change(active_pair, "h24"),
        "meteora_dbc_score": score,
        "score_reasons": reasons,
        "score_penalties": penalties,
        "would_trade_shadow": score >= min_score,
        "min_score_threshold": min_score,
        "raw_pairs": all_pairs,
        "last_status": "tracking",
    }))
}

fn compute_meteora_dbc_score(
    dbc_pair: &Value,
    meteora_pair: Option<&Value>,
) -> (f64, Vec<String>, Vec<String>) {
    let mut score: f64 = 50.0;
    let mut reasons = Vec::new();
    let mut penalties = Vec::new();

    let active_pair = meteora_pair.unwrap_or(dbc_pair);
    let market_cap = field_f64(active_pair, "marketCap").or_else(|| field_f64(active_pair, "fdv"));
    let liquidity_usd = nested_f64(active_pair, &["liquidity", "usd"]);
    let vol_m5 = volume(active_pair, "m5").unwrap_or(0.0);
    let vol_h1 = volume(active_pair, "h1")
        .or_else(|| volume(active_pair, "h24"))
        .unwrap_or(0.0);
    let buys_h1 = txns(active_pair, "h1", "buys")
        .or_else(|| txns(active_pair, "h24", "buys"))
        .unwrap_or(0);
    let sells_h1 = txns(active_pair, "h1", "sells")
        .or_else(|| txns(active_pair, "h24", "sells"))
        .unwrap_or(0);
    let buys_m5 = txns(active_pair, "m5", "buys").unwrap_or(0);
    let sells_m5 = txns(active_pair, "m5", "sells").unwrap_or(0);
    let bp_h1 = buy_pressure_pct(buys_h1, sells_h1);
    let bp_m5 = buy_pressure_pct(buys_m5, sells_m5);
    let ratio_h1 = buy_sell_ratio(buys_h1, sells_h1);
    let change_m5 = price_change(active_pair, "m5").unwrap_or(0.0);
    let change_h1 = price_change(active_pair, "h1")
        .or_else(|| price_change(active_pair, "h24"))
        .unwrap_or(0.0);

    if meteora_pair.is_some() {
        score += 10.0;
        reasons.push("active_meteora_pair".to_string());
    } else {
        score += 5.0;
        reasons.push("dbc_pair_only".to_string());
    }

    match liquidity_usd {
        None => {
            score -= 20.0;
            penalties.push("missing_liquidity".to_string());
        }
        Some(liq) if liq < 3_000.0 => {
            score -= 15.0;
            penalties.push(format!("thin_liquidity_usd_{liq:.0}"));
        }
        Some(liq) if liq <= 100_000.0 => {
            score += 10.0;
            reasons.push(format!("healthy_liquidity_usd_{liq:.0}"));
        }
        Some(liq) => {
            score -= 5.0;
            penalties.push(format!("large_liquidity_late_usd_{liq:.0}"));
        }
    }

    if vol_h1 >= 250_000.0 {
        score += 20.0;
        reasons.push(format!("very_high_h1_volume_usd_{vol_h1:.0}"));
    } else if vol_h1 >= 50_000.0 {
        score += 12.0;
        reasons.push(format!("high_h1_volume_usd_{vol_h1:.0}"));
    } else if vol_h1 >= 10_000.0 {
        score += 6.0;
        reasons.push(format!("moderate_h1_volume_usd_{vol_h1:.0}"));
    }

    if vol_m5 >= 25_000.0 {
        score += 12.0;
        reasons.push(format!("strong_m5_volume_usd_{vol_m5:.0}"));
    } else if vol_m5 >= 5_000.0 {
        score += 6.0;
        reasons.push(format!("moderate_m5_volume_usd_{vol_m5:.0}"));
    }

    let tx_h1 = buys_h1 + sells_h1;
    if tx_h1 >= 10_000 {
        score += 12.0;
        reasons.push(format!("extreme_h1_tx_count_{tx_h1}"));
    } else if tx_h1 >= 1_000 {
        score += 8.0;
        reasons.push(format!("high_h1_tx_count_{tx_h1}"));
    } else if tx_h1 >= 250 {
        score += 4.0;
        reasons.push(format!("active_h1_tx_count_{tx_h1}"));
    }

    if let Some(bp) = bp_h1 {
        if bp >= 55.0 {
            score += 10.0;
            reasons.push(format!("h1_buy_pressure_{bp:.1}"));
        } else if bp >= 49.0 {
            score += 4.0;
            reasons.push(format!("balanced_h1_flow_{bp:.1}"));
        } else if bp < 42.0 {
            score -= 12.0;
            penalties.push(format!("sell_heavy_h1_flow_{bp:.1}"));
        }
    }

    if let Some(bp) = bp_m5 {
        if bp >= 55.0 {
            score += 8.0;
            reasons.push(format!("m5_buy_pressure_{bp:.1}"));
        } else if bp < 40.0 {
            score -= 8.0;
            penalties.push(format!("sell_heavy_m5_flow_{bp:.1}"));
        }
    }

    if let Some(ratio) = ratio_h1 {
        if ratio >= 1.2 {
            score += 6.0;
            reasons.push(format!("h1_buy_sell_ratio_{ratio:.2}"));
        }
    }

    if change_h1 >= 1_000.0 {
        score += 12.0;
        reasons.push(format!("large_h1_price_change_{change_h1:.0}pct"));
    } else if change_h1 >= 150.0 {
        score += 8.0;
        reasons.push(format!("positive_h1_price_change_{change_h1:.0}pct"));
    } else if change_h1 <= -50.0 {
        score -= 15.0;
        penalties.push(format!("negative_h1_price_change_{change_h1:.0}pct"));
    }

    if change_m5 >= 100.0 {
        score += 8.0;
        reasons.push(format!("large_m5_price_change_{change_m5:.0}pct"));
    } else if change_m5 <= -35.0 {
        score -= 10.0;
        penalties.push(format!("negative_m5_price_change_{change_m5:.0}pct"));
    }

    if let Some(mc) = market_cap {
        if mc < 20_000.0 {
            score -= 10.0;
            penalties.push(format!("tiny_market_cap_{mc:.0}"));
        } else if mc <= 5_000_000.0 {
            score += 5.0;
            reasons.push(format!("tradable_market_cap_{mc:.0}"));
        } else if mc > 10_000_000.0 {
            score -= 10.0;
            penalties.push(format!("late_market_cap_{mc:.0}"));
        }
    }

    let score = score.clamp(0.0, 150.0);
    ((score * 100.0).round() / 100.0, reasons, penalties)
}

async fn update_shadow_row(supabase: &SupabaseClient, payload: Value) -> Result<&'static str> {
    let mint = payload
        .get("mint")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("payload missing mint"))?;
    let encoded_mint = percent_encode(mint);
    let select_url = format!(
        "{}/meteora_dbc_shadow?select=id,first_seen_market_cap_usd,first_seen_price_usd,peak_market_cap_usd,peak_price_usd,sample_count&mint=eq.{}&limit=1",
        supabase.base_url, encoded_mint
    );

    let rows: Vec<Value> = supabase
        .client
        .get(&select_url)
        .send()
        .await
        .context("Supabase select failed")?
        .error_for_status()
        .context("Supabase select returned non-success")?
        .json()
        .await
        .context("Supabase select JSON parse failed")?;

    let current_mc = payload.get("market_cap_usd").and_then(json_as_f64);
    let current_price = payload.get("price_usd").and_then(json_as_f64);

    if let Some(row) = rows.first() {
        let first_mc = row
            .get("first_seen_market_cap_usd")
            .and_then(json_as_f64)
            .or(current_mc);
        let first_price = row
            .get("first_seen_price_usd")
            .and_then(json_as_f64)
            .or(current_price);
        let peak_mc = max_opt(
            row.get("peak_market_cap_usd").and_then(json_as_f64),
            current_mc,
        );
        let peak_price = max_opt(row.get("peak_price_usd").and_then(json_as_f64), current_price);
        let peak_multiplier = match (peak_mc, first_mc) {
            (Some(peak), Some(first)) if first > 0.0 => Some(peak / first),
            _ => None,
        };
        let sample_count = row
            .get("sample_count")
            .and_then(Value::as_i64)
            .unwrap_or(1)
            + 1;

        let mut patch = object_from_payload(&payload)?;
        patch.insert("first_seen_market_cap_usd".to_string(), json!(first_mc));
        patch.insert("first_seen_price_usd".to_string(), json!(first_price));
        patch.insert("peak_market_cap_usd".to_string(), json!(peak_mc));
        patch.insert("peak_price_usd".to_string(), json!(peak_price));
        patch.insert("peak_multiplier".to_string(), json!(peak_multiplier));
        patch.insert("sample_count".to_string(), json!(sample_count));
        patch.insert("last_seen_at".to_string(), json!(now_iso()));
        patch.insert("updated_at".to_string(), json!(now_iso()));

        let patch_url = format!(
            "{}/meteora_dbc_shadow?mint=eq.{}",
            supabase.base_url, encoded_mint
        );
        send_supabase_write(supabase.client.patch(&patch_url).json(&Value::Object(patch))).await?;
        return Ok("patched");
    }

    let mut insert = object_from_payload(&payload)?;
    insert.insert("first_seen_market_cap_usd".to_string(), json!(current_mc));
    insert.insert("first_seen_price_usd".to_string(), json!(current_price));
    insert.insert("peak_market_cap_usd".to_string(), json!(current_mc));
    insert.insert("peak_price_usd".to_string(), json!(current_price));
    insert.insert(
        "peak_multiplier".to_string(),
        json!(current_mc.filter(|mc| *mc > 0.0).map(|_| 1.0)),
    );

    let insert_url = format!("{}/meteora_dbc_shadow", supabase.base_url);
    send_supabase_write(supabase.client.post(&insert_url).json(&Value::Object(insert))).await?;
    Ok("inserted")
}

async fn send_supabase_write(builder: reqwest::RequestBuilder) -> Result<()> {
    let resp = builder.send().await.context("Supabase write request failed")?;
    if resp.status().is_success() {
        return Ok(());
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    Err(anyhow!("Supabase write failed HTTP {status}: {body}"))
}

fn object_from_payload(payload: &Value) -> Result<Map<String, Value>> {
    payload
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("payload is not a JSON object"))
}

fn choose_best_pair<'a>(pairs: &'a [Value], dex_id: &str) -> Option<&'a Value> {
    pairs
        .iter()
        .filter(|pair| str_field(pair, "chainId") == Some("solana") && str_field(pair, "dexId") == Some(dex_id))
        .max_by(|a, b| {
            let av = nested_f64(a, &["liquidity", "usd"])
                .or_else(|| field_f64(a, "marketCap"))
                .unwrap_or(0.0);
            let bv = nested_f64(b, &["liquidity", "usd"])
                .or_else(|| field_f64(b, "marketCap"))
                .unwrap_or(0.0);
            av.partial_cmp(&bv).unwrap_or(Ordering::Equal)
        })
}

fn nested<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let mut cur = value;
    for key in keys {
        cur = cur.get(*key)?;
    }
    Some(cur)
}

fn str_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn field_f64(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(json_as_f64)
}

fn nested_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    nested(value, keys).and_then(json_as_f64)
}

fn json_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
    .filter(|v| v.is_finite())
}

fn json_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn txns(pair: &Value, window: &str, side: &str) -> Option<i64> {
    nested(pair, &["txns", window, side]).and_then(json_as_i64)
}

fn volume(pair: &Value, window: &str) -> Option<f64> {
    nested_f64(pair, &["volume", window])
}

fn price_change(pair: &Value, window: &str) -> Option<f64> {
    nested_f64(pair, &["priceChange", window])
}

fn buy_pressure_pct(buys: i64, sells: i64) -> Option<f64> {
    let total = buys + sells;
    if total <= 0 {
        None
    } else {
        Some(buys as f64 / total as f64 * 100.0)
    }
}

fn buy_sell_ratio(buys: i64, sells: i64) -> Option<f64> {
    if sells <= 0 {
        if buys > 0 {
            Some(buys as f64)
        } else {
            None
        }
    } else {
        Some(buys as f64 / sells as f64)
    }
}

fn max_opt(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

fn ms_to_iso(ms: Option<f64>) -> Option<String> {
    let ms = ms?;
    let ms_i = ms as i64;
    Utc.timestamp_millis_opt(ms_i)
        .single()
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}
