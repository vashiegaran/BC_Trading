use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use chrono::{TimeZone, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::logger::SupabaseClient;
use crate::monitoring::price::PriceFetcher;

const BAGS_AUTHORITY: &str = "BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv";
const BAGS_PROGRAM: &str = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const MAX_SIGNATURE_PAGE_SIZE: usize = 100;
const MAX_SIGNATURE_PAGES: usize = 4;
const MAX_PENDING_EVALS_PER_LOOP: usize = 12;
const SHADOW_PRICE_15M_SECS: u64 = 900;
const SHADOW_PRICE_1H_SECS: u64 = 3600;

#[derive(Debug, Clone, Deserialize)]
struct SignatureInfo {
    signature: String,
    slot: u64,
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
}

#[derive(Debug, Clone)]
struct ParsedAccountKey {
    pubkey: String,
    signer: bool,
}

#[derive(Debug, Clone)]
struct TokenBalance {
    mint: String,
    owner: Option<String>,
    account_index: usize,
    ui_amount: f64,
}

#[derive(Debug, Clone)]
struct DetectedLaunch {
    mint: String,
    launch_signature: String,
    launch_slot: u64,
    launch_at_ts: i64,
    creator_wallet: String,
    bags_fee_payer: String,
    creator_funding_lamports: u64,
    pool_owner_wallet: Option<String>,
    pool_token_account: Option<String>,
    signers: Vec<String>,
    name: Option<String>,
    symbol: Option<String>,
}

#[derive(Debug, Default)]
struct DemandStats {
    trade_count: usize,
    buy_tx_count: usize,
    unique_buyers: HashSet<String>,
    buy_volume_sol: f64,
    peak_single_buy_sol: f64,
}

#[derive(Debug, Deserialize)]
struct PendingLaunchRow {
    mint: String,
    creator_wallet: String,
    pool_owner_wallet: Option<String>,
    launch_signature: String,
    launch_at: String,
}

#[derive(Debug, Deserialize)]
struct CreatorLaunchRow {
    mint: String,
    launch_at: String,
    has_real_demand: Option<bool>,
    demand_unique_buyers: Option<i32>,
    demand_buy_volume_sol: Option<f64>,
}

#[derive(Debug, Serialize)]
struct CreatorStatsRow {
    creator_wallet: String,
    launch_count: i32,
    demand_launch_count: i32,
    demand_rate: f64,
    avg_unique_buyers: f64,
    avg_buy_volume_sol: f64,
    best_mint: Option<String>,
    best_buy_volume_sol: Option<f64>,
    last_launch_at: Option<String>,
    last_demand_launch_at: Option<String>,
    watchworthy: bool,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct CreatorStatsLookupRow {
    watchworthy: bool,
    launch_count: i32,
    demand_launch_count: i32,
    demand_rate: f64,
}

#[derive(Debug, Clone)]
struct ShadowTrigger {
    mint: String,
    symbol: Option<String>,
    name: Option<String>,
    launch_signature: String,
    creator_wallet: String,
    launch_at_ts: i64,
    launch_count_at_entry: i32,
    demand_launch_count_at_entry: i32,
    demand_rate_at_entry: f64,
}

#[derive(Debug, Clone, Copy)]
struct ShadowTrackerConfig {
    max_wait_secs: u64,
    poll_interval_secs: u64,
    duration_secs: u64,
}

pub fn start(cfg: Arc<AppConfig>, supabase: Arc<SupabaseClient>) {
    if !cfg.strategy.monitoring.bags_launch_monitor_enabled {
        info!("Bags launch monitor disabled");
        return;
    }

    tokio::spawn(async move {
        if !ensure_tables_ready(&supabase).await {
            return;
        }

        let shadow_table_ready = if cfg.strategy.monitoring.bags_watchworthy_shadow_enabled {
            ensure_table_ready(&supabase, "bags_shadow_entries", "id").await
        } else {
            false
        };

        let rpc_client = match Client::builder().timeout(Duration::from_secs(30)).build() {
            Ok(client) => client,
            Err(e) => {
                warn!(error = %e, "bags_monitor: failed to build RPC client");
                return;
            }
        };

        let price_fetcher = Arc::new(PriceFetcher::new(
            cfg.env.birdeye_api_key.clone(),
            cfg.strategy.monitoring.price_timeout_secs,
            cfg.strategy.execution.api_request_timeout_secs,
            cfg.strategy.execution.max_retries,
            cfg.strategy.monitoring.max_sane_price_usd,
            cfg.strategy.monitoring.max_price_change_ratio,
        ));

        log_event(
            &supabase,
            "bags_monitor_started",
            &format!(
                "poll_interval={}s demand_window={}s shadow_enabled={} shadow_table_ready={}",
                cfg.strategy.monitoring.bags_launch_poll_interval_secs,
                cfg.strategy.monitoring.bags_demand_window_secs,
                cfg.strategy.monitoring.bags_watchworthy_shadow_enabled,
                shadow_table_ready,
            ),
        )
        .await;

        let mut last_seen_signature: Option<String> = None;
        loop {
            if let Err(e) = poll_once(
                &cfg,
                &supabase,
                &rpc_client,
                &price_fetcher,
                shadow_table_ready,
                &mut last_seen_signature,
            )
            .await
            {
                warn!(error = %e, "bags_monitor: poll loop failed");
            }

            sleep(Duration::from_secs(
                cfg.strategy.monitoring.bags_launch_poll_interval_secs,
            ))
            .await;
        }
    });

    info!("Bags launch monitor task spawned");
}

async fn ensure_tables_ready(supabase: &SupabaseClient) -> bool {
    ensure_table_ready(supabase, "bags_launches", "id").await
        && ensure_table_ready(supabase, "bags_creator_stats", "creator_wallet").await
}

async fn ensure_table_ready(supabase: &SupabaseClient, table: &str, column: &str) -> bool {
    let url = format!("{}/{}?select={}&limit=1", supabase.base_url, table, column);
    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => true,
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(table, body = %body, "bags_monitor: table missing or inaccessible; run the matching migration and restart");
            false
        }
        Err(e) => {
            warn!(table, error = %e, "bags_monitor: table readiness check failed");
            false
        }
    }
}

async fn poll_once(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    rpc_client: &Client,
    price_fetcher: &Arc<PriceFetcher>,
    shadow_table_ready: bool,
    last_seen_signature: &mut Option<String>,
) -> Result<()> {
    let new_signatures = fetch_new_signatures(
        rpc_client,
        &cfg.env.solana_rpc_url,
        last_seen_signature.as_deref(),
    )
    .await?;

    if let Some(latest) = new_signatures.last() {
        *last_seen_signature = Some(latest.signature.clone());
    }

    for sig in &new_signatures {
        if let Some(launch) = detect_launch(rpc_client, &cfg.env.solana_rpc_url, sig).await? {
            upsert_launch(supabase, &launch).await?;
            if shadow_table_ready {
                maybe_fire_watchworthy_shadow(cfg, supabase, price_fetcher, &launch).await?;
            }
            log_event(
                supabase,
                "bags_launch_detected",
                &format!(
                    "mint={} creator={} signature={} symbol={} funding_sol={:.6}",
                    launch.mint,
                    launch.creator_wallet,
                    launch.launch_signature,
                    launch
                        .symbol
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    launch.creator_funding_lamports as f64 / 1_000_000_000.0,
                ),
            )
            .await;
        }
    }

    score_pending_launches(cfg, supabase, rpc_client).await?;
    Ok(())
}

async fn fetch_new_signatures(
    rpc_client: &Client,
    rpc_url: &str,
    last_seen_signature: Option<&str>,
) -> Result<Vec<SignatureInfo>> {
    let mut before: Option<String> = None;
    let mut collected: Vec<SignatureInfo> = Vec::new();

    for page_idx in 0..MAX_SIGNATURE_PAGES {
        let mut cfg = json!({ "limit": MAX_SIGNATURE_PAGE_SIZE });
        if let Some(before_sig) = &before {
            cfg["before"] = json!(before_sig);
        }

        let page_value = rpc_call(
            rpc_client,
            rpc_url,
            "getSignaturesForAddress",
            json!([BAGS_AUTHORITY, cfg]),
        )
        .await?;

        let page: Vec<SignatureInfo> = serde_json::from_value(page_value)
            .context("bags_monitor: decode getSignaturesForAddress")?;

        if page.is_empty() {
            break;
        }

        let mut found_last_seen = false;
        for info in &page {
            if last_seen_signature == Some(info.signature.as_str()) {
                found_last_seen = true;
                break;
            }
            collected.push(info.clone());
        }

        if found_last_seen || last_seen_signature.is_none() || page_idx + 1 >= MAX_SIGNATURE_PAGES {
            break;
        }

        before = page.last().map(|row| row.signature.clone());
    }

    collected.reverse();
    Ok(collected)
}

async fn detect_launch(
    rpc_client: &Client,
    rpc_url: &str,
    sig: &SignatureInfo,
) -> Result<Option<DetectedLaunch>> {
    let tx = rpc_call(
        rpc_client,
        rpc_url,
        "getTransaction",
        json!([
            sig.signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0
            }
        ]),
    )
    .await?;

    if tx.is_null() {
        return Ok(None);
    }

    let logs = transaction_logs(&tx);
    let is_launch = logs
        .iter()
        .any(|line| line.contains("Instruction: InitializeVirtualPoolWithSplToken"))
        && logs.iter().any(|line| line.contains("Instruction: MintTo"))
        && logs.iter().any(|line| line.contains(BAGS_PROGRAM));
    if !is_launch {
        return Ok(None);
    }

    let account_keys = transaction_account_keys(&tx);
    if account_keys.is_empty() {
        return Ok(None);
    }
    let signers: Vec<String> = account_keys
        .iter()
        .filter(|key| key.signer)
        .map(|key| key.pubkey.clone())
        .collect();
    if !signers.iter().any(|signer| signer == BAGS_AUTHORITY) {
        return Ok(None);
    }

    let post_balances = token_balances(&tx, "postTokenBalances");
    let minted_balance = post_balances
        .iter()
        .filter(|bal| bal.mint != WSOL_MINT)
        .max_by(|lhs, rhs| {
            lhs.ui_amount
                .partial_cmp(&rhs.ui_amount)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();
    let Some(minted_balance) = minted_balance else {
        return Ok(None);
    };

    let mint = minted_balance.mint.clone();
    let creator_wallet = signers
        .iter()
        .find(|signer| signer.as_str() != BAGS_AUTHORITY && signer.as_str() != mint.as_str())
        .cloned()
        .or_else(|| find_transfer_source_to_bags(&tx, BAGS_AUTHORITY));
    let Some(creator_wallet) = creator_wallet else {
        return Ok(None);
    };

    let fee_payer = account_keys
        .first()
        .map(|key| key.pubkey.clone())
        .unwrap_or_else(|| BAGS_AUTHORITY.to_string());
    let creator_funding_lamports = sum_system_transfers(&tx, &creator_wallet, BAGS_AUTHORITY);
    let pool_token_account = account_keys
        .get(minted_balance.account_index)
        .map(|key| key.pubkey.clone());

    let (name, symbol) = fetch_asset_metadata(rpc_client, rpc_url, &mint)
        .await
        .unwrap_or((None, None));

    Ok(Some(DetectedLaunch {
        mint,
        launch_signature: sig.signature.clone(),
        launch_slot: sig.slot,
        launch_at_ts: sig.block_time.unwrap_or_else(|| Utc::now().timestamp()),
        creator_wallet,
        bags_fee_payer: fee_payer,
        creator_funding_lamports,
        pool_owner_wallet: minted_balance.owner.clone(),
        pool_token_account,
        signers,
        name,
        symbol,
    }))
}

async fn fetch_asset_metadata(
    rpc_client: &Client,
    rpc_url: &str,
    mint: &str,
) -> Result<(Option<String>, Option<String>)> {
    let asset = rpc_call(rpc_client, rpc_url, "getAsset", json!({ "id": mint })).await;

    let Ok(asset) = asset else {
        return Ok((None, None));
    };

    let metadata = asset
        .get("content")
        .and_then(|content| content.get("metadata"));
    let name = metadata
        .and_then(|meta| meta.get("name"))
        .and_then(Value::as_str)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let symbol = metadata
        .and_then(|meta| meta.get("symbol"))
        .and_then(Value::as_str)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    Ok((name, symbol))
}

async fn upsert_launch(supabase: &SupabaseClient, launch: &DetectedLaunch) -> Result<()> {
    let url = format!("{}/bags_launches?on_conflict=mint", supabase.base_url);
    let launch_at = Utc
        .timestamp_opt(launch.launch_at_ts, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339();
    let payload = json!({
        "mint": launch.mint,
        "symbol": launch.symbol,
        "name": launch.name,
        "launch_signature": launch.launch_signature,
        "launch_slot": launch.launch_slot as i64,
        "launch_at": launch_at,
        "creator_wallet": launch.creator_wallet,
        "bags_fee_payer": launch.bags_fee_payer,
        "creator_funding_lamports": launch.creator_funding_lamports as i64,
        "pool_owner_wallet": launch.pool_owner_wallet,
        "pool_token_account": launch.pool_token_account,
        "signers": launch.signers,
        "updated_at": Utc::now().to_rfc3339(),
    });

    let resp = supabase
        .client
        .post(&url)
        .header("Prefer", "resolution=merge-duplicates,return=minimal")
        .json(&payload)
        .send()
        .await
        .context("bags_monitor: upsert bags_launches request")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: upsert bags_launches failed: HTTP {} — {}",
            status,
            body
        ));
    }

    Ok(())
}

async fn maybe_fire_watchworthy_shadow(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    price_fetcher: &Arc<PriceFetcher>,
    launch: &DetectedLaunch,
) -> Result<()> {
    if !cfg.strategy.monitoring.bags_watchworthy_shadow_enabled {
        return Ok(());
    }

    let age_secs = Utc::now().timestamp().saturating_sub(launch.launch_at_ts);
    if age_secs
        > cfg
            .strategy
            .monitoring
            .bags_watchworthy_shadow_max_age_seconds as i64
    {
        return Ok(());
    }

    let existing_url = format!(
        "{}/bags_shadow_entries?select=id&mint=eq.{}&limit=1",
        supabase.base_url, launch.mint,
    );
    let existing_resp = supabase
        .client
        .get(&existing_url)
        .send()
        .await
        .context("bags_monitor: query existing shadow entry")?;
    if !existing_resp.status().is_success() {
        let status = existing_resp.status();
        let body = existing_resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: existing shadow query failed: HTTP {} — {}",
            status,
            body
        ));
    }
    let existing_rows: Vec<Value> = existing_resp
        .json()
        .await
        .context("bags_monitor: decode existing shadow rows")?;
    if !existing_rows.is_empty() {
        return Ok(());
    }

    let stats = fetch_creator_stats_snapshot(supabase, &launch.creator_wallet).await?;
    let Some(stats) = stats else {
        return Ok(());
    };
    if !stats.watchworthy {
        return Ok(());
    }

    let trigger = ShadowTrigger {
        mint: launch.mint.clone(),
        symbol: launch.symbol.clone(),
        name: launch.name.clone(),
        launch_signature: launch.launch_signature.clone(),
        creator_wallet: launch.creator_wallet.clone(),
        launch_at_ts: launch.launch_at_ts,
        launch_count_at_entry: stats.launch_count,
        demand_launch_count_at_entry: stats.demand_launch_count,
        demand_rate_at_entry: stats.demand_rate,
    };

    insert_shadow_entry(supabase, &trigger).await?;
    log_event(
        supabase,
        "bags_watchworthy_shadow_fired",
        &format!(
            "mint={} creator={} launches={} demand_rate={:.2}",
            trigger.mint,
            trigger.creator_wallet,
            trigger.launch_count_at_entry,
            trigger.demand_rate_at_entry,
        ),
    )
    .await;

    let tracker_cfg = ShadowTrackerConfig {
        max_wait_secs: cfg
            .strategy
            .monitoring
            .bags_shadow_entry_price_max_wait_secs,
        poll_interval_secs: cfg.strategy.monitoring.bags_shadow_poll_interval_secs,
        duration_secs: cfg.strategy.monitoring.bags_shadow_duration_secs,
    };
    let supabase_clone = supabase.clone();
    let price_fetcher = Arc::clone(price_fetcher);
    tokio::spawn(async move {
        if let Err(e) =
            track_shadow_entry(tracker_cfg, supabase_clone, price_fetcher, trigger).await
        {
            warn!(error = %e, "bags_monitor: shadow tracker failed");
        }
    });

    Ok(())
}

async fn fetch_creator_stats_snapshot(
    supabase: &SupabaseClient,
    creator_wallet: &str,
) -> Result<Option<CreatorStatsLookupRow>> {
    let url = format!(
        "{}/bags_creator_stats?select=watchworthy,launch_count,demand_launch_count,demand_rate&creator_wallet=eq.{}&limit=1",
        supabase.base_url,
        creator_wallet,
    );
    let resp = supabase
        .client
        .get(&url)
        .send()
        .await
        .context("bags_monitor: fetch creator stats snapshot")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: creator stats snapshot failed: HTTP {} — {}",
            status,
            body
        ));
    }
    let rows: Vec<CreatorStatsLookupRow> = resp
        .json()
        .await
        .context("bags_monitor: decode creator stats snapshot")?;
    Ok(rows.into_iter().next())
}

async fn insert_shadow_entry(supabase: &SupabaseClient, trigger: &ShadowTrigger) -> Result<()> {
    let url = format!("{}/bags_shadow_entries", supabase.base_url);
    let launch_at = Utc
        .timestamp_opt(trigger.launch_at_ts, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339();
    let payload = json!({
        "mint": trigger.mint,
        "symbol": trigger.symbol,
        "name": trigger.name,
        "entry_trigger": "bags_watchworthy_shadow",
        "launch_signature": trigger.launch_signature,
        "launch_at": launch_at,
        "creator_wallet": trigger.creator_wallet,
        "creator_launch_count_at_entry": trigger.launch_count_at_entry,
        "creator_demand_launch_count_at_entry": trigger.demand_launch_count_at_entry,
        "creator_demand_rate_at_entry": trigger.demand_rate_at_entry,
        "status": "pending",
        "status_message": "Waiting for initial price",
        "updated_at": Utc::now().to_rfc3339(),
    });

    let resp = supabase
        .client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .context("bags_monitor: insert bags_shadow_entries")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: insert bags_shadow_entries failed: HTTP {} — {}",
            status,
            body
        ));
    }

    Ok(())
}

async fn track_shadow_entry(
    tracker_cfg: ShadowTrackerConfig,
    supabase: SupabaseClient,
    price_fetcher: Arc<PriceFetcher>,
    trigger: ShadowTrigger,
) -> Result<()> {
    let launch_age_secs = Utc::now()
        .timestamp()
        .saturating_sub(trigger.launch_at_ts)
        .max(0) as u64;
    let mut waited = Duration::from_secs(launch_age_secs.min(tracker_cfg.max_wait_secs));
    let max_wait = Duration::from_secs(tracker_cfg.max_wait_secs);
    let retry_sleep = Duration::from_secs(10);
    let patch_url = format!(
        "{}/bags_shadow_entries?mint=eq.{}",
        supabase.base_url, trigger.mint
    );

    let mut entry_price_usd = 0.0;
    while waited <= max_wait {
        let price = price_fetcher.get_price(&trigger.mint).await;
        if price > 0.0 {
            entry_price_usd = price;
            break;
        }
        sleep(retry_sleep).await;
        waited += retry_sleep;
    }

    if entry_price_usd <= 0.0 {
        let payload = json!({
            "status": "price_unavailable",
            "status_message": "Could not resolve non-zero entry price in time",
            "completed_at": Utc::now().to_rfc3339(),
            "updated_at": Utc::now().to_rfc3339(),
        });
        let _ = supabase
            .client
            .patch(&patch_url)
            .json(&payload)
            .send()
            .await;
        return Ok(());
    }

    let payload = json!({
        "entry_price_usd": entry_price_usd,
        "status": "tracking",
        "status_message": "Tracking 15m/1h/peak outcomes",
        "updated_at": Utc::now().to_rfc3339(),
    });
    let _ = supabase
        .client
        .patch(&patch_url)
        .json(&payload)
        .send()
        .await;

    let started = Instant::now();
    let mut price_15m_usd: Option<f64> = None;
    let mut price_1h_usd: Option<f64> = None;
    let mut peak_price_usd = entry_price_usd;

    loop {
        let elapsed = started.elapsed().as_secs();
        if elapsed >= tracker_cfg.duration_secs {
            break;
        }

        sleep(Duration::from_secs(tracker_cfg.poll_interval_secs)).await;
        let price = price_fetcher.get_price(&trigger.mint).await;
        if price <= 0.0 {
            continue;
        }

        if price > peak_price_usd {
            peak_price_usd = price;
        }
        let now_elapsed = started.elapsed().as_secs();
        if price_15m_usd.is_none() && now_elapsed >= SHADOW_PRICE_15M_SECS {
            price_15m_usd = Some(price);
        }
        if price_1h_usd.is_none() && now_elapsed >= SHADOW_PRICE_1H_SECS {
            price_1h_usd = Some(price);
        }
    }

    let final_payload = json!({
        "price_15m_usd": price_15m_usd,
        "price_1h_usd": price_1h_usd,
        "peak_price_usd": peak_price_usd,
        "peak_multiplier": peak_price_usd / entry_price_usd,
        "tracked_secs": tracker_cfg.duration_secs as i64,
        "status": "completed",
        "status_message": "Shadow tracking complete",
        "completed_at": Utc::now().to_rfc3339(),
        "updated_at": Utc::now().to_rfc3339(),
    });
    let _ = supabase
        .client
        .patch(&patch_url)
        .json(&final_payload)
        .send()
        .await;

    Ok(())
}

async fn score_pending_launches(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    rpc_client: &Client,
) -> Result<()> {
    let cutoff = Utc::now()
        - chrono::Duration::seconds(cfg.strategy.monitoring.bags_demand_window_secs as i64);
    let cutoff_str = cutoff.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let url = format!(
        "{}/bags_launches?select=mint,creator_wallet,pool_owner_wallet,launch_signature,launch_at&demand_checked_at=is.null&launch_at=lte.{}&order=launch_at.asc&limit={}",
        supabase.base_url,
        cutoff_str,
        MAX_PENDING_EVALS_PER_LOOP,
    );

    let resp = supabase
        .client
        .get(&url)
        .send()
        .await
        .context("bags_monitor: fetch pending launches")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: pending launches query failed: HTTP {} — {}",
            status,
            body
        ));
    }

    let pending: Vec<PendingLaunchRow> = resp
        .json()
        .await
        .context("bags_monitor: decode pending launches")?;
    if pending.is_empty() {
        return Ok(());
    }

    let mut creators_to_refresh: HashSet<String> = HashSet::new();
    for launch in pending {
        let launch_at = chrono::DateTime::parse_from_rfc3339(&launch.launch_at)
            .context("bags_monitor: parse launch_at")?
            .with_timezone(&Utc);
        let demand = score_launch_demand(
            rpc_client,
            &cfg.env.solana_rpc_url,
            &launch,
            launch_at.timestamp(),
            cfg.strategy.monitoring.bags_demand_window_secs as i64,
        )
        .await?;

        let has_real_demand = demand.unique_buyers.len()
            >= cfg.strategy.monitoring.bags_real_demand_min_unique_buyers
            && demand.buy_tx_count >= cfg.strategy.monitoring.bags_real_demand_min_buy_txs
            && demand.buy_volume_sol >= cfg.strategy.monitoring.bags_real_demand_min_buy_volume_sol;

        patch_launch_demand(supabase, &launch.mint, &demand, has_real_demand, cfg).await?;
        patch_shadow_demand(supabase, &launch.mint, &demand, has_real_demand).await?;
        creators_to_refresh.insert(launch.creator_wallet.clone());

        log_event(
            supabase,
            "bags_launch_demand_scored",
            &format!(
                "mint={} creator={} buyers={} buy_txs={} buy_volume_sol={:.4} demand={}",
                launch.mint,
                launch.creator_wallet,
                demand.unique_buyers.len(),
                demand.buy_tx_count,
                demand.buy_volume_sol,
                has_real_demand,
            ),
        )
        .await;
    }

    for creator in creators_to_refresh {
        refresh_creator_stats(cfg, supabase, &creator).await?;
    }

    Ok(())
}

async fn patch_launch_demand(
    supabase: &SupabaseClient,
    mint: &str,
    demand: &DemandStats,
    has_real_demand: bool,
    cfg: &AppConfig,
) -> Result<()> {
    let url = format!("{}/bags_launches?mint=eq.{}", supabase.base_url, mint);
    let payload = json!({
        "demand_checked_at": Utc::now().to_rfc3339(),
        "demand_window_seconds": cfg.strategy.monitoring.bags_demand_window_secs as i64,
        "demand_trade_count": demand.trade_count as i32,
        "demand_buy_tx_count": demand.buy_tx_count as i32,
        "demand_unique_buyers": demand.unique_buyers.len() as i32,
        "demand_buy_volume_sol": demand.buy_volume_sol,
        "demand_peak_single_buy_sol": demand.peak_single_buy_sol,
        "has_real_demand": has_real_demand,
        "updated_at": Utc::now().to_rfc3339(),
    });

    let resp = supabase
        .client
        .patch(&url)
        .json(&payload)
        .send()
        .await
        .context("bags_monitor: patch bags_launches")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: patch bags_launches failed: HTTP {} — {}",
            status,
            body
        ));
    }

    Ok(())
}

async fn score_launch_demand(
    rpc_client: &Client,
    rpc_url: &str,
    launch: &PendingLaunchRow,
    launch_ts: i64,
    demand_window_secs: i64,
) -> Result<DemandStats> {
    let pool_owner = launch.pool_owner_wallet.clone().ok_or_else(|| {
        anyhow!(
            "bags_monitor: missing pool_owner_wallet for mint {}",
            launch.mint
        )
    })?;
    let window_end = launch_ts + demand_window_secs;
    let signatures = fetch_signatures_for_address_window(
        rpc_client,
        rpc_url,
        &launch.mint,
        launch_ts,
        window_end,
    )
    .await?;

    let mut stats = DemandStats::default();
    for sig in signatures {
        if sig.signature == launch.launch_signature {
            continue;
        }

        stats.trade_count += 1;

        let tx = rpc_call(
            rpc_client,
            rpc_url,
            "getTransaction",
            json!([
                sig.signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }
            ]),
        )
        .await?;
        if tx.is_null() {
            continue;
        }

        if let Some((buyers, pool_wsol_delta)) =
            demand_buyers_from_transaction(&tx, &launch.mint, &launch.creator_wallet, &pool_owner)
        {
            stats.buy_tx_count += 1;
            stats.buy_volume_sol += pool_wsol_delta;
            stats.peak_single_buy_sol = stats.peak_single_buy_sol.max(pool_wsol_delta);
            for buyer in buyers {
                stats.unique_buyers.insert(buyer);
            }
        }
    }

    Ok(stats)
}

async fn fetch_signatures_for_address_window(
    rpc_client: &Client,
    rpc_url: &str,
    address: &str,
    launch_ts: i64,
    window_end: i64,
) -> Result<Vec<SignatureInfo>> {
    let mut before: Option<String> = None;
    let mut collected = Vec::new();

    for _ in 0..MAX_SIGNATURE_PAGES {
        let mut cfg = json!({ "limit": MAX_SIGNATURE_PAGE_SIZE });
        if let Some(before_sig) = &before {
            cfg["before"] = json!(before_sig);
        }

        let page_value = rpc_call(
            rpc_client,
            rpc_url,
            "getSignaturesForAddress",
            json!([address, cfg]),
        )
        .await?;
        let page: Vec<SignatureInfo> =
            serde_json::from_value(page_value).context("bags_monitor: decode mint signatures")?;
        if page.is_empty() {
            break;
        }

        let mut saw_older = false;
        for info in &page {
            let Some(block_time) = info.block_time else {
                continue;
            };
            if block_time < launch_ts {
                saw_older = true;
                break;
            }
            if block_time <= window_end {
                collected.push(info.clone());
            }
        }

        if saw_older {
            break;
        }
        before = page.last().map(|row| row.signature.clone());
    }

    collected.reverse();
    Ok(collected)
}

fn demand_buyers_from_transaction(
    tx: &Value,
    mint: &str,
    creator_wallet: &str,
    pool_owner_wallet: &str,
) -> Option<(HashSet<String>, f64)> {
    let pre_balances = token_balances(tx, "preTokenBalances");
    let post_balances = token_balances(tx, "postTokenBalances");
    let pre_map = balance_map(&pre_balances);
    let post_map = balance_map(&post_balances);

    let mut buyers = HashSet::new();
    let mut owners_to_check = HashSet::new();
    for key in pre_map.keys().chain(post_map.keys()) {
        owners_to_check.insert(key.clone());
    }

    for (owner, token_mint) in owners_to_check {
        if token_mint != mint {
            continue;
        }
        if owner == pool_owner_wallet || owner == creator_wallet || owner == BAGS_AUTHORITY {
            continue;
        }

        let pre = *pre_map
            .get(&(owner.clone(), token_mint.clone()))
            .unwrap_or(&0.0);
        let post = *post_map
            .get(&(owner.clone(), token_mint.clone()))
            .unwrap_or(&0.0);
        if post > pre {
            buyers.insert(owner);
        }
    }

    let pool_wsol_pre = *pre_map
        .get(&(pool_owner_wallet.to_string(), WSOL_MINT.to_string()))
        .unwrap_or(&0.0);
    let pool_wsol_post = *post_map
        .get(&(pool_owner_wallet.to_string(), WSOL_MINT.to_string()))
        .unwrap_or(&0.0);
    let pool_wsol_delta = pool_wsol_post - pool_wsol_pre;

    if buyers.is_empty() || pool_wsol_delta <= 0.0 {
        return None;
    }

    Some((buyers, pool_wsol_delta))
}

fn balance_map(balances: &[TokenBalance]) -> HashMap<(String, String), f64> {
    let mut map = HashMap::new();
    for balance in balances {
        if let Some(owner) = &balance.owner {
            map.insert((owner.clone(), balance.mint.clone()), balance.ui_amount);
        }
    }
    map
}

async fn refresh_creator_stats(
    cfg: &AppConfig,
    supabase: &SupabaseClient,
    creator_wallet: &str,
) -> Result<()> {
    let url = format!(
        "{}/bags_launches?select=mint,launch_at,has_real_demand,demand_unique_buyers,demand_buy_volume_sol&creator_wallet=eq.{}&demand_checked_at=not.is.null&order=launch_at.asc",
        supabase.base_url,
        creator_wallet,
    );
    let resp = supabase
        .client
        .get(&url)
        .send()
        .await
        .context("bags_monitor: fetch creator launches")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: creator launches query failed: HTTP {} — {}",
            status,
            body
        ));
    }
    let launches: Vec<CreatorLaunchRow> = resp
        .json()
        .await
        .context("bags_monitor: decode creator launches")?;
    if launches.is_empty() {
        return Ok(());
    }

    let launch_count = launches.len() as i32;
    let demand_launch_count = launches
        .iter()
        .filter(|row| row.has_real_demand.unwrap_or(false))
        .count() as i32;
    let demand_rate = if launch_count > 0 {
        demand_launch_count as f64 / launch_count as f64
    } else {
        0.0
    };
    let avg_unique_buyers = launches
        .iter()
        .map(|row| row.demand_unique_buyers.unwrap_or(0) as f64)
        .sum::<f64>()
        / launch_count as f64;
    let avg_buy_volume_sol = launches
        .iter()
        .map(|row| row.demand_buy_volume_sol.unwrap_or(0.0))
        .sum::<f64>()
        / launch_count as f64;

    let best_launch = launches.iter().max_by(|lhs, rhs| {
        lhs.demand_buy_volume_sol
            .unwrap_or(0.0)
            .partial_cmp(&rhs.demand_buy_volume_sol.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let last_launch_at = launches.last().map(|row| row.launch_at.clone());
    let last_demand_launch_at = launches
        .iter()
        .rev()
        .find(|row| row.has_real_demand.unwrap_or(false))
        .map(|row| row.launch_at.clone());
    let watchworthy = launch_count as usize
        >= cfg.strategy.monitoring.bags_creator_watch_min_launches
        && demand_rate >= cfg.strategy.monitoring.bags_creator_watch_min_demand_rate;

    let row = CreatorStatsRow {
        creator_wallet: creator_wallet.to_string(),
        launch_count,
        demand_launch_count,
        demand_rate,
        avg_unique_buyers,
        avg_buy_volume_sol,
        best_mint: best_launch.map(|row| row.mint.clone()),
        best_buy_volume_sol: best_launch.and_then(|row| row.demand_buy_volume_sol),
        last_launch_at,
        last_demand_launch_at,
        watchworthy,
        updated_at: Utc::now().to_rfc3339(),
    };

    let url = format!(
        "{}/bags_creator_stats?on_conflict=creator_wallet",
        supabase.base_url
    );
    let resp = supabase
        .client
        .post(&url)
        .header("Prefer", "resolution=merge-duplicates,return=minimal")
        .json(&row)
        .send()
        .await
        .context("bags_monitor: upsert bags_creator_stats")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: upsert bags_creator_stats failed: HTTP {} — {}",
            status,
            body
        ));
    }

    debug!(
        creator = creator_wallet,
        launch_count,
        demand_launch_count,
        demand_rate,
        watchworthy,
        "bags_monitor: refreshed creator stats"
    );

    Ok(())
}

async fn rpc_call(client: &Client, rpc_url: &str, method: &str, params: Value) -> Result<Value> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });

    let resp = client
        .post(rpc_url)
        .json(&payload)
        .send()
        .await
        .with_context(|| format!("bags_monitor: RPC {} request failed", method))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "bags_monitor: RPC {} returned HTTP {} — {}",
            method,
            status,
            body
        ));
    }

    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("bags_monitor: RPC {} invalid JSON", method))?;
    if let Some(error) = value.get("error") {
        return Err(anyhow!(
            "bags_monitor: RPC {} returned error {}",
            method,
            error
        ));
    }

    value
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("bags_monitor: RPC {} missing result", method))
}

fn transaction_logs(tx: &Value) -> Vec<String> {
    tx.get("meta")
        .and_then(|meta| meta.get("logMessages"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn transaction_account_keys(tx: &Value) -> Vec<ParsedAccountKey> {
    tx.get("transaction")
        .and_then(|value| value.get("message"))
        .and_then(|value| value.get("accountKeys"))
        .and_then(Value::as_array)
        .map(|keys| {
            keys.iter()
                .filter_map(|key| {
                    if let Some(pubkey) = key.as_str() {
                        return Some(ParsedAccountKey {
                            pubkey: pubkey.to_string(),
                            signer: false,
                        });
                    }

                    Some(ParsedAccountKey {
                        pubkey: key.get("pubkey")?.as_str()?.to_string(),
                        signer: key.get("signer").and_then(Value::as_bool).unwrap_or(false),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn token_balances(tx: &Value, field: &str) -> Vec<TokenBalance> {
    tx.get("meta")
        .and_then(|meta| meta.get(field))
        .and_then(Value::as_array)
        .map(|balances| {
            balances
                .iter()
                .filter_map(|item| {
                    let mint = item.get("mint")?.as_str()?.to_string();
                    let account_index = item.get("accountIndex")?.as_u64()? as usize;
                    let owner = item
                        .get("owner")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    let ui_amount = item
                        .get("uiTokenAmount")
                        .and_then(|amount| amount.get("uiAmountString"))
                        .and_then(Value::as_str)
                        .and_then(|value| value.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    Some(TokenBalance {
                        mint,
                        owner,
                        account_index,
                        ui_amount,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn sum_system_transfers(tx: &Value, source: &str, destination: &str) -> u64 {
    let mut total = 0_u64;
    collect_transfer_instructions(tx, |info| {
        if info.get("source").and_then(Value::as_str) == Some(source)
            && info.get("destination").and_then(Value::as_str) == Some(destination)
        {
            total += info.get("lamports").and_then(Value::as_u64).unwrap_or(0);
        }
    });
    total
}

fn find_transfer_source_to_bags(tx: &Value, destination: &str) -> Option<String> {
    let mut source: Option<String> = None;
    collect_transfer_instructions(tx, |info| {
        if source.is_none() && info.get("destination").and_then(Value::as_str) == Some(destination)
        {
            source = info
                .get("source")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
    });
    source
}

fn collect_transfer_instructions(tx: &Value, mut visit: impl FnMut(&Value)) {
    if let Some(instructions) = tx
        .get("transaction")
        .and_then(|value| value.get("message"))
        .and_then(|value| value.get("instructions"))
        .and_then(Value::as_array)
    {
        for instruction in instructions {
            if let Some(info) = instruction
                .get("parsed")
                .and_then(|parsed| parsed.get("info"))
            {
                if instruction
                    .get("parsed")
                    .and_then(|parsed| parsed.get("type"))
                    .and_then(Value::as_str)
                    == Some("transfer")
                {
                    visit(info);
                }
            }
        }
    }

    if let Some(inner_sets) = tx
        .get("meta")
        .and_then(|value| value.get("innerInstructions"))
        .and_then(Value::as_array)
    {
        for inner in inner_sets {
            if let Some(instructions) = inner.get("instructions").and_then(Value::as_array) {
                for instruction in instructions {
                    if let Some(info) = instruction
                        .get("parsed")
                        .and_then(|parsed| parsed.get("info"))
                    {
                        if instruction
                            .get("parsed")
                            .and_then(|parsed| parsed.get("type"))
                            .and_then(Value::as_str)
                            == Some("transfer")
                        {
                            visit(info);
                        }
                    }
                }
            }
        }
    }
}

async fn log_event(supabase: &SupabaseClient, event_type: &str, message: &str) {
    let url = format!("{}/system_events", supabase.base_url);
    let payload = json!({
        "event_type": event_type,
        "message": message,
    });
    let _ = supabase.client.post(&url).json(&payload).send().await;
}

async fn patch_shadow_demand(
    supabase: &SupabaseClient,
    mint: &str,
    demand: &DemandStats,
    has_real_demand: bool,
) -> Result<()> {
    let url = format!("{}/bags_shadow_entries?mint=eq.{}", supabase.base_url, mint);
    let payload = json!({
        "demand_trade_count": demand.trade_count as i32,
        "demand_buy_tx_count": demand.buy_tx_count as i32,
        "demand_unique_buyers": demand.unique_buyers.len() as i32,
        "demand_buy_volume_sol": demand.buy_volume_sol,
        "demand_peak_single_buy_sol": demand.peak_single_buy_sol,
        "has_real_demand": has_real_demand,
        "updated_at": Utc::now().to_rfc3339(),
    });

    let resp = supabase
        .client
        .patch(&url)
        .json(&payload)
        .send()
        .await
        .context("bags_monitor: patch bags_shadow_entries")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "bags_monitor: patch bags_shadow_entries failed: HTTP {} — {}",
            status,
            body
        ));
    }

    Ok(())
}
