use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::logger::SupabaseClient;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const RPC_TIMEOUT_SECS: u64 = 25;
const MAX_EMITTED_SIGNAL_KEYS: usize = 50_000;
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const ASSOCIATED_TOKEN_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

static WALLET_GRAPH: OnceLock<WalletGraphState> = OnceLock::new();

#[derive(Debug, Clone)]
struct WalletGraphState {
    cfg: Arc<AppConfig>,
    supabase: Arc<SupabaseClient>,
    inner: Arc<RwLock<WalletGraphInner>>,
}

impl WalletGraphState {
    /// Prefer the Helius RPC URL when available so wallet graph polling does
    /// not hit Chainstack's archive/historical signature restrictions.
    fn rpc_url(&self) -> &str {
        self.cfg
            .env
            .helius_rpc_url
            .as_deref()
            .unwrap_or(&self.cfg.env.solana_rpc_url)
    }
}

#[derive(Debug, Default)]
struct WalletGraphInner {
    parents: HashMap<String, ProvenParent>,
    children: HashMap<String, DerivedChild>,
    last_seen_signature_by_parent: HashMap<String, String>,
    emitted_signals: HashSet<String>,
    parent_mint_touches: HashSet<String>,
    child_mint_touches: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Clone)]
struct ProvenParent {
    wallet: String,
    label: String,
    parent_score: f64,
}

#[derive(Debug, Clone)]
struct DerivedChild {
    wallet: String,
    parent_wallet: String,
    parent_label: String,
    parent_score: f64,
    edge_score: f64,
    funded_amount_sol: f64,
    first_seen_at_ms: i64,
    expires_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct ProvenWalletRow {
    wallet: String,
    label: Option<String>,
    parent_score: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct DerivedWalletRow {
    wallet: String,
    parent_wallet: String,
    parent_label: Option<String>,
    parent_score: Option<f64>,
    edge_score: Option<f64>,
    funded_amount_sol: Option<f64>,
    first_seen_at: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SignatureInfo {
    signature: String,
    slot: u64,
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
}

#[derive(Debug, Clone)]
struct FundingTransfer {
    destination: String,
    lamports: u64,
}

#[derive(Debug, Clone)]
struct FundingEdge {
    edge_key: String,
    parent: ProvenParent,
    child_wallet: String,
    funded_at_ms: i64,
    amount_sol: f64,
    signature: String,
    slot: u64,
    child_previous_signature_count: usize,
    edge_score: f64,
}

#[derive(Debug, Clone)]
pub struct PumpfunCreateContext {
    pub mint: String,
    pub creator_wallet: String,
    pub name: String,
    pub symbol: String,
    pub token_age_ms: i64,
}

#[derive(Debug, Clone)]
pub struct PumpfunTradeContext {
    pub mint: String,
    pub trader_wallet: String,
    pub name: String,
    pub symbol: String,
    pub token_age_ms: i64,
    pub is_buy: bool,
    pub amount_sol: f64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub unique_buyers: usize,
    pub buy_volume_sol: f64,
    pub sell_volume_sol: f64,
    pub largest_buy_sol: f64,
    pub buy_pressure_pct: f64,
    pub bc_progress_pct: Option<f64>,
    pub virtual_sol_reserves: f64,
    pub virtual_token_reserves: f64,
    pub market_cap_sol: f64,
}

#[derive(Debug, Clone)]
pub struct PumpfunGraduationContext {
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub token_age_ms: i64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub unique_buyers: usize,
    pub buy_volume_sol: f64,
    pub sell_volume_sol: f64,
    pub largest_buy_sol: f64,
    pub buy_pressure_pct: f64,
    pub bc_progress_pct: Option<f64>,
    pub virtual_sol_reserves: f64,
    pub virtual_token_reserves: f64,
    pub market_cap_sol: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct WalletLinkedSignalRow {
    id: i64,
    mint: String,
    signal_type: Option<String>,
    signal_at: Option<String>,
    wallet_graph_score: Option<f64>,
    flow_score: Option<f64>,
    parent_wallet: Option<String>,
    child_wallet: Option<String>,
}

pub fn start(cfg: Arc<AppConfig>, supabase: Arc<SupabaseClient>) {
    if !cfg.strategy.detection.proven_wallet_graph_enabled {
        info!("Proven-wallet graph shadow monitor disabled");
        return;
    }

    let state = WalletGraphState {
        cfg,
        supabase,
        inner: Arc::new(RwLock::new(WalletGraphInner::default())),
    };

    if WALLET_GRAPH.set(state.clone()).is_err() {
        warn!("Proven-wallet graph shadow monitor already initialized");
        return;
    }

    tokio::spawn(async move {
        run_loop(state).await;
    });

    info!("Proven-wallet graph shadow monitor task spawned");
}

pub async fn observe_pumpfun_create(ctx: PumpfunCreateContext) {
    let Some(state) = WALLET_GRAPH.get().cloned() else {
        return;
    };

    let mut pending: Vec<(String, Value)> = Vec::new();
    let mut child_activity: Option<(String, String, String)> = None;

    {
        let now_ms = Utc::now().timestamp_millis();
        let mut inner = state.inner.write().await;
        prune_expired_children(&mut inner, now_ms);

        if inner.emitted_signals.len() > MAX_EMITTED_SIGNAL_KEYS {
            inner.emitted_signals.clear();
        }

        if let Some(parent) = inner.parents.get(&ctx.creator_wallet).cloned() {
            inner.parent_mint_touches.insert(parent_mint_key(&parent.wallet, &ctx.mint));
            maybe_queue_create_signal(
                &state,
                &mut inner,
                &mut pending,
                "PROVEN_PARENT_CREATED_PUMP_MINT",
                &ctx,
                &parent,
                None,
                None,
                8.0,
            );
        }

        if let Some(child) = inner.children.get(&ctx.creator_wallet).cloned() {
            let parent = ProvenParent {
                wallet: child.parent_wallet.clone(),
                label: child.parent_label.clone(),
                parent_score: child.parent_score,
            };
            inner
                .child_mint_touches
                .entry(parent_mint_key(&child.parent_wallet, &ctx.mint))
                .or_default()
                .insert(child.wallet.clone());
            maybe_queue_create_signal(
                &state,
                &mut inner,
                &mut pending,
                "PROVEN_CHILD_CREATED_PUMP_MINT",
                &ctx,
                &parent,
                Some(&child),
                None,
                14.0,
            );
            child_activity = Some((child.wallet.clone(), ctx.mint.clone(), "create".to_string()));
        }
    }

    flush_signal_writes(state.clone(), pending).await;
    if let Some((wallet, mint, action)) = child_activity {
        write_child_activity(&state, &wallet, &mint, &action).await;
    }
}

pub async fn observe_pumpfun_trade(ctx: PumpfunTradeContext) {
    if !ctx.is_buy {
        return;
    }

    let Some(state) = WALLET_GRAPH.get().cloned() else {
        return;
    };

    let mut pending: Vec<(String, Value)> = Vec::new();
    let mut child_activity: Option<(String, String, String)> = None;

    {
        let now_ms = Utc::now().timestamp_millis();
        let mut inner = state.inner.write().await;
        prune_expired_children(&mut inner, now_ms);

        if inner.emitted_signals.len() > MAX_EMITTED_SIGNAL_KEYS {
            inner.emitted_signals.clear();
        }

        if let Some(parent) = inner.parents.get(&ctx.trader_wallet).cloned() {
            inner.parent_mint_touches.insert(parent_mint_key(&parent.wallet, &ctx.mint));
            maybe_queue_trade_signal(
                &state,
                &mut inner,
                &mut pending,
                "PROVEN_PARENT_BOUGHT_NEW_MINT",
                &ctx,
                &parent,
                None,
                None,
                10.0,
            );

            let cluster_key = parent_mint_key(&parent.wallet, &ctx.mint);
            if inner.child_mint_touches.contains_key(&cluster_key) {
                maybe_queue_trade_signal(
                    &state,
                    &mut inner,
                    &mut pending,
                    "PARENT_CHILD_SAME_MINT",
                    &ctx,
                    &parent,
                    None,
                    None,
                    18.0,
                );
            }
        }

        if let Some(child) = inner.children.get(&ctx.trader_wallet).cloned() {
            let parent = ProvenParent {
                wallet: child.parent_wallet.clone(),
                label: child.parent_label.clone(),
                parent_score: child.parent_score,
            };
            let cluster_key = parent_mint_key(&child.parent_wallet, &ctx.mint);
            let child_count = {
                let children = inner.child_mint_touches.entry(cluster_key.clone()).or_default();
                children.insert(child.wallet.clone());
                children.len()
            };

            maybe_queue_trade_signal(
                &state,
                &mut inner,
                &mut pending,
                "PROVEN_CHILD_BOUGHT_NEW_MINT",
                &ctx,
                &parent,
                Some(&child),
                Some(child_count),
                16.0,
            );

            if child_count >= 2 {
                maybe_queue_trade_signal(
                    &state,
                    &mut inner,
                    &mut pending,
                    "MULTI_CHILD_SAME_MINT",
                    &ctx,
                    &parent,
                    None,
                    Some(child_count),
                    24.0,
                );
            }

            if inner.parent_mint_touches.contains(&cluster_key) {
                maybe_queue_trade_signal(
                    &state,
                    &mut inner,
                    &mut pending,
                    "PARENT_CHILD_SAME_MINT",
                    &ctx,
                    &parent,
                    Some(&child),
                    Some(child_count),
                    22.0,
                );
            }

            child_activity = Some((child.wallet.clone(), ctx.mint.clone(), "buy".to_string()));
        }
    }

    flush_signal_writes(state.clone(), pending).await;
    if let Some((wallet, mint, action)) = child_activity {
        write_child_activity(&state, &wallet, &mint, &action).await;
    }
}

pub async fn observe_pumpfun_graduation(ctx: PumpfunGraduationContext) {
    let Some(state) = WALLET_GRAPH.get().cloned() else {
        return;
    };

    tokio::spawn(async move {
        if let Err(e) = write_graduation_outcomes(&state, &ctx).await {
            warn!(mint = %ctx.mint, error = %e, "wallet_graph: graduation outcome write failed");
        }
    });
}

async fn run_loop(state: WalletGraphState) {
    if !ensure_tables_ready(&state.supabase).await {
        warn!("Proven-wallet graph tables are missing; run migration 031 before deploying this lane");
        return;
    }

    let rpc_client = match Client::builder()
        .timeout(Duration::from_secs(RPC_TIMEOUT_SECS))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            warn!(error = %e, "wallet_graph: failed to build RPC client");
            return;
        }
    };

    if let Err(e) = reload_parent_roster(&state).await {
        warn!(error = %e, "wallet_graph: initial parent roster reload failed");
    }
    if let Err(e) = load_recent_children(&state).await {
        warn!(error = %e, "wallet_graph: recent child load failed");
    }

    let mut last_parent_reload_ms = Utc::now().timestamp_millis();
    let poll_interval = Duration::from_secs(
        state
            .cfg
            .strategy
            .detection
            .proven_wallet_graph_poll_interval_secs
            .max(5),
    );
    let reload_interval_ms =
        (state.cfg.strategy.detection.proven_wallet_graph_parent_reload_secs.max(60) as i64)
            * 1_000;

    loop {
        let now_ms = Utc::now().timestamp_millis();
        if now_ms.saturating_sub(last_parent_reload_ms) >= reload_interval_ms {
            if let Err(e) = reload_parent_roster(&state).await {
                warn!(error = %e, "wallet_graph: parent roster reload failed");
            } else {
                last_parent_reload_ms = now_ms;
            }
        }

        if let Err(e) = poll_parent_funding_once(&state, &rpc_client).await {
            warn!(error = %e, "wallet_graph: parent funding poll failed");
        }

        sleep(poll_interval).await;
    }
}

async fn ensure_tables_ready(supabase: &SupabaseClient) -> bool {
    ensure_table_ready(supabase, "proven_wallets", "wallet").await
        && ensure_table_ready(supabase, "wallet_funding_edges", "id").await
        && ensure_table_ready(supabase, "derived_wallets", "wallet").await
        && ensure_table_ready(supabase, "wallet_linked_mint_signals", "id").await
        && ensure_table_ready(supabase, "wallet_graph_outcomes", "id").await
}

async fn ensure_table_ready(supabase: &SupabaseClient, table: &str, column: &str) -> bool {
    let url = format!("{}/{}?select={}&limit=1", supabase.base_url, table, column);
    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => true,
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!(table, body = %body, "wallet_graph: table missing or inaccessible");
            false
        }
        Err(e) => {
            warn!(table, error = %e, "wallet_graph: table readiness check failed");
            false
        }
    }
}

async fn reload_parent_roster(state: &WalletGraphState) -> Result<usize> {
    let det = &state.cfg.strategy.detection;
    let url = format!(
        "{}/proven_wallets?select=wallet,label,parent_score&status=eq.active&parent_score=gte.{}&order=parent_score.desc&limit={}",
        state.supabase.base_url,
        det.proven_wallet_graph_min_parent_score,
        det.proven_wallet_graph_max_parents.max(1),
    );

    let resp = state
        .supabase
        .client
        .get(&url)
        .send()
        .await
        .context("wallet_graph: query proven_wallets")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "wallet_graph: proven_wallets query failed: HTTP {} — {}",
            status,
            body
        ));
    }

    let rows: Vec<ProvenWalletRow> = resp
        .json()
        .await
        .context("wallet_graph: decode proven_wallets")?;

    let mut parents = HashMap::new();
    for row in rows {
        if Pubkey::from_str(&row.wallet).is_err() {
            warn!(wallet = %row.wallet, "wallet_graph: skipping invalid parent wallet");
            continue;
        }
        let parent = ProvenParent {
            wallet: row.wallet.clone(),
            label: row.label.unwrap_or_else(|| "UNKNOWN_PROVEN".to_string()),
            parent_score: row.parent_score.unwrap_or(60.0),
        };
        parents.insert(parent.wallet.clone(), parent);
    }

    let count = parents.len();
    {
        let mut inner = state.inner.write().await;
        inner.parents = parents;
    }

    if count == 0 {
        warn!("wallet_graph: no active proven parent wallets loaded; seed proven_wallets after migration 031");
    } else {
        info!(parent_count = count, "wallet_graph: active proven parent roster loaded");
    }

    Ok(count)
}

async fn load_recent_children(state: &WalletGraphState) -> Result<usize> {
    let cutoff = Utc::now()
        - chrono::Duration::hours(state.cfg.strategy.detection.proven_wallet_graph_child_watch_hours.max(1));
    let cutoff_str = cutoff.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let url = format!(
        "{}/derived_wallets?select=wallet,parent_wallet,parent_label,parent_score,edge_score,funded_amount_sol,first_seen_at&activity_status=in.(funded,active)&first_seen_at=gte.{}&limit=5000",
        state.supabase.base_url,
        cutoff_str,
    );

    let resp = state
        .supabase
        .client
        .get(&url)
        .send()
        .await
        .context("wallet_graph: query recent derived_wallets")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "wallet_graph: derived_wallets query failed: HTTP {} — {}",
            status,
            body
        ));
    }

    let rows: Vec<DerivedWalletRow> = resp
        .json()
        .await
        .context("wallet_graph: decode recent derived_wallets")?;

    let watch_ms = state.cfg.strategy.detection.proven_wallet_graph_child_watch_hours.max(1)
        * 60
        * 60
        * 1_000;
    let mut children = HashMap::new();
    for row in rows {
        if Pubkey::from_str(&row.wallet).is_err() || Pubkey::from_str(&row.parent_wallet).is_err() {
            continue;
        }
        let first_seen_at_ms = parse_rfc3339_ms(&row.first_seen_at)
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        let child = DerivedChild {
            wallet: row.wallet.clone(),
            parent_wallet: row.parent_wallet.clone(),
            parent_label: row.parent_label.unwrap_or_else(|| "UNKNOWN_PROVEN".to_string()),
            parent_score: row.parent_score.unwrap_or(60.0),
            edge_score: row.edge_score.unwrap_or(50.0),
            funded_amount_sol: row.funded_amount_sol.unwrap_or(0.0),
            first_seen_at_ms,
            expires_at_ms: first_seen_at_ms.saturating_add(watch_ms),
        };
        children.insert(child.wallet.clone(), child);
    }

    let count = children.len();
    {
        let mut inner = state.inner.write().await;
        inner.children.extend(children);
    }
    info!(child_count = count, "wallet_graph: recent derived children loaded");
    Ok(count)
}

async fn poll_parent_funding_once(state: &WalletGraphState, rpc_client: &Client) -> Result<()> {
    let poll_items: Vec<(ProvenParent, Option<String>)> = {
        let inner = state.inner.read().await;
        inner
            .parents
            .values()
            .cloned()
            .map(|parent| {
                let last_seen = inner.last_seen_signature_by_parent.get(&parent.wallet).cloned();
                (parent, last_seen)
            })
            .collect()
    };

    if poll_items.is_empty() {
        return Ok(());
    }

    for (parent, last_seen) in poll_items {
        let signatures = fetch_new_signatures_for_parent(
            rpc_client,
            state.rpc_url(),
            &parent.wallet,
            last_seen.as_deref(),
            state.cfg.strategy.detection.proven_wallet_graph_signature_limit.max(1),
        )
        .await?;

        for sig in &signatures {
            if let Err(e) = process_parent_signature(state, rpc_client, &parent, sig).await {
                warn!(parent = %parent.wallet, signature = %sig.signature, error = %e, "wallet_graph: parent signature processing failed");
            }
        }

        if let Some(latest) = signatures.last() {
            let mut inner = state.inner.write().await;
            inner
                .last_seen_signature_by_parent
                .insert(parent.wallet.clone(), latest.signature.clone());
        }
    }

    Ok(())
}

async fn fetch_new_signatures_for_parent(
    rpc_client: &Client,
    rpc_url: &str,
    parent_wallet: &str,
    last_seen_signature: Option<&str>,
    limit: usize,
) -> Result<Vec<SignatureInfo>> {
    let value = rpc_call(
        rpc_client,
        rpc_url,
        "getSignaturesForAddress",
        json!([parent_wallet, { "limit": limit.min(100) }]),
    )
    .await?;
    let page: Vec<SignatureInfo> = serde_json::from_value(value)
        .context("wallet_graph: decode getSignaturesForAddress")?;

    let mut collected = Vec::new();
    for info in page {
        if last_seen_signature == Some(info.signature.as_str()) {
            break;
        }
        collected.push(info);
    }
    collected.reverse();
    Ok(collected)
}

async fn process_parent_signature(
    state: &WalletGraphState,
    rpc_client: &Client,
    parent: &ProvenParent,
    sig: &SignatureInfo,
) -> Result<()> {
    let tx = rpc_call(
        rpc_client,
        state.rpc_url(),
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
        return Ok(());
    }

    let transfers = system_transfers_from_parent(&tx, &parent.wallet);
    if transfers.is_empty() {
        return Ok(());
    }

    for transfer in transfers {
        let amount_sol = transfer.lamports as f64 / LAMPORTS_PER_SOL;
        if amount_sol < state.cfg.strategy.detection.proven_wallet_graph_min_funding_sol {
            continue;
        }
        if is_ignored_destination(&transfer.destination) || transfer.destination == parent.wallet {
            continue;
        }

        let prev_count = count_previous_signatures(
            rpc_client,
            state.rpc_url(),
            &transfer.destination,
            &sig.signature,
            sig.slot,
            state
                .cfg
                .strategy
                .detection
                .proven_wallet_graph_max_child_prev_sigs
                + 3,
        )
        .await
        .unwrap_or(usize::MAX);

        if prev_count > state.cfg.strategy.detection.proven_wallet_graph_max_child_prev_sigs {
            debug!(
                parent = %parent.wallet,
                child = %transfer.destination,
                prev_count,
                amount_sol = format!("{:.4}", amount_sol),
                "wallet_graph: funded wallet is not fresh enough"
            );
            continue;
        }

        let block_time = tx
            .get("blockTime")
            .and_then(Value::as_i64)
            .or(sig.block_time)
            .unwrap_or_else(|| Utc::now().timestamp());
        let funded_at_ms = block_time.saturating_mul(1_000);
        let edge_score = score_funding_edge(parent.parent_score, prev_count, amount_sol);
        let edge = FundingEdge {
            edge_key: format!("{}:{}:{}", parent.wallet, transfer.destination, sig.signature),
            parent: parent.clone(),
            child_wallet: transfer.destination.clone(),
            funded_at_ms,
            amount_sol,
            signature: sig.signature.clone(),
            slot: sig.slot,
            child_previous_signature_count: prev_count,
            edge_score,
        };

        cache_child_from_edge(state, &edge).await;
        write_funding_edge_and_child(state, &edge).await;

        info!(
            parent = %parent.wallet,
            child = %transfer.destination,
            amount_sol = format!("{:.4}", amount_sol),
            prev_count,
            edge_score = format!("{:.1}", edge_score),
            "🧬 Proven parent funded fresh child wallet"
        );
    }

    Ok(())
}

async fn count_previous_signatures(
    rpc_client: &Client,
    rpc_url: &str,
    wallet: &str,
    current_signature: &str,
    funding_slot: u64,
    limit: usize,
) -> Result<usize> {
    let value = rpc_call(
        rpc_client,
        rpc_url,
        "getSignaturesForAddress",
        json!([wallet, { "limit": limit.min(20).max(1) }]),
    )
    .await?;
    let page: Vec<SignatureInfo> = serde_json::from_value(value)
        .context("wallet_graph: decode child getSignaturesForAddress")?;
    Ok(page
        .iter()
        .filter(|info| info.signature != current_signature && info.slot < funding_slot)
        .count())
}

async fn cache_child_from_edge(state: &WalletGraphState, edge: &FundingEdge) {
    let watch_ms = state.cfg.strategy.detection.proven_wallet_graph_child_watch_hours.max(1)
        * 60
        * 60
        * 1_000;
    let child = DerivedChild {
        wallet: edge.child_wallet.clone(),
        parent_wallet: edge.parent.wallet.clone(),
        parent_label: edge.parent.label.clone(),
        parent_score: edge.parent.parent_score,
        edge_score: edge.edge_score,
        funded_amount_sol: edge.amount_sol,
        first_seen_at_ms: edge.funded_at_ms,
        expires_at_ms: edge.funded_at_ms.saturating_add(watch_ms),
    };

    let mut inner = state.inner.write().await;
    inner.children.insert(child.wallet.clone(), child);
}

async fn write_funding_edge_and_child(state: &WalletGraphState, edge: &FundingEdge) {
    let funded_at = ms_to_rfc3339(edge.funded_at_ms);
    let edge_payload = json!({
        "edge_key": edge.edge_key,
        "parent_wallet": edge.parent.wallet,
        "child_wallet": edge.child_wallet,
        "parent_label": edge.parent.label,
        "parent_score": edge.parent.parent_score,
        "funded_at": funded_at,
        "amount_sol": edge.amount_sol,
        "tx_signature": edge.signature,
        "slot": edge.slot as i64,
        "child_previous_signature_count": edge.child_previous_signature_count as i32,
        "is_fresh_child": true,
        "edge_type": "PROVEN_PARENT_FUNDED_FRESH_CHILD",
        "edge_score": edge.edge_score,
        "source": "solana_rpc",
        "is_shadow": true,
    });
    let edge_url = format!(
        "{}/wallet_funding_edges?on_conflict=edge_key",
        state.supabase.base_url
    );
    match state
        .supabase
        .client
        .post(&edge_url)
        .header("Prefer", "resolution=ignore-duplicates,return=minimal")
        .json(&edge_payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, body = %body, "wallet_graph: wallet_funding_edges write failed");
        }
        Err(e) => warn!(error = %e, "wallet_graph: wallet_funding_edges write error"),
    }

    let child_payload = json!({
        "wallet": edge.child_wallet,
        "parent_wallet": edge.parent.wallet,
        "parent_label": edge.parent.label,
        "parent_score": edge.parent.parent_score,
        "first_edge_key": edge.edge_key,
        "first_seen_at": funded_at,
        "funded_amount_sol": edge.amount_sol,
        "child_previous_signature_count": edge.child_previous_signature_count as i32,
        "edge_score": edge.edge_score,
        "activity_status": "funded",
        "last_seen_at": Utc::now().to_rfc3339(),
        "updated_at": Utc::now().to_rfc3339(),
    });
    let child_url = format!(
        "{}/derived_wallets?on_conflict=wallet",
        state.supabase.base_url
    );
    match state
        .supabase
        .client
        .post(&child_url)
        .header("Prefer", "resolution=merge-duplicates,return=minimal")
        .json(&child_payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, body = %body, "wallet_graph: derived_wallets upsert failed");
        }
        Err(e) => warn!(error = %e, "wallet_graph: derived_wallets upsert error"),
    }
}

fn maybe_queue_create_signal(
    state: &WalletGraphState,
    inner: &mut WalletGraphInner,
    pending: &mut Vec<(String, Value)>,
    signal_type: &str,
    ctx: &PumpfunCreateContext,
    parent: &ProvenParent,
    child: Option<&DerivedChild>,
    child_count: Option<usize>,
    bonus: f64,
) {
    let child_wallet = child.map(|c| c.wallet.clone());
    let edge_score = child.map(|c| c.edge_score);
    let dedupe_key = signal_dedupe_key(signal_type, &ctx.mint, &parent.wallet, child_wallet.as_deref());
    if !inner.emitted_signals.insert(dedupe_key.clone()) {
        return;
    }

    let wallet_graph_score = score_wallet_graph(parent.parent_score, edge_score, None, bonus);
    let payload = json!({
        "dedupe_key": dedupe_key,
        "mint": ctx.mint,
        "name": ctx.name,
        "symbol": ctx.symbol,
        "parent_wallet": parent.wallet,
        "child_wallet": child_wallet,
        "parent_label": parent.label,
        "parent_score": parent.parent_score,
        "edge_score": edge_score,
        "signal_type": signal_type,
        "signal_at": Utc::now().to_rfc3339(),
        "token_age_ms": ctx.token_age_ms,
        "wallet_graph_score": wallet_graph_score,
        "child_count": child_count.map(|v| v as i32),
        "trigger_reason": "proven_wallet_graph_create_v1",
        "raw_context": json!({
            "creator_wallet": ctx.creator_wallet,
            "event": "pumpfun_create"
        }),
        "strategy_version": strategy_version(state),
        "is_shadow": true,
    });
    pending.push((ctx.mint.clone(), payload));
}

fn maybe_queue_trade_signal(
    state: &WalletGraphState,
    inner: &mut WalletGraphInner,
    pending: &mut Vec<(String, Value)>,
    signal_type: &str,
    ctx: &PumpfunTradeContext,
    parent: &ProvenParent,
    child: Option<&DerivedChild>,
    child_count: Option<usize>,
    bonus: f64,
) {
    let child_wallet = child.map(|c| c.wallet.clone());
    let edge_score = child.map(|c| c.edge_score);
    let dedupe_key = signal_dedupe_key(signal_type, &ctx.mint, &parent.wallet, child_wallet.as_deref());
    if !inner.emitted_signals.insert(dedupe_key.clone()) {
        return;
    }

    let flow_score = score_flow_snapshot(ctx);
    let wallet_graph_score = score_wallet_graph(parent.parent_score, edge_score, flow_score, bonus);
    let payload = json!({
        "dedupe_key": dedupe_key,
        "mint": ctx.mint,
        "name": ctx.name,
        "symbol": ctx.symbol,
        "parent_wallet": parent.wallet,
        "child_wallet": child_wallet,
        "parent_label": parent.label,
        "parent_score": parent.parent_score,
        "edge_score": edge_score,
        "signal_type": signal_type,
        "signal_at": Utc::now().to_rfc3339(),
        "token_age_ms": ctx.token_age_ms,
        "amount_sol": ctx.amount_sol,
        "wallet_graph_score": wallet_graph_score,
        "flow_score": flow_score,
        "buy_count": ctx.buy_count as i64,
        "sell_count": ctx.sell_count as i64,
        "unique_buyers": ctx.unique_buyers as i64,
        "buy_volume_sol": ctx.buy_volume_sol,
        "sell_volume_sol": ctx.sell_volume_sol,
        "largest_buy_sol": ctx.largest_buy_sol,
        "buy_pressure_pct": ctx.buy_pressure_pct,
        "bc_progress_pct": ctx.bc_progress_pct,
        "virtual_sol_reserves": ctx.virtual_sol_reserves,
        "virtual_token_reserves": ctx.virtual_token_reserves,
        "market_cap_sol": ctx.market_cap_sol,
        "child_count": child_count.map(|v| v as i32),
        "trigger_reason": "proven_wallet_graph_trade_v1",
        "raw_context": json!({
            "trader_wallet": ctx.trader_wallet,
            "event": "pumpfun_buy"
        }),
        "strategy_version": strategy_version(state),
        "is_shadow": true,
    });
    pending.push((ctx.mint.clone(), payload));
}

async fn flush_signal_writes(state: WalletGraphState, pending: Vec<(String, Value)>) {
    for (mint, payload) in pending {
        let state_c = state.clone();
        tokio::spawn(async move {
            write_wallet_linked_signal(&state_c, &mint, &payload).await;
        });
    }
}

async fn write_wallet_linked_signal(state: &WalletGraphState, mint: &str, payload: &Value) {
    let signal_type = payload
        .get("signal_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let url = format!(
        "{}/wallet_linked_mint_signals?on_conflict=dedupe_key",
        state.supabase.base_url
    );
    match state
        .supabase
        .client
        .post(&url)
        .header("Prefer", "resolution=ignore-duplicates,return=minimal")
        .json(payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            info!(mint = %mint, signal_type = %signal_type, "Wallet-linked mint shadow signal written");
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, signal_type = %signal_type, http_status = %status, "wallet_linked_mint_signals write failed: {}", body);
        }
        Err(e) => {
            warn!(mint = %mint, signal_type = %signal_type, "wallet_linked_mint_signals write error: {}", e);
        }
    }
}

async fn write_child_activity(state: &WalletGraphState, child_wallet: &str, mint: &str, action: &str) {
    let url = format!(
        "{}/derived_wallets?wallet=eq.{}",
        state.supabase.base_url,
        child_wallet
    );
    let payload = json!({
        "activity_status": "active",
        "first_pump_mint": mint,
        "first_pump_action": action,
        "first_pump_at": Utc::now().to_rfc3339(),
        "last_seen_at": Utc::now().to_rfc3339(),
        "updated_at": Utc::now().to_rfc3339(),
    });
    match state.supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(child = %child_wallet, http_status = %status, "wallet_graph: child activity patch failed: {}", body);
        }
        Err(e) => warn!(child = %child_wallet, "wallet_graph: child activity patch error: {}", e),
    }
}

async fn write_graduation_outcomes(
    state: &WalletGraphState,
    ctx: &PumpfunGraduationContext,
) -> Result<()> {
    let signals = fetch_wallet_linked_signals_for_mint(state, &ctx.mint).await?;
    if signals.is_empty() {
        debug!(mint = %ctx.mint, "wallet_graph: graduation had no wallet-linked signals");
        return Ok(());
    }

    for signal in signals {
        write_graduation_outcome_for_signal(state, ctx, &signal).await?;
    }

    info!(
        mint = %ctx.mint,
        "🎓 Wallet-linked mint graduated — outcome rows updated"
    );
    Ok(())
}

async fn fetch_wallet_linked_signals_for_mint(
    state: &WalletGraphState,
    mint: &str,
) -> Result<Vec<WalletLinkedSignalRow>> {
    let url = format!(
        "{}/wallet_linked_mint_signals?select=id,mint,signal_type,signal_at,wallet_graph_score,flow_score,parent_wallet,child_wallet&mint=eq.{}&is_shadow=eq.true&order=signal_at.asc&limit=100",
        state.supabase.base_url,
        mint,
    );

    let resp = state
        .supabase
        .client
        .get(&url)
        .send()
        .await
        .context("wallet_graph: query wallet-linked signals for graduation")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "wallet_graph: wallet-linked signal graduation query failed: HTTP {} — {}",
            status,
            body
        ));
    }

    resp.json()
        .await
        .context("wallet_graph: decode wallet-linked graduation signals")
}

async fn write_graduation_outcome_for_signal(
    state: &WalletGraphState,
    ctx: &PumpfunGraduationContext,
    signal: &WalletLinkedSignalRow,
) -> Result<()> {
    let existing_url = format!(
        "{}/wallet_graph_outcomes?select=id&signal_id=eq.{}&limit=1",
        state.supabase.base_url,
        signal.id,
    );
    let existing_resp = state
        .supabase
        .client
        .get(&existing_url)
        .send()
        .await
        .context("wallet_graph: check existing graduation outcome")?;
    if !existing_resp.status().is_success() {
        let status = existing_resp.status();
        let body = existing_resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "wallet_graph: outcome existence query failed: HTTP {} — {}",
            status,
            body
        ));
    }
    let existing: Vec<Value> = existing_resp
        .json()
        .await
        .context("wallet_graph: decode existing outcomes")?;

    let signal_at = signal.signal_at.clone().unwrap_or_else(|| Utc::now().to_rfc3339());
    let signal_type = signal
        .signal_type
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let notes = format!(
        "Graduated after wallet-linked signal. name={} symbol={} token_age_ms={} buy_count={} sell_count={} unique_buyers={} buy_vol_sol={:.4} sell_vol_sol={:.4} largest_buy_sol={:.4} buy_pressure_pct={:.1} bc_progress_pct={} virtual_sol_reserves={:.4} virtual_token_reserves={:.4} market_cap_sol={:.4} wallet_graph_score={} flow_score={} parent={} child={}",
        ctx.name,
        ctx.symbol,
        ctx.token_age_ms,
        ctx.buy_count,
        ctx.sell_count,
        ctx.unique_buyers,
        ctx.buy_volume_sol,
        ctx.sell_volume_sol,
        ctx.largest_buy_sol,
        ctx.buy_pressure_pct,
        ctx.bc_progress_pct
            .map(|v| format!("{:.1}", v))
            .unwrap_or_else(|| "null".to_string()),
        ctx.virtual_sol_reserves,
        ctx.virtual_token_reserves,
        ctx.market_cap_sol,
        signal
            .wallet_graph_score
            .map(|v| format!("{:.1}", v))
            .unwrap_or_else(|| "null".to_string()),
        signal
            .flow_score
            .map(|v| format!("{:.1}", v))
            .unwrap_or_else(|| "null".to_string()),
        signal.parent_wallet.as_deref().unwrap_or("-"),
        signal.child_wallet.as_deref().unwrap_or("-"),
    );
    let payload = json!({
        "signal_id": signal.id,
        "mint": signal.mint,
        "signal_type": signal_type,
        "signal_at": signal_at,
        "checked_at": Utc::now().to_rfc3339(),
        "graduated": true,
        "rugged": false,
        "outcome_label": "graduated_after_wallet_signal",
        "notes": notes,
    });

    if let Some(existing_id) = existing
        .first()
        .and_then(|row| row.get("id"))
        .and_then(Value::as_i64)
    {
        let patch_url = format!(
            "{}/wallet_graph_outcomes?id=eq.{}",
            state.supabase.base_url,
            existing_id,
        );
        let resp = state
            .supabase
            .client
            .patch(&patch_url)
            .json(&payload)
            .send()
            .await
            .context("wallet_graph: patch graduation outcome")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "wallet_graph: patch graduation outcome failed: HTTP {} — {}",
                status,
                body
            ));
        }
        return Ok(());
    }

    let insert_url = format!("{}/wallet_graph_outcomes", state.supabase.base_url);
    let resp = state
        .supabase
        .client
        .post(&insert_url)
        .json(&payload)
        .send()
        .await
        .context("wallet_graph: insert graduation outcome")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "wallet_graph: insert graduation outcome failed: HTTP {} — {}",
            status,
            body
        ));
    }

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
        .with_context(|| format!("wallet_graph: RPC {} request failed", method))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "wallet_graph: RPC {} returned HTTP {} — {}",
            method,
            status,
            body
        ));
    }

    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("wallet_graph: RPC {} invalid JSON", method))?;
    if let Some(error) = value.get("error") {
        return Err(anyhow!("wallet_graph: RPC {} returned error {}", method, error));
    }

    value
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("wallet_graph: RPC {} missing result", method))
}

fn system_transfers_from_parent(tx: &Value, parent_wallet: &str) -> Vec<FundingTransfer> {
    let mut transfers = Vec::new();
    collect_transfer_instructions(tx, |info| {
        if info.get("source").and_then(Value::as_str) == Some(parent_wallet) {
            let Some(destination) = info.get("destination").and_then(Value::as_str) else {
                return;
            };
            let lamports = info
                .get("lamports")
                .and_then(Value::as_u64)
                .or_else(|| info.get("amount").and_then(Value::as_str).and_then(|v| v.parse::<u64>().ok()))
                .unwrap_or(0);
            if lamports > 0 {
                transfers.push(FundingTransfer {
                    destination: destination.to_string(),
                    lamports,
                });
            }
        }
    });
    transfers
}

fn collect_transfer_instructions(tx: &Value, mut visit: impl FnMut(&Value)) {
    if let Some(instructions) = tx
        .get("transaction")
        .and_then(|value| value.get("message"))
        .and_then(|value| value.get("instructions"))
        .and_then(Value::as_array)
    {
        for instruction in instructions {
            visit_parsed_transfer(instruction, &mut visit);
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
                    visit_parsed_transfer(instruction, &mut visit);
                }
            }
        }
    }
}

fn visit_parsed_transfer(instruction: &Value, visit: &mut impl FnMut(&Value)) {
    let parsed = instruction.get("parsed");
    let Some(info) = parsed.and_then(|parsed| parsed.get("info")) else {
        return;
    };
    let parsed_type = parsed
        .and_then(|parsed| parsed.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if parsed_type == "transfer" || parsed_type == "transferChecked" {
        visit(info);
    }
}

fn is_ignored_destination(destination: &str) -> bool {
    if Pubkey::from_str(destination).is_err() {
        return true;
    }
    matches!(
        destination,
        SYSTEM_PROGRAM | TOKEN_PROGRAM | TOKEN_2022_PROGRAM | ASSOCIATED_TOKEN_PROGRAM
    )
}

fn score_funding_edge(parent_score: f64, child_previous_signature_count: usize, amount_sol: f64) -> f64 {
    let mut score = (parent_score * 0.45).clamp(0.0, 45.0);

    score += match child_previous_signature_count {
        0 => 28.0,
        1 => 23.0,
        2..=3 => 16.0,
        _ => 0.0,
    };

    score += if amount_sol >= 5.0 {
        18.0
    } else if amount_sol >= 1.0 {
        13.0
    } else if amount_sol >= 0.25 {
        8.0
    } else {
        4.0
    };

    score.clamp(0.0, 100.0)
}

fn score_wallet_graph(
    parent_score: f64,
    edge_score: Option<f64>,
    flow_score: Option<f64>,
    bonus: f64,
) -> f64 {
    let mut score = parent_score * 0.35;
    score += edge_score.unwrap_or(45.0) * 0.35;
    score += flow_score.unwrap_or(35.0) * 0.20;
    score += bonus;
    score.clamp(0.0, 100.0)
}

fn score_flow_snapshot(ctx: &PumpfunTradeContext) -> Option<f64> {
    if ctx.buy_count == 0 && ctx.sell_count == 0 {
        return None;
    }

    let age_secs = ctx.token_age_ms.max(0) as f64 / 1_000.0;
    let mut score: f64 = 0.0;

    score += if age_secs <= 30.0 {
        20.0
    } else if age_secs <= 90.0 {
        14.0
    } else if age_secs <= 300.0 {
        8.0
    } else {
        3.0
    };
    score += if ctx.buy_count >= 30 {
        18.0
    } else if ctx.buy_count >= 15 {
        12.0
    } else if ctx.buy_count >= 5 {
        6.0
    } else {
        2.0
    };
    score += if ctx.unique_buyers >= 20 {
        18.0
    } else if ctx.unique_buyers >= 10 {
        12.0
    } else if ctx.unique_buyers >= 4 {
        6.0
    } else {
        2.0
    };
    score += if ctx.buy_volume_sol >= 20.0 {
        18.0
    } else if ctx.buy_volume_sol >= 8.0 {
        12.0
    } else if ctx.buy_volume_sol >= 2.0 {
        6.0
    } else {
        2.0
    };
    score += if ctx.largest_buy_sol >= 3.0 {
        9.0
    } else if ctx.largest_buy_sol >= 1.0 {
        6.0
    } else {
        2.0
    };
    score += if ctx.buy_pressure_pct >= 85.0 {
        9.0
    } else if ctx.buy_pressure_pct >= 70.0 {
        5.0
    } else {
        0.0
    };
    if ctx.sell_count == 0 {
        score += 8.0;
    }

    Some(score.clamp(0.0, 100.0))
}

fn prune_expired_children(inner: &mut WalletGraphInner, now_ms: i64) {
    inner.children.retain(|_, child| child.expires_at_ms >= now_ms);
}

fn parent_mint_key(parent_wallet: &str, mint: &str) -> String {
    format!("{}|{}", parent_wallet, mint)
}

fn signal_dedupe_key(signal_type: &str, mint: &str, parent_wallet: &str, child_wallet: Option<&str>) -> String {
    format!(
        "{}|{}|{}|{}",
        signal_type,
        mint,
        parent_wallet,
        child_wallet.unwrap_or("-")
    )
}

fn strategy_version(state: &WalletGraphState) -> String {
    state
        .cfg
        .strategy
        .strategy_version
        .clone()
        .unwrap_or_else(|| "entry-skeleton".to_string())
}

fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn ms_to_rfc3339(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}
