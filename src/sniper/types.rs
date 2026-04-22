//! Core types for the sniper enrichment and data collection pipeline.

use serde::{Deserialize, Serialize};

// ─── Solana Tracker API response ─────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SolanaTrackerData {
    pub risk_score: Option<f64>,
    pub holders: Option<u64>,
    pub dev_pct: Option<f64>,
    pub insiders_pct: Option<f64>,
    pub bundlers_pct: Option<f64>,
    pub snipers_pct: Option<f64>,
    pub top10_pct: Option<f64>,
    pub sniper_count: Option<u64>,
    pub bundler_count: Option<u64>,
    pub insider_count: Option<u64>,
    pub total_buys: Option<u64>,
    pub total_sells: Option<u64>,
    pub total_txns: Option<u64>,
    pub lp_burn_pct: Option<f64>,
    pub has_freeze_authority: Option<bool>,
    pub has_mint_authority: Option<bool>,
    pub jupiter_verified: Option<bool>,
    pub rugged: Option<bool>,
    // ── Advanced tier: volume / momentum ──
    pub volume_5m: Option<f64>,
    pub volume_15m: Option<f64>,
    pub volume_1h: Option<f64>,
    pub volume_24h: Option<f64>,
    pub price_change_5m: Option<f64>,
    pub price_change_1h: Option<f64>,
    // ── Advanced tier: fees / tips (Jito smart-money signal) ──
    pub fees_total_sol: Option<f64>,
    pub fees_total_tips: Option<f64>,
    pub fees_total_trading: Option<f64>,
    // ── Advanced tier: deployer / market info ──
    pub deployer: Option<String>,
    pub market: Option<String>,
}

/// A single trade from the /trades/{mint} endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaTrackerTrade {
    pub tx: String,
    pub trade_type: String,
    pub volume_usd: f64,
    pub volume_sol: f64,
    pub wallet: String,
    pub time_ms: i64,
}

// ─── On-chain mint data ──────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
pub struct MintData {
    pub mint_authority_revoked: bool,
    pub freeze_authority_revoked: bool,
    pub supply: f64,
    pub decimals: u8,
}

// ─── PumpFun bonding curve data ──────────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
pub struct BondingCurveData {
    pub creation_timestamp: Option<i64>,
    pub complete: bool,
    pub virtual_sol_reserves: Option<f64>,
    pub virtual_token_reserves: Option<f64>,
    /// Computed: (graduation_time - creation_time) / 60
    pub sale_duration_mins: Option<f64>,
}

// ─── Creator wallet history ──────────────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
pub struct CreatorData {
    pub wallet_age_days: Option<f64>,
    pub previous_token_count: Option<u64>,
    pub is_serial_creator: bool,
    pub total_signatures: Option<u64>,
}

// ─── Birdeye API responses ───────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BirdeyeOverview {
    pub price: Option<f64>,
    pub mc: Option<f64>,
    #[serde(rename = "v24hUSD")]
    pub v24h_usd: Option<f64>,
    #[serde(rename = "uniqueWallet24h")]
    pub unique_wallet_24h: Option<u64>,
    #[serde(rename = "trade24h")]
    pub trade_24h: Option<u64>,
    #[serde(rename = "buy24h")]
    pub buy_24h: Option<u64>,
    #[serde(rename = "sell24h")]
    pub sell_24h: Option<u64>,
    pub liquidity: Option<f64>,
    pub holder: Option<u64>,
    #[serde(rename = "lastTradeUnixTime")]
    pub last_trade_unix_time: Option<i64>,
    pub supply: Option<f64>,
    pub decimals: Option<u8>,
    pub extensions: Option<BirdeyeExtensions>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BirdeyeExtensions {
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub website: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BirdeyeSecurity {
    #[serde(rename = "ownerBalance")]
    pub owner_balance: Option<f64>,
    #[serde(rename = "creatorBalance")]
    pub creator_balance: Option<f64>,
    #[serde(rename = "ownerPercentage")]
    pub owner_percentage: Option<f64>,
    #[serde(rename = "creatorPercentage")]
    pub creator_percentage: Option<f64>,
    #[serde(rename = "top10HolderBalance")]
    pub top10_holder_balance: Option<f64>,
    #[serde(rename = "top10HolderPercent")]
    pub top10_holder_percent: Option<f64>,
    #[serde(rename = "isToken2022")]
    pub is_token_2022: Option<bool>,
    #[serde(rename = "isProxy")]
    pub is_proxy: Option<bool>,
    #[serde(rename = "isMintable")]
    pub is_mintable: Option<bool>,
    #[serde(rename = "isMutable")]
    pub is_mutable: Option<bool>,
    #[serde(rename = "lockInfo")]
    pub lock_info: Option<BirdeyeLockInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BirdeyeLockInfo {
    #[serde(rename = "lockPercent")]
    pub lock_percent: Option<f64>,
    #[serde(rename = "lockTag")]
    pub lock_tag: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BirdeyeCreation {
    #[serde(rename = "txHash")]
    pub tx_hash: Option<String>,
    pub creator: Option<String>,
    pub slot: Option<u64>,
    #[serde(rename = "blockUnixTime")]
    pub block_unix_time: Option<i64>,
    pub decimals: Option<u8>,
    #[serde(rename = "initialSupply")]
    pub initial_supply: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BirdeyeMemeDetail {
    pub price: Option<f64>,
    #[serde(rename = "priceChange24h")]
    pub price_change_24h: Option<f64>,
    pub volume_24h: Option<f64>,
}

// ─── GoPlus API response ─────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GoPlusResult {
    pub is_honeypot: Option<String>,
    pub is_mintable: Option<String>,
    pub transfer_pausable: Option<String>,
    pub is_blacklisted: Option<String>,
    pub can_take_back_ownership: Option<String>,
    pub is_proxy: Option<String>,
    pub is_open_source: Option<String>,
    pub holder_count: Option<u64>,
}

impl GoPlusResult {
    pub fn is_flag_set(val: &Option<String>) -> bool {
        val.as_ref().map_or(false, |v| v == "1")
    }
}

// ─── Smart wallet metrics ────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
pub struct SmartWalletMetrics {
    pub total_scored: u32,
    pub suspicious_count: u32,
    pub genuine_count: u32,
    pub suspicious_ratio: f64,
    pub max_same_funder_count: u32,
    pub avg_tx_count: f64,
    pub avg_wallet_age_secs: f64,
    pub min_wallet_age_secs: f64,
}

// ─── Whale buy metrics (from /trades at enrichment) ──────────

#[derive(Debug, Clone, Default, Serialize)]
pub struct WhaleBuyMetrics {
    /// Largest single buy trade in SOL
    pub max_single_buy_sol: f64,
    /// Number of buys > 1 SOL
    pub whale_buy_count: u32,
    /// Total whale buy volume (buys > 1 SOL)
    pub whale_buy_volume_sol: f64,
    /// Average buy size in SOL (all buys)
    pub avg_buy_size_sol: f64,
    /// Total recent trades analyzed
    pub total_trades: u32,
    /// Buy/sell ratio by volume
    pub buy_sell_volume_ratio: f64,
}

// ─── Aggregate enrichment result ─────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct EnrichmentResult {
    pub solana_tracker: Option<SolanaTrackerData>,
    pub on_chain_mint: Option<MintData>,
    pub pumpfun_curve: Option<BondingCurveData>,
    pub creator_history: Option<CreatorData>,
    pub birdeye_overview: Option<BirdeyeOverview>,
    pub birdeye_security: Option<BirdeyeSecurity>,
    pub birdeye_creation: Option<BirdeyeCreation>,
    pub birdeye_meme: Option<BirdeyeMemeDetail>,
    pub goplus: Option<GoPlusResult>,
    pub smart_wallets: Option<SmartWalletMetrics>,
    pub whale_buys: Option<WhaleBuyMetrics>,
    pub enrichment_duration_ms: u64,
    pub sources_completed: Vec<String>,
    pub sources_timed_out: Vec<String>,
    /// Per-source wall-clock timing in ms
    pub per_source_ms: std::collections::HashMap<String, u64>,
}

// ─── Hard filter result ──────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HardFilterResult {
    pub passed: bool,
    pub rejection_reason: Option<String>,
    /// Which hard filter blocked it (for sniper_candidates logging)
    pub filter_name: Option<String>,
}

// ─── Soft flags ──────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
pub struct SoftFlags {
    pub flag_high_bundlers: bool,
    pub flag_serial_creator: bool,
    pub flag_fast_graduation: bool,
    pub flag_low_holders: bool,
    pub flag_high_concentration: bool,
    pub flag_honeypot_goplus: bool,
    pub flag_wash_trade_ring: bool,
    pub flag_suspicious_holders: bool,
    pub flag_no_socials: bool,
    pub flag_mintable_any_source: bool,
    pub flag_lpi_divergence: bool,
    pub flag_dev_holds: bool,
    pub flag_sniper_heavy: bool,
    pub flag_lp_not_burned: bool,
    pub flag_transfer_pausable: bool,
    pub flag_ownership_reclaimable: bool,
    pub flag_low_risk_score: bool,
}

// ─── Scoring (Phase 3 placeholder) ──────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
pub struct SniperScore {
    pub score: f64,
    pub components: serde_json::Value,
    pub hard_reject: bool,
}
