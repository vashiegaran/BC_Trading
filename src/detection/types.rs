use std::collections::HashMap;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

// ─── Pipeline-wide latency accumulator ──────────────────────

/// Accumulates timing data for every stage of the pipeline.
/// Threaded through `GraduatedToken` → `FilteredToken` → written to Supabase.
#[derive(Debug, Clone, Default)]
pub struct PipelineTiming {
    pub detected_at_ms: i64,
    /// Wall-clock ms from detected_at to sniper enrichment start
    pub detection_to_sniper_ms: Option<i64>,
    /// Total enrichment elapsed (all 9 parallel calls)
    pub enrichment_total_ms: Option<u64>,
    /// Per-source API timing: {"solana_tracker": 234, "birdeye_overview": 156, …}
    pub enrichment_per_source: HashMap<String, u64>,
    /// Total hard-filter evaluation time
    pub hard_filter_total_ms: Option<u64>,
    /// Total filter-engine (fast-gate) elapsed
    pub filter_engine_total_ms: Option<u64>,
    /// Per-check filter timing: {"sanity": 0, "age": 0, "liquidity": 234, …}
    pub filter_per_check: HashMap<String, u64>,
    /// Pre-execution safety checks elapsed
    pub precheck_total_ms: Option<u64>,
    /// Trade execution elapsed (buy)
    pub execution_total_ms: Option<u64>,
    /// Post-buy verification total elapsed
    pub post_buy_total_ms: Option<u64>,
    /// Per-check post-buy timing
    pub post_buy_per_check: HashMap<String, u64>,
    /// End-to-end: detected_at → position opened
    pub pipeline_total_ms: Option<u64>,
    /// Final outcome
    pub outcome: Option<String>,
    pub rejection_stage: Option<String>,
    pub rejection_reason: Option<String>,
    pub position_id: Option<i64>,
}

impl PipelineTiming {
    pub fn new(detected_at_ms: i64) -> Self {
        Self {
            detected_at_ms,
            ..Default::default()
        }
    }

    /// Build the JSON payload for the pipeline_latency Supabase table.
    pub fn to_json(&self, mint: &str) -> serde_json::Value {
        serde_json::json!({
            "mint": mint,
            "detected_at_ms": self.detected_at_ms,
            "detection_to_sniper_ms": self.detection_to_sniper_ms,
            "enrichment_total_ms": self.enrichment_total_ms,
            "enrichment_per_source": if self.enrichment_per_source.is_empty() { serde_json::Value::Null } else { serde_json::json!(self.enrichment_per_source) },
            "hard_filter_total_ms": self.hard_filter_total_ms,
            "filter_engine_total_ms": self.filter_engine_total_ms,
            "filter_per_check": if self.filter_per_check.is_empty() { serde_json::Value::Null } else { serde_json::json!(self.filter_per_check) },
            "precheck_total_ms": self.precheck_total_ms,
            "execution_total_ms": self.execution_total_ms,
            "post_buy_total_ms": self.post_buy_total_ms,
            "post_buy_per_check": if self.post_buy_per_check.is_empty() { serde_json::Value::Null } else { serde_json::json!(self.post_buy_per_check) },
            "pipeline_total_ms": self.pipeline_total_ms,
            "outcome": self.outcome,
            "rejection_stage": self.rejection_stage,
            "rejection_reason": self.rejection_reason,
            "position_id": self.position_id,
        })
    }
}

/// Source that detected the new token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DetectionSource {
    PumpFun,
    Geyser,
    Poll,
    SolanaTrackerSearch,
}

impl std::fmt::Display for DetectionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PumpFun => write!(f, "pumpfun_ws"),
            Self::Geyser => write!(f, "geyser"),
            Self::Poll => write!(f, "poll"),
            Self::SolanaTrackerSearch => write!(f, "st_search"),
        }
    }
}

/// In-memory watchlist entry for a token that has been created on pump.fun
/// but has not yet graduated to Raydium.
///
/// Populated by `newToken` events and updated by `tokenTrade` events.
#[derive(Debug, Clone)]
pub struct WatchlistEntry {
    /// Token mint (contract) address.
    pub mint: Pubkey,
    /// Creator wallet that launched the token.
    pub creator_wallet: Pubkey,
    /// Unix timestamp (ms) when the token was first seen.
    pub detected_at: i64,
    /// SOL amount of the creator's initial buy.
    pub initial_buy_sol: f64,
    /// Token name from the newToken event.
    pub name: String,
    /// Token symbol (ticker) from the newToken event.
    pub symbol: String,
    /// Cumulative SOL volume traded on the bonding curve.
    pub total_volume_sol: f64,
    /// Number of buy transactions observed.
    pub buy_count: u64,
    /// Number of sell transactions observed.
    pub sell_count: u64,
    /// Set of unique buyer wallet addresses seen during bonding curve.
    pub unique_buyers: HashSet<Pubkey>,
    /// Trade timestamps paired with buyer wallet — used for wash-trade detection.
    /// Each entry is (unix_timestamp_ms, buyer_pubkey).
    pub trade_timestamps: Vec<(i64, Pubkey)>,
    /// Per-trade log for bonding curve signal detection.
    /// (timestamp_ms, sol_amount, is_buy, trader_pubkey)
    /// Capped at MAX_TRADE_LOG_SIZE entries.
    pub trade_log: Vec<(i64, f64, bool, Pubkey)>,
    /// Whether a bonding_curve_signals row has already been written.
    pub signal_recorded: bool,
}

/// Maximum trade log entries per token to bound memory.
pub const MAX_TRADE_LOG_SIZE: usize = 500;

impl WatchlistEntry {
    /// Buy-pressure percentage: buys / (buys + sells).
    /// Returns 0.0 when no trades have been recorded.
    pub fn buy_pressure_pct(&self) -> f64 {
        let total = self.buy_count + self.sell_count;
        if total == 0 {
            return 0.0;
        }
        self.buy_count as f64 / total as f64 * 100.0
    }
}

/// Event emitted when a pump.fun token **graduates** to Raydium.
///
/// Sent from the detection engine through an MPSC channel to downstream
/// consumers (filter engine, logger, etc.).  Contains aggregated data
/// collected while the token was on the bonding curve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraduatedToken {
    /// Token mint (contract) address.
    pub mint: Pubkey,

    /// Raydium liquidity pool address (from the graduation event).
    /// `None` when the graduation payload did not include a valid pool address.
    pub pool_address: Option<Pubkey>,

    /// Creator wallet that launched the token on pump.fun.
    pub creator_wallet: Pubkey,

    /// Total SOL volume traded on the bonding curve before graduation.
    pub bonding_curve_volume_sol: f64,

    /// Buy-pressure percentage: buys / (buys + sells) × 100.
    pub buy_pressure_pct: f64,

    /// Seconds elapsed between token creation and graduation.
    pub time_to_graduate_seconds: f64,

    /// Unix timestamp in milliseconds when the token was first detected.
    pub detected_at: i64,

    /// Which detection method found this token.
    pub source: DetectionSource,

    /// Number of unique buyer wallets observed during bonding curve.
    pub unique_buyer_count: usize,

    /// Number of buy transactions observed on the bonding curve.
    pub buy_count: u64,

    /// Number of sell transactions observed on the bonding curve.
    pub sell_count: u64,

    /// Trade timestamps paired with buyer wallet — for wash-trade detection.
    /// Each entry is (unix_timestamp_ms, buyer_pubkey).
    #[serde(skip)]
    pub trade_timestamps: Vec<(i64, Pubkey)>,

    /// Token name from the creation event.
    pub name: String,

    /// Token symbol (ticker) from the creation event.
    pub symbol: String,

    /// Initial liquidity in SOL added to the pool at graduation.
    pub initial_liquidity_sol: f64,

    /// Whether the creator wallet bought back in during bonding curve.
    pub creator_rebuy: bool,

    /// buy_count / sell_count ratio on the bonding curve.
    pub buy_sell_ratio: f64,

    /// sniper_candidates row ID (set after sniper enrichment logs to Supabase).
    #[serde(skip)]
    pub candidate_id: Option<i64>,

    /// Sniper features JSON computed during enrichment (attached for
    /// positions.sniper_features column so downstream ML/analysis can
    /// inspect per-trade feature values without a sniper_candidates join).
    #[serde(skip)]
    pub sniper_features: Option<serde_json::Value>,

    /// Sniper score computed during enrichment (for positions.sniper_score).
    #[serde(skip)]
    pub sniper_score: Option<f64>,

    /// Accumulated pipeline timing data (not serialized — in-process only).
    #[serde(skip)]
    pub pipeline_timing: PipelineTiming,
}
