use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::Mutex;

// ─── BC Score Cache (shared between detection + sniper) ─────

/// A cached bonding curve score entry, stored when a BC signal is recorded
/// (at ~30-50 SOL volume). Consumed by the sniper pipeline at graduation
/// to decide fast-track eligibility.
#[derive(Debug, Clone)]
pub struct BcScoreEntry {
    /// Composite score (0-100) from BC trading pattern analysis.
    pub score: f64,
    pub unique_buyers: usize,
    pub buy_sell_ratio: f64,
    pub creator_rebuy: bool,
    pub whale_buy: bool,
    /// Largest single buy seen when the BC score was recorded.
    pub max_single_buy_sol: f64,
    /// Bonding-curve progress percent when the BC score was recorded.
    pub bc_progress_pct: f64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub total_volume_sol: f64,
    /// When this entry was recorded (epoch ms).
    pub recorded_at: i64,
}

/// Thread-safe cache: mint (base58 string) → BcScoreEntry.
pub type BcScoreCache = Arc<Mutex<HashMap<String, BcScoreEntry>>>;

/// Create a new empty BC score cache.
pub fn new_bc_score_cache() -> BcScoreCache {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Maximum entries in the BC score cache before pruning.
const MAX_BC_CACHE_SIZE: usize = 5_000;

/// Prune the BC score cache by removing the oldest half of entries.
pub async fn prune_bc_score_cache(cache: &BcScoreCache) {
    let mut map = cache.lock().await;
    if map.len() < MAX_BC_CACHE_SIZE {
        return;
    }
    let mut entries: Vec<(String, i64)> = map
        .iter()
        .map(|(k, v)| (k.clone(), v.recorded_at))
        .collect();
    entries.sort_by_key(|(_, ts)| *ts);
    let to_remove = entries.len() / 2;
    for (key, _) in entries.into_iter().take(to_remove) {
        map.remove(&key);
    }
}

/// Compute a BC score from trading pattern signals.
/// Score is 0-100, with higher = more likely to perform well post-graduation.
///
/// v18.6 retune (2026-05-01): re-fit against rahwn `peak_multiplier >= 3.0`
/// outcome on n=152 BC-eligible closed positions. The previous formula had
/// AUC=0.533 (essentially random); the new weights below score AUC=0.592
/// and align with logistic-regression coefficients on the same target:
/// - `unique_buyers` flipped sign — runners cluster at lower buyer counts
/// - `whale_buy` weight raised (strongest discrete signal)
/// - `buy_count` band added (lower = better)
/// - `volume_sol` band dropped (no measurable signal)
pub fn compute_bc_score(
    unique_buyers: usize,
    buy_sell_ratio: f64,
    creator_rebuy: bool,
    whale_buy: bool,
    buy_count: u64,
    _sell_count: u64,
    _total_volume_sol: f64,
) -> f64 {
    let mut score: f64 = 50.0;

    // Creator rebuy: strong negative (1.1% grad rate vs 6.0%) — kept.
    if creator_rebuy {
        score -= 30.0;
    }

    // Buy/sell ratio — graduated tokens skew higher.
    if buy_sell_ratio >= 4.0 {
        score += 15.0;
    } else if buy_sell_ratio >= 2.5 {
        score += 8.0;
    } else if buy_sell_ratio >= 1.5 {
        score += 3.0;
    } else if buy_sell_ratio < 1.0 {
        score -= 15.0;
    }

    // Unique buyers — INVERTED from the prior formula. rahwn data:
    // runners avg 41.7 buyers vs non-runners 48.1; high-buyer tokens are
    // popular but already priced-in, so they pump less post-graduation.
    if unique_buyers <= 25 {
        score += 8.0;
    } else if unique_buyers <= 40 {
        score += 0.0;
    } else if unique_buyers <= 60 {
        score -= 5.0;
    } else {
        score -= 12.0;
    }

    // Whale buy — strongest discrete signal in the BC features (33% of
    // runners had a whale buy vs 20% of non-runners). Bumped from +10.
    if whale_buy {
        score += 15.0;
    }

    // Buy count — fewer total buys = under-the-radar opportunity.
    if buy_count <= 30 {
        score += 5.0;
    } else if buy_count >= 60 {
        score -= 8.0;
    }

    // sell_count and total_volume_sol kept in signature for compat but
    // dropped from the score: neither showed a significant signal in the
    // n=152 outcome fit.

    score.clamp(0.0, 100.0)
}

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
    /// Normalized label key derived from symbol/name for shadow label-flow tracking.
    pub normalized_label: String,
    /// Number of distinct prior mints with the same normalized label seen in the
    /// recent label window before this token arrived.
    pub prior_same_label_mints_6h: usize,
    /// Number of distinct prior creators associated with the same normalized label.
    pub prior_same_label_creators_6h: usize,
    /// Seconds since the most recent prior mint with the same normalized label.
    pub seconds_since_label_seen: Option<i64>,
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

    /// Whether the Lane-B 75% band (`progress_75pct`) row was written.
    /// Kept under the historical name to avoid churn at every fire site.
    pub progress_signal_recorded: bool,
    /// Whether the v14 60% band (`progress_60pct`) row was written.
    pub progress_60_recorded: bool,
    /// Whether the v14 90% band (`progress_90pct`) row was written.
    pub progress_90_recorded: bool,
    /// Whether v14 graduation-lane rows (`graduation_raw` / `graduation_goplus`)
    /// were written. Set the first time `handle_token_complete` fires them so
    /// reconnects don't double-write on replayed graduation events.
    pub graduation_recorded: bool,
    /// v14.1 counterfactual: whether the `control_no_fire` row has been
    /// written. Fires once when bc_progress_pct first crosses 30% — gives
    /// us the negative class for "tokens that started warming but didn't
    /// reach our 60% threshold". Re-firing is harmless (one-shot flag).
    pub control_recorded: bool,
    /// Whether the mint-time same-label launch shadow lane row has already
    /// been written.
    pub launch_label_shadow_recorded: bool,
    /// Whether the repeated-label shadow lane row has already been written.
    pub label_flow_shadow_recorded: bool,
    /// Whether the narrative-cluster armed-post-grad simulation row has been written.
    pub narrative_cluster_shadow_recorded: bool,
    /// Whether the probe stage of the probe-add shadow ladder has been written.
    pub probe_add_probe_recorded: bool,
    /// Whether the add stage of the probe-add shadow ladder has been written.
    pub probe_add_add_recorded: bool,
    /// Snapshot metrics captured when the probe-add shadow probe fires.
    pub probe_add_probe_buy_count: u64,
    pub probe_add_probe_unique_buyers: usize,
    pub probe_add_probe_volume_sol: f64,
    pub probe_add_probe_buy_pressure_pct: f64,
    /// Whether the early-buyer rebuy shadow row has already been written.
    pub early_buyer_rebuy_shadow_recorded: bool,

    // ── Bonding curve state, updated from each tokenTrade WS event ──
    // PumpPortal includes vSolInBondingCurve, vTokensInBondingCurve and
    // marketCapSol on every trade. Snapshotting them here lets us compute
    // bc_progress_pct without the unreliable pump.fun REST API.
    /// Most recent virtual SOL reserves observed on the BC (units: SOL).
    pub last_v_sol_reserves: f64,
    /// Most recent virtual token reserves observed on the BC.
    pub last_v_token_reserves: f64,
    /// Most recent market cap in SOL reported by PumpPortal.
    pub last_market_cap_sol: f64,
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
