use std::env;
use std::fs;

use serde::Deserialize;

// ── TOML config structs ──────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TomlConfig {
    /// Strategy version tag stamped into every position row (e.g. "v1").
    #[serde(default)]
    pub strategy_version: Option<String>,
    pub detection: DetectionConfig,
    pub filters: FiltersConfig,
    pub execution: ExecutionConfig,
    pub jito: JitoConfig,
    pub exit: ExitConfig,
    pub risk: RiskConfig,
    pub monitoring: MonitoringConfig,
    /// Re-entry shadow-mode tracking (post-moonbag exit).
    #[serde(default)]
    pub reentry: ReentryConfig,
}

#[derive(Debug, Deserialize)]
pub struct ReentryConfig {
    /// Enable post-moonbag re-entry watcher. Requires OPENAI_API_KEY + X_API_BEARER_TOKEN
    /// for narrative scoring. If false, the watcher task does not start.
    #[serde(default)]
    pub enabled: bool,
    /// Shadow mode: log every would-be re-entry to `reentry_candidates` but never
    /// execute a paper/real buy. MUST remain true until data supports flipping it.
    #[serde(default = "default_true")]
    pub shadow_mode: bool,
    /// Window (seconds) after moonbag exit during which re-entries are evaluated.
    #[serde(default = "default_reentry_window_seconds")]
    pub window_seconds: u64,
    /// Minimum dip % from exit price (or previous attempt price) before a
    /// re-entry is considered. e.g. 0.15 = must be ≥ 15% below exit price.
    #[serde(default = "default_reentry_min_dip_pct")]
    pub min_dip_pct: f64,
    /// Minimum narrative score (0..100) for the "permissive" would-enter flag.
    #[serde(default = "default_reentry_min_narrative_score")]
    pub min_narrative_score: u8,
    /// Interval (seconds) between evaluation passes of all tracked exited mints.
    #[serde(default = "default_reentry_check_interval_seconds")]
    pub check_interval_seconds: u64,
    /// Interval (seconds) between outcome-backfill passes.
    #[serde(default = "default_reentry_outcome_interval_seconds")]
    pub outcome_interval_seconds: u64,
    /// Enqueue-poll lookback window (seconds) for finding newly-closed positions.
    /// Should be ≥ 2 × check_interval_seconds.
    #[serde(default = "default_reentry_enqueue_lookback_seconds")]
    pub enqueue_lookback_seconds: u64,
    /// Piggyback gate: only track exits whose `peak_multiplier` met this floor.
    /// 0 = track all closed positions (legacy). 3.0 = moonbag-only.
    #[serde(default = "default_reentry_min_peak_multiplier")]
    pub min_peak_multiplier_to_track: f64,
    /// When false, the narrative gate is auto-passed (dip-only piggyback).
    /// Use this to collect post-moonbag-exit price data without burning OpenAI/X
    /// credits on every tick.
    #[serde(default = "default_true")]
    pub require_narrative: bool,
}

impl Default for ReentryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shadow_mode: true,
            window_seconds: default_reentry_window_seconds(),
            min_dip_pct: default_reentry_min_dip_pct(),
            min_narrative_score: default_reentry_min_narrative_score(),
            check_interval_seconds: default_reentry_check_interval_seconds(),
            outcome_interval_seconds: default_reentry_outcome_interval_seconds(),
            enqueue_lookback_seconds: default_reentry_enqueue_lookback_seconds(),
            min_peak_multiplier_to_track: default_reentry_min_peak_multiplier(),
            require_narrative: true,
        }
    }
}

fn default_reentry_window_seconds() -> u64 {
    21_600
} // 6h
fn default_reentry_min_dip_pct() -> f64 {
    0.15
} // 15%
fn default_reentry_min_narrative_score() -> u8 {
    60
} // 0..100 scale
fn default_reentry_check_interval_seconds() -> u64 {
    90
}
fn default_reentry_outcome_interval_seconds() -> u64 {
    300
} // 5m
fn default_reentry_enqueue_lookback_seconds() -> u64 {
    300
} // 5m
fn default_reentry_min_peak_multiplier() -> f64 {
    3.0
} // moonbag-only by default

#[derive(Debug, Deserialize)]
pub struct DetectionConfig {
    pub method: String,
    pub poll_raydium: bool,
    pub poll_interval_seconds: u64,
    /// Enable bonding curve signal recording for pattern mining.
    #[serde(default = "default_true")]
    pub bonding_curve_signals_enabled: bool,
    /// Minimum cumulative SOL volume before recording a signal row.
    /// ~50 SOL ≈ 60% to graduation, filters out 95% of dead tokens.
    #[serde(default = "default_bc_signal_volume_threshold")]
    pub bc_signal_volume_threshold: f64,
    /// Shadow-only lane: record a would-be mint-time entry when a brand-new
    /// token arrives into a very recent same-label cluster.
    #[serde(default)]
    pub launch_label_shadow_enabled: bool,
    /// Maximum age, in seconds, from token creation for the mint-time launch
    /// label shadow lane.
    #[serde(default = "default_launch_label_shadow_max_age_seconds")]
    pub launch_label_shadow_max_age_seconds: u64,
    /// Maximum bonding-curve progress for the mint-time launch label shadow
    /// lane. Keeps the lane focused on first-seconds launch entries.
    #[serde(default = "default_launch_label_shadow_max_progress_pct")]
    pub launch_label_shadow_max_progress_pct: f64,
    /// Minimum number of prior same-label mints seen in the recent window
    /// before a token qualifies for the mint-time launch label shadow lane.
    #[serde(default = "default_launch_label_shadow_min_prior_mints")]
    pub launch_label_shadow_min_prior_mints: usize,
    /// Minimum number of distinct prior creators across the same-label mint
    /// cluster before the mint-time launch label shadow lane can fire.
    #[serde(default = "default_launch_label_shadow_min_prior_creators")]
    pub launch_label_shadow_min_prior_creators: usize,
    /// Maximum age, in seconds, of the most recent prior same-label mint for
    /// the mint-time launch label shadow lane.
    #[serde(default = "default_launch_label_shadow_max_gap_seconds")]
    pub launch_label_shadow_max_gap_seconds: u64,
    /// Shadow-only lane: record repeated same-label mint clusters to
    /// `bc_paper_trades` without affecting live execution.
    #[serde(default)]
    pub label_flow_shadow_enabled: bool,
    /// Minimum bonding-curve progress before the label-flow shadow lane can
    /// fire.
    #[serde(default = "default_label_flow_shadow_min_progress_pct")]
    pub label_flow_shadow_min_progress_pct: f64,
    /// Minimum number of prior same-label mints seen in the recent window
    /// before a token qualifies for the label-flow shadow lane.
    #[serde(default = "default_label_flow_shadow_min_prior_mints")]
    pub label_flow_shadow_min_prior_mints: usize,
    /// Maximum age, in seconds, of the most recent prior same-label mint for
    /// the label-flow shadow lane.
    #[serde(default = "default_label_flow_shadow_max_gap_seconds")]
    pub label_flow_shadow_max_gap_seconds: u64,
    /// Shadow-only armed-post-graduation simulation for repeated narrative
    /// clusters. Allows creator rebuy only inside this research lane.
    #[serde(default)]
    pub narrative_cluster_shadow_enabled: bool,
    /// Minimum prior same-label mints required before narrative-cluster arming.
    #[serde(default = "default_narrative_cluster_min_prior_mints")]
    pub narrative_cluster_min_prior_mints: usize,
    /// Minimum BC progress required before narrative-cluster arming.
    #[serde(default = "default_narrative_cluster_min_progress_pct")]
    pub narrative_cluster_min_progress_pct: f64,
    /// Maximum BC progress allowed for narrative-cluster arming.
    #[serde(default = "default_narrative_cluster_max_progress_pct")]
    pub narrative_cluster_max_progress_pct: f64,
    /// Maximum age, in seconds, of the most recent prior same-label mint.
    #[serde(default = "default_narrative_cluster_max_gap_seconds")]
    pub narrative_cluster_max_gap_seconds: u64,
    /// Minimum composite score required to arm the narrative-cluster shadow row.
    #[serde(default = "default_narrative_cluster_min_score")]
    pub narrative_cluster_min_score: f64,
    /// Shadow-only ladder model: record a would-be probe row and later a
    /// would-be add row if the same mint keeps strengthening.
    #[serde(default)]
    pub probe_add_shadow_enabled: bool,
    /// Minimum bonding-curve progress before the would-be probe row fires.
    #[serde(default = "default_probe_add_probe_progress_pct")]
    pub probe_add_probe_progress_pct: f64,
    /// Minimum bonding-curve progress before the would-be add row can fire.
    #[serde(default = "default_probe_add_add_progress_pct")]
    pub probe_add_add_progress_pct: f64,
    /// Minimum increase in distinct buyers required between the probe and add
    /// shadow rows.
    #[serde(default = "default_probe_add_min_unique_buyer_delta")]
    pub probe_add_min_unique_buyer_delta: usize,
    /// Minimum volume growth multiplier required between the probe and add
    /// shadow rows.
    #[serde(default = "default_probe_add_min_volume_multiplier")]
    pub probe_add_min_volume_multiplier: f64,
    /// Shadow-only signal: track whether any of the first N non-creator buyers
    /// buys again before graduation. Pure data collection; never forwards live.
    #[serde(default)]
    pub early_buyer_rebuy_shadow_enabled: bool,
    /// Number of earliest distinct non-creator buyers to watch.
    #[serde(default = "default_early_buyer_rebuy_first_n")]
    pub early_buyer_rebuy_first_n: usize,
    /// Minimum distinct early buyers that must rebuy to record a shadow pass.
    #[serde(default = "default_early_buyer_rebuy_min_rebuy_wallets")]
    pub early_buyer_rebuy_min_rebuy_wallets: usize,
    /// Minimum SOL size for a repeat buy to count. 0 = count any non-zero rebuy.
    #[serde(default)]
    pub early_buyer_rebuy_min_rebuy_sol: f64,
    /// Maximum BC progress at which the shadow row can fire. 100 = any time
    /// before graduation.
    #[serde(default = "default_early_buyer_rebuy_max_progress_pct")]
    pub early_buyer_rebuy_max_progress_pct: f64,
}

fn default_true() -> bool {
    true
}
fn default_bc_signal_volume_threshold() -> f64 {
    50.0
}
fn default_launch_label_shadow_max_age_seconds() -> u64 {
    30
}
fn default_launch_label_shadow_max_progress_pct() -> f64 {
    25.0
}
fn default_launch_label_shadow_min_prior_mints() -> usize {
    1
}
fn default_launch_label_shadow_min_prior_creators() -> usize {
    1
}
fn default_launch_label_shadow_max_gap_seconds() -> u64 {
    180
}
fn default_label_flow_shadow_min_progress_pct() -> f64 {
    30.0
}
fn default_label_flow_shadow_min_prior_mints() -> usize {
    1
}
fn default_label_flow_shadow_max_gap_seconds() -> u64 {
    1_800
}
fn default_narrative_cluster_min_prior_mints() -> usize {
    1
}
fn default_narrative_cluster_min_progress_pct() -> f64 {
    20.0
}
fn default_narrative_cluster_max_progress_pct() -> f64 {
    100.0
}
fn default_narrative_cluster_max_gap_seconds() -> u64 {
    1_800
}
fn default_narrative_cluster_min_score() -> f64 {
    45.0
}
fn default_probe_add_probe_progress_pct() -> f64 {
    60.0
}
fn default_probe_add_add_progress_pct() -> f64 {
    75.0
}
fn default_probe_add_min_unique_buyer_delta() -> usize {
    10
}
fn default_probe_add_min_volume_multiplier() -> f64 {
    1.5
}
fn default_early_buyer_rebuy_first_n() -> usize {
    5
}
fn default_early_buyer_rebuy_min_rebuy_wallets() -> usize {
    1
}
fn default_early_buyer_rebuy_max_progress_pct() -> f64 {
    100.0
}
fn default_max_liquidity_usd() -> u64 {
    0
} // 0 = disabled, must be set explicitly to take effect
fn default_max_initial_liquidity_sol() -> f64 {
    0.0
} // 0 = disabled

#[derive(Debug, Deserialize)]
pub struct FiltersConfig {
    pub min_liquidity_usd: u64,
    /// Maximum USD value of liquidity at launch.
    /// Data (rahwn 280 closed positions): liquidity 80-150 SOL bucket avg
    /// pnl/trade is essentially flat-to-negative (n≈40, pnl ≈ -0.05 SOL),
    /// while 30-80 SOL band carried the full +8 SOL upside. Cap default
    /// 27_000 USD ≈ 90 SOL trims dead-capital fires without losing winners.
    /// 0 = disabled (back-compat).
    #[serde(default = "default_max_liquidity_usd")]
    pub max_liquidity_usd: u64,
    /// Maximum SOL on the SOL side of the pool at detection time.
    /// 0 = disabled. Data (rahwn n=282, v18.6 retune): tokens with
    /// `initial_liquidity_sol > 80` had a >=3x rate of 5.1% vs 26.2% for
    /// `<= 80`. Tightening to 80 lifts the moonbag rate from 16.0% baseline
    /// to 26.2% on the full corpus and from 7.7% to 11.1% on the realistic-
    /// fills (v18.x) slice.
    #[serde(default = "default_max_initial_liquidity_sol")]
    pub max_initial_liquidity_sol: f64,
    pub max_top_holder_pct: f64,
    pub max_dev_hold_pct: f64,
    pub max_token_age_seconds: u64,
    pub max_graduation_time_seconds: f64,
    pub max_market_cap_usd: u64,
    #[serde(default)]
    pub min_market_cap_usd: u64,
    pub max_price_impact_pct: f64,
    pub max_rugcheck_score: u32,
    pub require_liquidity_locked: bool,
    pub min_lock_duration_days: u64,
    pub reject_bundled: bool,
    pub min_buy_pressure_pct: f64,
    pub min_bonding_volume_sol: f64,
    pub min_unique_buyers: usize,
    pub coordinated_window_ms: i64,
    pub coordinated_buy_threshold: usize,
    /// Reject tokens where the creator wallet bought back during bonding curve.
    /// Data: creator_rebuy tokens graduate at 1.1% vs 6.0% without (5x worse).
    #[serde(default = "default_true")]
    pub reject_creator_rebuy: bool,
    /// Shadow-only experiment: for high BC-score creator-rebuy tokens, run the
    /// Fast-Track safety checks and log the would-have-qualified row, but never
    /// forward to live/paper execution. Keeps `reject_creator_rebuy` intact.
    #[serde(default)]
    pub creator_rebuy_shadow_enabled: bool,
    /// Minimum BC score required for the creator-rebuy shadow lane.
    #[serde(default = "default_creator_rebuy_shadow_min_score")]
    pub creator_rebuy_shadow_min_score: f64,
    /// Extremely narrow live canary for creator-rebuy tokens that already pass
    /// Fast-Track safety plus stronger bonding-curve profile gates. Keeps the
    /// broad `reject_creator_rebuy` hard gate intact.
    #[serde(default)]
    pub creator_rebuy_live_test_enabled: bool,
    /// Minimum BC score for the creator-rebuy live canary.
    #[serde(default = "default_creator_rebuy_live_test_min_score")]
    pub creator_rebuy_live_test_min_score: f64,
    /// Minimum buy pressure for the creator-rebuy live canary.
    #[serde(default = "default_creator_rebuy_live_test_min_buy_pressure_pct")]
    pub creator_rebuy_live_test_min_buy_pressure_pct: f64,
    /// Minimum buy/sell ratio for the creator-rebuy live canary.
    #[serde(default = "default_creator_rebuy_live_test_min_buy_sell_ratio")]
    pub creator_rebuy_live_test_min_buy_sell_ratio: f64,
    /// Minimum distinct buyers for the creator-rebuy live canary.
    #[serde(default = "default_creator_rebuy_live_test_min_unique_buyers")]
    pub creator_rebuy_live_test_min_unique_buyers: usize,
    /// Maximum sell count for the creator-rebuy live canary.
    #[serde(default = "default_creator_rebuy_live_test_max_sell_count")]
    pub creator_rebuy_live_test_max_sell_count: u64,
    /// Minimum SOL-side pool liquidity for the creator-rebuy live canary.
    /// 0 = disabled. Unknown/zero liquidity fails when this is > 0.
    #[serde(default = "default_creator_rebuy_live_test_min_initial_liquidity_sol")]
    pub creator_rebuy_live_test_min_initial_liquidity_sol: f64,
    /// Maximum SOL-side pool liquidity for the creator-rebuy live canary.
    /// 0 = disabled.
    #[serde(default = "default_creator_rebuy_live_test_max_initial_liquidity_sol")]
    pub creator_rebuy_live_test_max_initial_liquidity_sol: f64,
    /// Maximum concurrently open positions from the creator-rebuy live canary.
    /// 0 = no lane-specific cap.
    #[serde(default = "default_creator_rebuy_live_test_max_open_positions")]
    pub creator_rebuy_live_test_max_open_positions: u32,
    /// Require non-empty token identity and a non-sentinel creator wallet before
    /// any creator-rebuy live canary can forward to execution.
    #[serde(default = "default_true")]
    pub creator_rebuy_live_test_require_valid_identity: bool,
    /// Maximum BC progress at the cached signal time for creator-rebuy canary.
    /// 0 = disabled.
    #[serde(default = "default_creator_rebuy_live_test_max_bc_progress_pct")]
    pub creator_rebuy_live_test_max_bc_progress_pct: f64,
    /// Minimum cached BC volume for creator-rebuy canary.
    #[serde(default = "default_creator_rebuy_live_test_min_total_volume_sol")]
    pub creator_rebuy_live_test_min_total_volume_sol: f64,
    /// Minimum largest single buy in SOL for creator-rebuy canary.
    #[serde(default = "default_creator_rebuy_live_test_min_whale_buy_sol")]
    pub creator_rebuy_live_test_min_whale_buy_sol: f64,
    /// Enable the high-precision zero-sell creator-rebuy profile.
    #[serde(default = "default_true")]
    pub creator_rebuy_live_test_zero_sell_enabled: bool,
    /// Minimum BC score for the zero-sell creator-rebuy profile.
    #[serde(default = "default_creator_rebuy_live_test_zero_sell_min_score")]
    pub creator_rebuy_live_test_zero_sell_min_score: f64,
    /// Minimum buy pressure for the zero-sell creator-rebuy profile.
    #[serde(default = "default_creator_rebuy_live_test_zero_sell_min_buy_pressure_pct")]
    pub creator_rebuy_live_test_zero_sell_min_buy_pressure_pct: f64,
    /// Minimum buy/sell ratio for the zero-sell creator-rebuy profile.
    #[serde(default = "default_creator_rebuy_live_test_zero_sell_min_buy_sell_ratio")]
    pub creator_rebuy_live_test_zero_sell_min_buy_sell_ratio: f64,
    /// Minimum BC progress for the strong-flow creator-rebuy profile.
    #[serde(default = "default_creator_rebuy_live_test_strong_flow_min_bc_progress_pct")]
    pub creator_rebuy_live_test_strong_flow_min_bc_progress_pct: f64,
    /// Minimum buy/sell ratio on the bonding curve.
    /// Data: Q4 (>2.3) graduates at 10.9% vs Q1 (<1.1) at 3.2%.
    #[serde(default = "default_min_buy_sell_ratio")]
    pub min_buy_sell_ratio: f64,
    /// Allow a strong BC fast-track score to bypass the low buy/sell ratio hard
    /// reject. This keeps `creator_rebuy` hard while rescuing high-conviction
    /// fast-track names that would otherwise die before minimal enrichment.
    #[serde(default)]
    pub allow_fast_track_buy_sell_ratio_bypass: bool,
    /// Maximum sell count on bonding curve. High sells = dump pressure.
    /// Data: graduated tokens median 20 sells vs non-graduated 28.
    #[serde(default = "default_max_bc_sell_count")]
    pub max_bc_sell_count: u64,
    /// Maximum % a single non-dev wallet can hold. One whale = dump risk.
    #[serde(default = "default_max_single_holder_pct")]
    pub max_single_holder_pct: f64,
    /// Minimum number of distinct token holders. Below this = too thin.
    #[serde(default = "default_min_holder_count")]
    pub min_holder_count: usize,
    /// Minimum sniper score to pass (v9, data-driven).
    /// Data: score >= 65 → +0.056 SOL, < 65 → -0.500 SOL over 92 trades.
    #[serde(default = "default_min_sniper_score")]
    pub min_sniper_score: f64,
    /// Enable the BC Fast-Track pipeline: tokens with high BC scores skip
    /// full enrichment and buy with only mint+GoPlus checks (~250ms vs 2s).
    #[serde(default)]
    pub bc_fast_track_enabled: bool,
    /// Minimum BC score (from bonding curve signal analysis) to qualify for
    /// the fast-track pipeline. Data: score >= 65 → median peak 1.97x, 49% hit 2x+.
    #[serde(default = "default_bc_fast_track_min_score")]
    pub bc_fast_track_min_score: f64,
    /// Enable the Standard buy lane (full 2s enrichment + sniper-score gate).
    /// v14.1 data: Standard lane peaked under 2x on every closed position
    /// (median peak 1.22x vs Fast-Track 2.38x; +0.014 SOL vs +1.017 SOL).
    /// Set false to disable Standard entirely — only Fast-Track buys.
    #[serde(default = "default_standard_lane_enabled")]
    pub standard_lane_enabled: bool,
    /// Enable the `graduation_goplus` paper-trade lane.
    /// v14.1 data: produced identical rows to `graduation_raw` (n=148/148,
    /// total +1.54/+1.54 SOL). The GoPlus check filters nothing post-grad.
    /// Disabled by default to save Helius/GoPlus API calls.
    #[serde(default = "default_graduation_goplus_enabled")]
    pub graduation_goplus_enabled: bool,
}

fn default_max_single_holder_pct() -> f64 {
    25.0
}
fn default_min_holder_count() -> usize {
    8
}
fn default_min_buy_sell_ratio() -> f64 {
    1.2
}
fn default_creator_rebuy_shadow_min_score() -> f64 {
    60.0
}
fn default_creator_rebuy_live_test_min_score() -> f64 {
    63.0
}
fn default_creator_rebuy_live_test_min_buy_pressure_pct() -> f64 {
    80.0
}
fn default_creator_rebuy_live_test_min_buy_sell_ratio() -> f64 {
    4.0
}
fn default_creator_rebuy_live_test_min_unique_buyers() -> usize {
    0
}
fn default_creator_rebuy_live_test_max_sell_count() -> u64 {
    3
}
fn default_creator_rebuy_live_test_min_initial_liquidity_sol() -> f64 {
    30.0
}
fn default_creator_rebuy_live_test_max_initial_liquidity_sol() -> f64 {
    80.0
}
fn default_creator_rebuy_live_test_max_open_positions() -> u32 {
    1
}
fn default_creator_rebuy_live_test_max_bc_progress_pct() -> f64 {
    45.0
}
fn default_creator_rebuy_live_test_min_total_volume_sol() -> f64 {
    30.0
}
fn default_creator_rebuy_live_test_min_whale_buy_sol() -> f64 {
    4.0
}
fn default_creator_rebuy_live_test_zero_sell_min_score() -> f64 {
    50.0
}
fn default_creator_rebuy_live_test_zero_sell_min_buy_pressure_pct() -> f64 {
    70.0
}
fn default_creator_rebuy_live_test_zero_sell_min_buy_sell_ratio() -> f64 {
    5.0
}
fn default_creator_rebuy_live_test_strong_flow_min_bc_progress_pct() -> f64 {
    20.0
}
fn default_max_bc_sell_count() -> u64 {
    40
}
fn default_min_sniper_score() -> f64 {
    60.0
}
fn default_bc_fast_track_min_score() -> f64 {
    65.0
}
fn default_standard_lane_enabled() -> bool {
    true
}
fn default_graduation_goplus_enabled() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct ExecutionConfig {
    pub buy_amount_sol: f64,
    /// Minimum buy amount for dynamic sizing (low-liquidity tokens).
    /// When set, position size scales linearly with pool liquidity.
    #[serde(default)]
    pub min_buy_sol: Option<f64>,
    /// Maximum buy amount for dynamic sizing (high-liquidity tokens).
    /// Defaults to buy_amount_sol if not set.
    #[serde(default)]
    pub max_buy_sol: Option<f64>,
    /// Liquidity threshold (SOL) below which a token is considered "low-liq".
    /// Used for dynamic sizing and tighter exit params. 0 = disabled.
    #[serde(default)]
    pub low_liq_threshold_sol: f64,
    pub slippage_bps: u64,
    pub max_open_positions: u32,
    pub tx_confirm_timeout_secs: u64,
    pub tx_confirm_poll_ms: u64,
    pub api_request_timeout_secs: u64,
    pub max_retries: u32,
    #[serde(default = "default_priority_fee_max_lamports")]
    pub priority_fee_max_lamports: u64,
    #[serde(default = "default_priority_level")]
    pub priority_level: String,
    /// Maximum allowed price move (%) between filter time and execution time.
    /// If the token price has already moved up more than this since detection,
    /// skip the trade to avoid chasing.  0 = disabled.
    #[serde(default = "default_max_entry_price_move_pct")]
    pub max_entry_price_move_pct: f64,
    /// Simulated slippage for paper trades (basis points).
    /// Applied as a penalty: entry price raised, exit price lowered.
    /// 0 = no simulated slippage (perfect fills).
    #[serde(default = "default_paper_slippage_bps")]
    pub paper_slippage_bps: u64,
    /// Simulated execution delay for paper exits (milliseconds).
    /// After the delay, the price is re-fetched via DexScreener to
    /// simulate realistic exit price drift.  0 = instant (no delay).
    #[serde(default = "default_paper_exit_delay_ms")]
    pub paper_exit_delay_ms: u64,
    /// When true, paper buys use a real Jupiter quote (same as exits) to
    /// derive entry price + token amount with real price impact, AND reject
    /// trades whose size would consume more than `paper_max_pool_fill_pct`
    /// of the bonding-curve pool. When false (legacy), buys use the
    /// DexScreener mid-price with only flat `paper_slippage_bps`, which
    /// produces fills no real tx could ever achieve on thin curves.
    #[serde(default = "default_paper_realistic_fills")]
    pub paper_realistic_fills: bool,
    /// Maximum % of `initial_liquidity_sol` a single paper buy is allowed to
    /// consume. Above this, the trade is skipped (would move the pool too
    /// much for a real fill). Only applies when `paper_realistic_fills=true`.
    #[serde(default = "default_paper_max_pool_fill_pct")]
    pub paper_max_pool_fill_pct: f64,
    /// Moonbag-targeting filter: minimum pool liquidity (SOL) at entry.
    /// Below this, paper-fill cap usually rejects anyway. 0 = disabled.
    #[serde(default = "default_moonbag_liq_min_sol")]
    pub moonbag_liq_min_sol: f64,
    /// Moonbag-targeting filter: maximum pool liquidity (SOL) at entry.
    /// Backtest (n=265): liq>=80 SOL = 1.6% hit-3x rate; cutting that band
    /// alone lifts portfolio ROI from +56% to +88%. 0 = disabled.
    #[serde(default = "default_moonbag_liq_max_sol")]
    pub moonbag_liq_max_sol: f64,
    /// Moonbag-targeting filter: maximum 24h USD volume at entry.
    /// Tokens with >$50k 24h volume are already discovered — moon rate drops.
    /// Read from `sniper_features.be_volume_24h_usd`. 0 = disabled.
    #[serde(default = "default_moonbag_vol_24h_usd_max")]
    pub moonbag_vol_24h_usd_max: f64,
    /// Optional fixed buy size for the creator-rebuy live canary. 0 = use
    /// normal dynamic sizing. When set, execution uses the smaller of this and
    /// the normal dynamic size.
    #[serde(default)]
    pub creator_rebuy_live_test_buy_amount_sol: f64,
}

fn default_paper_slippage_bps() -> u64 {
    0
}
fn default_paper_exit_delay_ms() -> u64 {
    0
}
fn default_paper_realistic_fills() -> bool {
    true
}
fn default_paper_max_pool_fill_pct() -> f64 {
    5.0
}
fn default_moonbag_liq_min_sol() -> f64 {
    0.0
}
fn default_moonbag_liq_max_sol() -> f64 {
    0.0
}
fn default_moonbag_vol_24h_usd_max() -> f64 {
    0.0
}
fn default_priority_fee_max_lamports() -> u64 {
    1_000_000
}
fn default_priority_level() -> String {
    "veryHigh".to_string()
}
fn default_max_entry_price_move_pct() -> f64 {
    50.0
}

#[derive(Debug, Deserialize)]
pub struct JitoConfig {
    pub tip_multiplier: f64,
}

#[derive(Debug, Deserialize)]
pub struct ExitConfig {
    pub tp1_multiplier: f64,
    pub tp1_sell_pct: u32,
    pub tp2_multiplier: f64,
    pub tp2_sell_pct: u32,
    pub tp3_multiplier: f64,
    pub stop_loss_pct: f64,
    /// Tighter stop when position has never been profitable (peak < entry).
    /// Applied only after `never_profitable_grace_secs`. 0 = use normal stop_loss_pct.
    #[serde(default = "default_never_profitable_stop_loss_pct")]
    pub never_profitable_stop_loss_pct: f64,
    /// Grace period (seconds) before the never-profitable stop activates.
    /// Gives the token time to find its legs after entry.
    #[serde(default = "default_never_profitable_grace_secs")]
    pub never_profitable_grace_secs: u64,
    pub max_hold_seconds: u64,
    pub volume_drop_threshold_pct: f64,
    pub trailing_stop_pct: f64,
    pub trailing_stop_min_multiplier: f64,
    /// When peak_multiplier exceeds this threshold, use a wider trailing stop %
    /// to give big runners more room to breathe.
    #[serde(default = "default_trailing_stop_adaptive_multiplier")]
    pub trailing_stop_adaptive_multiplier: f64,
    /// Wider trailing stop % used when peak > adaptive_multiplier.
    #[serde(default = "default_trailing_stop_adaptive_pct")]
    pub trailing_stop_adaptive_pct: f64,
    #[serde(default = "default_trailing_stop_post_tp1_pct")]
    pub trailing_stop_post_tp1_pct: f64,
    #[serde(default = "default_trailing_stop_post_tp1_floor")]
    pub trailing_stop_post_tp1_floor: bool,
    /// Post-TP2 moonbag trailing stop %.
    #[serde(default = "default_trailing_stop_post_tp2_pct")]
    pub trailing_stop_post_tp2_pct: f64,
    pub entry_confirmation_delay_secs: u64,
    pub entry_confirmation_checks: u64,
    /// Hard cap on realized loss per trade (%).  If the Jupiter exit quote
    /// shows a loss worse than this, force-execute immediately at current
    /// slippage rather than escalating retries (which make it worse).
    /// 0 = disabled.
    #[serde(default = "default_max_realized_loss_pct")]
    pub max_realized_loss_pct: f64,
    /// Minimum seconds before any stop-loss can fire.
    /// Gives entry chop room to settle.
    #[serde(default = "default_min_hold_before_stop_loss")]
    pub min_hold_before_stop_loss: u64,
    /// Low-liquidity override: tighter trailing stop (%). 0 = use normal.
    #[serde(default)]
    pub low_liq_trailing_stop_pct: f64,
    /// Low-liquidity override: shorter max hold (seconds). 0 = use normal.
    #[serde(default)]
    pub low_liq_max_hold_seconds: u64,
    /// Low-liquidity override: tighter stop loss (%). 0 = use normal.
    #[serde(default)]
    pub low_liq_stop_loss_pct: f64,
    /// Early momentum kill: seconds after entry before gate activates. 0 = disabled.
    #[serde(default)]
    pub momentum_kill_secs: u64,
    /// Early momentum kill: minimum multiplier required by momentum_kill_secs.
    /// If price < entry * this after momentum_kill_secs, exit.
    #[serde(default = "default_momentum_kill_min_multiplier")]
    pub momentum_kill_min_multiplier: f64,
}

fn default_never_profitable_stop_loss_pct() -> f64 {
    0.0
}
fn default_never_profitable_grace_secs() -> u64 {
    30
}
fn default_momentum_kill_min_multiplier() -> f64 {
    1.3
}
fn default_trailing_stop_post_tp1_pct() -> f64 {
    15.0
}
fn default_trailing_stop_post_tp1_floor() -> bool {
    true
}
fn default_trailing_stop_post_tp2_pct() -> f64 {
    20.0
}
fn default_max_realized_loss_pct() -> f64 {
    40.0
}
fn default_min_hold_before_stop_loss() -> u64 {
    5
}
fn default_trailing_stop_adaptive_multiplier() -> f64 {
    2.5
}
fn default_trailing_stop_adaptive_pct() -> f64 {
    40.0
}

#[derive(Debug, Deserialize)]
pub struct RiskConfig {
    pub daily_loss_limit_sol: f64,
    pub max_portfolio_exposure_sol: f64,
    pub min_sol_balance: f64,
}

#[derive(Debug, Deserialize)]
pub struct MonitoringConfig {
    pub monitor_interval_ms: u64,
    pub dev_wallet_check_interval_secs: u64,
    pub dev_wallet_rpc_timeout_secs: u64,
    pub dev_dump_threshold_pct: f64,
    pub max_sane_price_usd: f64,
    pub max_price_change_ratio: f64,
    pub price_timeout_secs: u64,
    #[serde(default = "default_lp_drop_threshold_pct")]
    pub lp_drop_threshold_pct: f64,
    /// Grace period (seconds) before LP removal watcher activates.
    /// Fresh PumpFun graduates shuffle LP during migration to PumpSwap;
    /// waiting avoids false liquidity_removed exits.
    #[serde(default = "default_lp_grace_period_secs")]
    pub lp_grace_period_secs: u64,

    // ── Tick stream & dip state machine ──
    /// Tick window duration in seconds (rolling window for momentum calc).
    #[serde(default = "default_tick_window_secs")]
    pub tick_window_secs: f64,
    /// Drawdown % from peak to enter DIP_WATCH.
    #[serde(default = "default_dip_threshold_pct")]
    pub dip_threshold_pct: f64,
    /// Grace period (seconds) to wait for recovery signals before selling.
    #[serde(default = "default_dip_grace_period_secs")]
    pub dip_grace_period_secs: f64,
    /// Momentum ratio (buy_vol / total_vol) above which recovery is detected.
    #[serde(default = "default_dip_recovery_buy_ratio")]
    pub dip_recovery_buy_ratio: f64,
    /// Minimum SOL volume in tick window to trust momentum signals.
    #[serde(default = "default_dip_min_volume_sol")]
    pub dip_min_volume_sol: f64,
    /// Single sell > this × avg trade size = whale dump (death signal).
    #[serde(default = "default_dip_whale_sell_multiplier")]
    pub dip_whale_sell_multiplier: f64,
    /// No trades for this many seconds during dip = token dead.
    #[serde(default = "default_dip_no_trades_timeout_secs")]
    pub dip_no_trades_timeout_secs: f64,
    /// Minimum position age (seconds) before dip_death can fire.
    /// Prevents premature exits on tokens that dip-then-rip right after entry.
    #[serde(default = "default_min_hold_before_dip_death")]
    pub min_hold_before_dip_death: u64,
    /// Grace window for strict creator-rebuy quality entries. During this
    /// window, soft dip-death and momentum-kill exits are suppressed, while
    /// hard protections (stop loss, dev dump, LP drain, whale dump) remain live.
    #[serde(default)]
    pub creator_rebuy_runner_grace_secs: u64,
    /// Peak multiplier that arms the strict creator-rebuy protected-runner state.
    /// Once armed, soft exits remain suppressed for `creator_rebuy_runner_protection_secs`.
    #[serde(default = "default_creator_rebuy_runner_protection_peak_multiplier")]
    pub creator_rebuy_runner_protection_peak_multiplier: f64,
    /// Protected-runner duration after the peak trigger arms. 0 = disabled.
    #[serde(default)]
    pub creator_rebuy_runner_protection_secs: u64,
    /// Profit floor for protected creator-rebuy runners. 0 = disabled.
    #[serde(default = "default_creator_rebuy_runner_floor_multiplier")]
    pub creator_rebuy_runner_floor_multiplier: f64,
    /// How long (seconds) to continue shadow-logging price after a position
    /// is closed. 0 = disabled.  Data is written to the `shadow_log` table.
    #[serde(default)]
    pub shadow_log_duration_secs: u64,
    /// Force-close any position still open after this many seconds.
    /// Safety net for stuck positions that somehow evade TimeStop.
    /// 0 = disabled.  Default: 1800 (30 minutes).
    #[serde(default = "default_stale_position_timeout_secs")]
    pub stale_position_timeout_secs: u64,

    // ── Helius WebSocket price stream ──
    /// Subscribe to pump.fun bonding-curve accounts via Helius Enhanced WS
    /// and serve cached prices instead of polling Jupiter for every tick.
    /// Falls back to Jupiter automatically for graduated / Raydium tokens.
    /// Default `false` — opt-in via config so a bad rollout can be reverted
    /// without recompiling.
    #[serde(default)]
    pub enable_helius_price_ws: bool,

    // ── Bags launch monitor (research-only) ──
    /// Poll the shared Bags authority for new launch transactions and record
    /// creator-side wallets plus early post-launch demand metrics.
    #[serde(default)]
    pub bags_launch_monitor_enabled: bool,
    /// Poll interval, in seconds, for checking new Bags authority signatures.
    #[serde(default = "default_bags_launch_poll_interval_secs")]
    pub bags_launch_poll_interval_secs: u64,
    /// Time window, in seconds, used to judge whether a Bags launch found real
    /// early demand after birth.
    #[serde(default = "default_bags_demand_window_secs")]
    pub bags_demand_window_secs: u64,
    /// Minimum number of distinct buyers inside the demand window before a
    /// Bags launch counts as having real demand.
    #[serde(default = "default_bags_real_demand_min_unique_buyers")]
    pub bags_real_demand_min_unique_buyers: usize,
    /// Minimum number of buy transactions inside the demand window before a
    /// Bags launch counts as having real demand.
    #[serde(default = "default_bags_real_demand_min_buy_txs")]
    pub bags_real_demand_min_buy_txs: usize,
    /// Minimum buy-side SOL volume flowing into the pool inside the demand
    /// window before a Bags launch counts as having real demand.
    #[serde(default = "default_bags_real_demand_min_buy_volume_sol")]
    pub bags_real_demand_min_buy_volume_sol: f64,
    /// Minimum number of scored launches required before a creator can be
    /// marked watchworthy.
    #[serde(default = "default_bags_creator_watch_min_launches")]
    pub bags_creator_watch_min_launches: usize,
    /// Minimum share of launches with real demand before a creator can be
    /// marked watchworthy.
    #[serde(default = "default_bags_creator_watch_min_demand_rate")]
    pub bags_creator_watch_min_demand_rate: f64,
    /// Enable a research-only shadow lane that fires only when a fresh Bags
    /// launch comes from a creator already marked watchworthy.
    #[serde(default)]
    pub bags_watchworthy_shadow_enabled: bool,
    /// Maximum age, in seconds, from launch discovery for the watchworthy
    /// Bags shadow lane to still fire.
    #[serde(default = "default_bags_watchworthy_shadow_max_age_seconds")]
    pub bags_watchworthy_shadow_max_age_seconds: u64,
    /// Maximum time, in seconds, to wait for a non-zero entry price before a
    /// Bags shadow row is marked price-unavailable.
    #[serde(default = "default_bags_shadow_entry_price_max_wait_secs")]
    pub bags_shadow_entry_price_max_wait_secs: u64,
    /// Poll interval, in seconds, for Bags shadow outcome tracking.
    #[serde(default = "default_bags_shadow_poll_interval_secs")]
    pub bags_shadow_poll_interval_secs: u64,
    /// Total duration, in seconds, to track Bags shadow outcomes.
    #[serde(default = "default_bags_shadow_duration_secs")]
    pub bags_shadow_duration_secs: u64,

    // ── Enrichment sampler (passive data collection, v6 prep) ──
    /// Enable scheduled + event-triggered enrichment snapshots written to
    /// `position_enrichment_snapshots`. Pure logging; no strategy change.
    #[serde(default)]
    pub enrichment_sampler_enabled: bool,
    /// Enable Tier 3 post-exit T+1h moonbag check. Requires sampler enabled.
    #[serde(default)]
    pub enrichment_post_exit_check_enabled: bool,

    // ── Narrative moonbag system ──
    /// Enable narrative-detection moonbag system. Requires OPENAI_API_KEY.
    #[serde(default)]
    pub narrative_check_enabled: bool,
    /// Seconds after entry to run each narrative check (decaying schedule).
    #[serde(default = "default_narrative_check_intervals")]
    pub narrative_check_intervals_secs: Vec<u64>,
    /// Maximum concurrent moonbag positions.
    #[serde(default = "default_moonbag_max_concurrent")]
    pub moonbag_max_concurrent: usize,
    /// Max hold hours for EARLY_ATTENTION moonbags.
    #[serde(default = "default_moonbag_max_hold_early_hours")]
    pub moonbag_max_hold_early_hours: u64,
    /// Max hold hours for EXPANDING_ATTENTION moonbags.
    #[serde(default = "default_moonbag_max_hold_expanding_hours")]
    pub moonbag_max_hold_expanding_hours: u64,
    /// Max hold hours for RUNNER_CONFIRMED moonbags.
    #[serde(default = "default_moonbag_max_hold_confirmed_hours")]
    pub moonbag_max_hold_confirmed_hours: u64,
    /// Floor multiplier — moonbag never exits below this × entry price.
    #[serde(default = "default_moonbag_floor_multiplier")]
    pub moonbag_floor_multiplier: f64,
    /// Initial trailing stop % for EARLY_ATTENTION (0-30m).
    #[serde(default = "default_moonbag_trailing_early")]
    pub moonbag_trailing_early: f64,
    /// Initial trailing stop % for EXPANDING_ATTENTION (0-30m).
    #[serde(default = "default_moonbag_trailing_expanding")]
    pub moonbag_trailing_expanding: f64,
    /// Initial trailing stop % for RUNNER_CONFIRMED (0-30m).
    #[serde(default = "default_moonbag_trailing_confirmed")]
    pub moonbag_trailing_confirmed: f64,
    /// Early grace window after moonbag promotion where 2x-5x runners get a
    /// wider trail before normal multiplier-based tightening resumes.
    #[serde(default)]
    pub moonbag_early_trailing_grace_secs: u64,
    /// Moonbag split exit: percent of the promoted moonbag stack to sell at 3x.
    #[serde(default = "default_moonbag_partial_3x_pct")]
    pub moonbag_partial_3x_pct: u8,
    /// Moonbag split exit: percent of the promoted moonbag stack to sell at 5x.
    #[serde(default = "default_moonbag_partial_5x_pct")]
    pub moonbag_partial_5x_pct: u8,
    /// Minimum age before soft/full moonbag trailing exits are allowed.
    /// Partial exits and floor/max-hold exits are still allowed.
    #[serde(default = "default_moonbag_min_hold_secs")]
    pub moonbag_min_hold_secs: u64,
    /// Price-only confirmation checks required before a soft trailing tail exit.
    /// This is the no-extra-API fallback for weak-flow confirmation.
    #[serde(default = "default_moonbag_trailing_confirm_checks")]
    pub moonbag_trailing_confirm_checks: u32,
    /// Multiplier-based trailing tiers for moonbag tails.
    #[serde(default = "default_moonbag_trail_2x_5x")]
    pub moonbag_trail_2x_5x: f64,
    #[serde(default = "default_moonbag_trail_5x_10x")]
    pub moonbag_trail_5x_10x: f64,
    #[serde(default = "default_moonbag_trail_10x_15x")]
    pub moonbag_trail_10x_15x: f64,
    #[serde(default = "default_moonbag_trail_15x_20x")]
    pub moonbag_trail_15x_20x: f64,
    #[serde(default = "default_moonbag_trail_20x_plus")]
    pub moonbag_trail_20x_plus: f64,
    /// Peak multiplier threshold before age-based trail decay kicks in.
    /// Below this, keep the initial wide trail to avoid premature exits.
    #[serde(default = "default_moonbag_profit_gate_multiplier")]
    pub moonbag_profit_gate_multiplier: f64,

    /// Minimum combined score (on-chain 0-70 + narrative 0-30) for moonbag promotion.
    /// Higher = stricter. 60 = needs either strong on-chain OR decent on-chain + narrative.
    #[serde(default = "default_moonbag_promotion_min_score")]
    pub moonbag_promotion_min_score: f64,

    /// Number of consecutive low-score narrative re-checks before state is downgraded.
    /// 0 = never downgrade (old behavior). 2 = require 2 bad checks in a row.
    #[serde(default = "default_moonbag_downgrade_consecutive")]
    pub moonbag_downgrade_consecutive: u32,

    /// Fast-runner threshold: if hold time at TP2 is below this (seconds),
    /// auto-promote to moonbag regardless of narrative score.
    /// A background narrative check fires immediately after promotion.
    #[serde(default = "default_fast_runner_threshold_secs")]
    pub fast_runner_threshold_secs: u64,

    /// Minimum dev holding (% of supply) to qualify for CTO path.
    /// Dev with <3% selling to 0 is noise, not a meaningful CTO.
    #[serde(default = "default_cto_min_dev_hold_pct")]
    pub cto_min_dev_hold_pct: f64,

    /// Staged evaluation checkpoints (seconds after CTO detected).
    /// 2min / 5min / 10min — gives community time to rally.
    #[serde(default = "default_cto_stage_secs")]
    pub cto_stage_secs: Vec<u64>,

    /// Strong CTO: price recovers to ≥ this % of pre-CTO level AND momentum >= strong threshold.
    #[serde(default = "default_cto_strong_recovery_pct")]
    pub cto_strong_recovery_pct: f64,

    /// Moderate CTO: price holds ≥ this % (but < strong) AND momentum >= moderate threshold.
    /// Below this at final stage = Failed CTO → exit.
    #[serde(default = "default_cto_moderate_recovery_pct")]
    pub cto_moderate_recovery_pct: f64,

    /// Hard collapse threshold: if price drops below this % of pre-CTO at any tick, instant exit.
    #[serde(default = "default_cto_collapse_pct")]
    pub cto_collapse_pct: f64,

    /// Early kill at stage 2 (5min): if momentum below this AND price making lower lows, exit early.
    #[serde(default = "default_cto_early_kill_momentum")]
    pub cto_early_kill_momentum: f64,

    /// Momentum threshold for strong CTO grading at final stage.
    #[serde(default = "default_cto_strong_momentum")]
    pub cto_strong_momentum: f64,

    /// Momentum threshold for moderate CTO grading at final stage.
    #[serde(default = "default_cto_moderate_momentum")]
    pub cto_moderate_momentum: f64,
}

fn default_lp_drop_threshold_pct() -> f64 {
    20.0
}
fn default_lp_grace_period_secs() -> u64 {
    45
}
fn default_tick_window_secs() -> f64 {
    5.0
}
fn default_dip_threshold_pct() -> f64 {
    15.0
}
fn default_dip_grace_period_secs() -> f64 {
    5.0
}
fn default_dip_recovery_buy_ratio() -> f64 {
    0.55
}
fn default_dip_min_volume_sol() -> f64 {
    0.5
}
fn default_dip_whale_sell_multiplier() -> f64 {
    2.5
}
fn default_dip_no_trades_timeout_secs() -> f64 {
    4.0
}
fn default_min_hold_before_dip_death() -> u64 {
    45
}
fn default_creator_rebuy_runner_protection_peak_multiplier() -> f64 {
    1.5
}
fn default_creator_rebuy_runner_floor_multiplier() -> f64 {
    1.15
}
fn default_stale_position_timeout_secs() -> u64 {
    1800
}
fn default_bags_launch_poll_interval_secs() -> u64 {
    15
}
fn default_bags_demand_window_secs() -> u64 {
    900
}
fn default_bags_real_demand_min_unique_buyers() -> usize {
    8
}
fn default_bags_real_demand_min_buy_txs() -> usize {
    6
}
fn default_bags_real_demand_min_buy_volume_sol() -> f64 {
    6.0
}
fn default_bags_creator_watch_min_launches() -> usize {
    2
}
fn default_bags_creator_watch_min_demand_rate() -> f64 {
    0.5
}
fn default_bags_watchworthy_shadow_max_age_seconds() -> u64 {
    180
}
fn default_bags_shadow_entry_price_max_wait_secs() -> u64 {
    90
}
fn default_bags_shadow_poll_interval_secs() -> u64 {
    60
}
fn default_bags_shadow_duration_secs() -> u64 {
    21_600
}
fn default_narrative_check_intervals() -> Vec<u64> {
    vec![120, 300]
}
fn default_moonbag_max_concurrent() -> usize {
    20
}
fn default_moonbag_max_hold_early_hours() -> u64 {
    12
}
fn default_moonbag_max_hold_expanding_hours() -> u64 {
    24
}
fn default_moonbag_max_hold_confirmed_hours() -> u64 {
    48
}
fn default_moonbag_floor_multiplier() -> f64 {
    1.2
}
fn default_moonbag_trailing_early() -> f64 {
    45.0
}
fn default_moonbag_trailing_expanding() -> f64 {
    55.0
}
fn default_moonbag_trailing_confirmed() -> f64 {
    55.0
}
fn default_moonbag_partial_3x_pct() -> u8 {
    30
}
fn default_moonbag_partial_5x_pct() -> u8 {
    20
}
fn default_moonbag_min_hold_secs() -> u64 {
    3600
}
fn default_moonbag_trailing_confirm_checks() -> u32 {
    2
}
fn default_moonbag_trail_2x_5x() -> f64 {
    70.0
}
fn default_moonbag_trail_5x_10x() -> f64 {
    50.0
}
fn default_moonbag_trail_10x_15x() -> f64 {
    40.0
}
fn default_moonbag_trail_15x_20x() -> f64 {
    30.0
}
fn default_moonbag_trail_20x_plus() -> f64 {
    20.0
}
fn default_moonbag_profit_gate_multiplier() -> f64 {
    2.0
}
fn default_moonbag_promotion_min_score() -> f64 {
    60.0
}
fn default_moonbag_downgrade_consecutive() -> u32 {
    2
}
fn default_fast_runner_threshold_secs() -> u64 {
    60
}
fn default_cto_min_dev_hold_pct() -> f64 {
    3.0
}
fn default_cto_stage_secs() -> Vec<u64> {
    vec![120, 300, 600]
}
fn default_cto_strong_recovery_pct() -> f64 {
    70.0
}
fn default_cto_moderate_recovery_pct() -> f64 {
    30.0
}
fn default_cto_collapse_pct() -> f64 {
    20.0
}
fn default_cto_early_kill_momentum() -> f64 {
    0.3
}
fn default_cto_strong_momentum() -> f64 {
    0.5
}
fn default_cto_moderate_momentum() -> f64 {
    0.4
}

// ── Env-sourced config ───────────────────────────────────────

#[derive(Debug)]
pub struct EnvConfig {
    // Network
    pub solana_network: String,

    // RPC
    pub solana_rpc_url: String,
    pub solana_ws_url: String,
    pub solana_rpc_backup_url: String,

    // Wallet (raw key kept private; only expose the keypair downstream)
    pub wallet_private_key: String,

    // Supabase
    pub supabase_url: String,
    pub supabase_service_key: String,

    // Detection
    pub detection_method: String,
    pub geyser_grpc_url: Option<String>,
    pub helius_api_key: Option<String>,
    pub poll_raydium: bool,
    pub poll_interval_seconds: u64,

    // Jito
    pub use_jito: bool,
    pub jito_block_engine_url: String,
    pub jito_max_tip_sol: f64,

    // Helius Sender (replaces Jito bundles — dual routes to validators + Jito)
    pub use_helius_sender: bool,
    pub helius_sender_url: String,
    pub helius_sender_tip_sol: f64,

    // Price data
    pub birdeye_api_key: Option<String>,

    // Solana Tracker
    pub solana_tracker_api_key: Option<String>,

    // Operational
    pub paper_trade: bool,
    pub log_level: String,

    // Telegram (optional pair)
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,

    // Helius WebSocket (optional — real-time dev wallet + LP monitoring)
    pub helius_ws_url: Option<String>,

    // Helius RPC (optional — derived from WS URL for DAS getAsset calls)
    pub helius_rpc_url: Option<String>,

    // OpenAI (optional — for narrative detection moonbag system)
    pub openai_api_key: Option<String>,

    // X API (optional — for tweet/user metrics in narrative scoring)
    pub x_api_bearer_token: Option<String>,

    // ── PR-F1: Chainstack Yellowstone gRPC ─────────────────────
    /// Base URL without path, e.g. `https://yellowstone-solana-mainnet.core.chainstack.com`.
    pub yellowstone_grpc_endpoint: Option<String>,
    /// x-token header auth (primary). Preferred over basic auth.
    pub yellowstone_grpc_x_token: Option<String>,
    /// Optional basic-auth fallback (used only if x-token is blank).
    pub yellowstone_grpc_username: Option<String>,
    pub yellowstone_grpc_password: Option<String>,
    /// Feature flag — when true (and endpoint/token present) the mux runs
    /// in place of `helius_price_ws`.
    pub enable_yellowstone_grpc: bool,
    /// Feature flag — when true, swap the PumpPortal WebSocket detection
    /// source for a Yellowstone gRPC pump.fun event stream (see
    /// `src/detection/yellowstone_pumpfun.rs`). Reuses the same
    /// `YELLOWSTONE_GRPC_*` credentials.
    pub use_grpc_pumpfun_detection: bool,
}

// ── Combined app config ──────────────────────────────────────

#[derive(Debug)]
pub struct AppConfig {
    pub env: EnvConfig,
    pub strategy: TomlConfig,
}

// ── Helpers ──────────────────────────────────────────────────

/// Read a required env var; panics with a clear message if missing or empty.
fn require_env(name: &str) -> String {
    match env::var(name) {
        Ok(val) if !val.trim().is_empty() => val.trim().to_string(),
        _ => panic!(
            "MISSING REQUIRED ENV VAR: `{}` is not set or is empty. Check your .env file.",
            name
        ),
    }
}

/// Read an optional env var; returns `None` if unset or empty.
fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.trim().to_string())
}

// ── Load & validate ──────────────────────────────────────────

impl AppConfig {
    /// Load config from `.env` + `config.toml`, validate, and return.
    /// Panics with a descriptive message on any missing/invalid value.
    pub fn load() -> Self {
        // ── 1. Load .env ────────────────────────────────────
        dotenv::dotenv().ok(); // .env is optional in CI; vars may come from environment

        // ── 2. Load config.toml ─────────────────────────────
        let toml_path = "config.toml";
        let toml_text = fs::read_to_string(toml_path).unwrap_or_else(|e| {
            panic!(
                "FAILED TO READ `{}`: {}. Make sure config.toml exists in the project root.",
                toml_path, e
            )
        });
        let strategy: TomlConfig = toml::from_str(&toml_text).unwrap_or_else(|e| {
            panic!(
                "FAILED TO PARSE `{}`: {}. Check config.toml for syntax errors.",
                toml_path, e
            )
        });

        // ── 3. Read required env vars ───────────────────────
        let solana_network = require_env("SOLANA_NETWORK");
        let solana_rpc_url = require_env("SOLANA_RPC_URL");
        let solana_ws_url = require_env("SOLANA_WS_URL");
        let solana_rpc_backup_url = require_env("SOLANA_RPC_BACKUP_URL");
        let wallet_private_key = require_env("WALLET_PRIVATE_KEY");
        let supabase_url = require_env("SUPABASE_URL");
        let supabase_service_key = require_env("SUPABASE_SERVICE_KEY");
        let detection_method = require_env("DETECTION_METHOD");
        let use_jito_str = optional_env("USE_JITO").unwrap_or_else(|| "false".to_string());
        let jito_block_engine_url = optional_env("JITO_BLOCK_ENGINE_URL").unwrap_or_default();
        let jito_max_tip_str =
            optional_env("JITO_MAX_TIP_SOL").unwrap_or_else(|| "0.02".to_string());
        let paper_trade_str = require_env("PAPER_TRADE");

        // Helius Sender (optional — defaults to disabled)
        let use_helius_sender = optional_env("USE_HELIUS_SENDER")
            .map(|v| v == "true")
            .unwrap_or(false);
        let helius_sender_url = optional_env("HELIUS_SENDER_URL")
            .unwrap_or_else(|| "https://sender.helius-rpc.com/fast".to_string());
        let helius_sender_tip_sol: f64 = optional_env("HELIUS_SENDER_TIP_SOL")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0005);
        let log_level = require_env("LOG_LEVEL");

        // Optional vars
        let geyser_grpc_url = optional_env("GEYSER_GRPC_URL");
        let helius_api_key = optional_env("HELIUS_API_KEY");
        let birdeye_api_key = optional_env("BIRDEYE_API_KEY");
        let solana_tracker_api_key = optional_env("SOLANA_TRACKER_API_KEY");
        let telegram_bot_token = optional_env("TELEGRAM_BOT_TOKEN");
        let telegram_chat_id = optional_env("TELEGRAM_CHAT_ID");
        let helius_ws_url = optional_env("HELIUS_WS_URL");

        // Derive Helius RPC URL from WebSocket URL (wss:// → https://)
        let helius_rpc_url = helius_ws_url
            .as_ref()
            .map(|ws| ws.replace("wss://", "https://"));

        let poll_raydium = optional_env("POLL_RAYDIUM")
            .map(|v| v == "true")
            .unwrap_or(true);
        let poll_interval_seconds: u64 = optional_env("POLL_INTERVAL_SECONDS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);

        // ── 4. Validate ─────────────────────────────────────

        // SOLANA_NETWORK
        if solana_network != "devnet" && solana_network != "mainnet" {
            panic!(
                "INVALID SOLANA_NETWORK: `{}`. Must be exactly \"devnet\" or \"mainnet\".",
                solana_network
            );
        }

        // SOLANA_RPC_URL
        if !solana_rpc_url.starts_with("https://") {
            panic!(
                "INVALID SOLANA_RPC_URL: `{}`. Must start with \"https://\".",
                solana_rpc_url
            );
        }

        // SOLANA_WS_URL
        if !solana_ws_url.starts_with("wss://") {
            panic!(
                "INVALID SOLANA_WS_URL: `{}`. Must start with \"wss://\".",
                solana_ws_url
            );
        }

        // WALLET_PRIVATE_KEY — must decode as valid base58 byte array
        let key_bytes = bs58::decode(&wallet_private_key)
            .into_vec()
            .unwrap_or_else(|e| {
                panic!(
                    "INVALID WALLET_PRIVATE_KEY: failed to decode base58: {}. \
                     Ensure this is the base58-encoded private key from bot-wallet.json.",
                    e
                );
            });
        if key_bytes.len() != 64 {
            panic!(
                "INVALID WALLET_PRIVATE_KEY: decoded to {} bytes, expected 64. \
                 Make sure you copied the full keypair (not just the public key).",
                key_bytes.len()
            );
        }

        // SUPABASE_URL
        if !supabase_url.starts_with("https://") || !supabase_url.ends_with(".supabase.co") {
            panic!(
                "INVALID SUPABASE_URL: `{}`. Must start with \"https://\" and end with \".supabase.co\".",
                supabase_url
            );
        }

        // DETECTION_METHOD
        if detection_method != "pumpfun_ws" && detection_method != "geyser" {
            panic!(
                "INVALID DETECTION_METHOD: `{}`. Must be \"pumpfun_ws\" or \"geyser\".",
                detection_method
            );
        }

        // If geyser, GEYSER_GRPC_URL must be set
        if detection_method == "geyser" && geyser_grpc_url.is_none() {
            panic!(
                "MISSING GEYSER_GRPC_URL: DETECTION_METHOD is \"geyser\" but GEYSER_GRPC_URL is not set. \
                 Provide a Geyser gRPC endpoint or switch DETECTION_METHOD to \"pumpfun_ws\"."
            );
        }

        // USE_JITO
        let use_jito = match use_jito_str.as_str() {
            "true" => true,
            "false" => false,
            _ => panic!(
                "INVALID USE_JITO: `{}`. Must be \"true\" or \"false\".",
                use_jito_str
            ),
        };

        // JITO_MAX_TIP_SOL — only validate when Jito is enabled
        let jito_max_tip_sol: f64 = jito_max_tip_str.parse().unwrap_or(0.02);
        if use_jito {
            if jito_max_tip_sol <= 0.0 {
                panic!(
                    "INVALID JITO_MAX_TIP_SOL: `{}`. Must be a positive number.",
                    jito_max_tip_sol
                );
            }
            if jito_max_tip_sol > 0.5 {
                panic!(
                    "INVALID JITO_MAX_TIP_SOL: `{}` exceeds safety cap of 0.5 SOL.",
                    jito_max_tip_sol
                );
            }
            if jito_block_engine_url.is_empty() {
                panic!(
                    "MISSING JITO_BLOCK_ENGINE_URL: USE_JITO is true but JITO_BLOCK_ENGINE_URL is not set."
                );
            }
        }

        // PAPER_TRADE
        let paper_trade = match paper_trade_str.as_str() {
            "true" => true,
            "false" => false,
            _ => panic!(
                "INVALID PAPER_TRADE: `{}`. Must be \"true\" or \"false\".",
                paper_trade_str
            ),
        };

        // LOG_LEVEL — accept simple levels or tracing EnvFilter directives
        // e.g. "info", "debug", "info,solana_memecoin_bot=debug"
        let valid_simple_levels = ["debug", "info", "warn", "error"];
        let base_level = log_level.split(',').next().unwrap_or("");
        if !valid_simple_levels.contains(&base_level.trim()) {
            panic!(
                "INVALID LOG_LEVEL: `{}`. Must start with one of: debug, info, warn, error. \
                 You may append per-crate overrides like: info,solana_memecoin_bot=debug",
                log_level
            );
        }

        // Telegram: if bot token is set, chat ID must also be set
        if telegram_bot_token.is_some() && telegram_chat_id.is_none() {
            panic!(
                "MISSING TELEGRAM_CHAT_ID: TELEGRAM_BOT_TOKEN is set but TELEGRAM_CHAT_ID is empty. \
                 Provide your Telegram chat ID or remove the bot token."
            );
        }

        // ── 5. Assemble ─────────────────────────────────────
        let env = EnvConfig {
            solana_network,
            solana_rpc_url,
            solana_ws_url,
            solana_rpc_backup_url,
            wallet_private_key,
            supabase_url,
            supabase_service_key,
            detection_method,
            geyser_grpc_url,
            helius_api_key,
            poll_raydium,
            poll_interval_seconds,
            use_jito,
            jito_block_engine_url,
            jito_max_tip_sol,
            use_helius_sender,
            helius_sender_url,
            helius_sender_tip_sol,
            birdeye_api_key,
            solana_tracker_api_key,
            paper_trade,
            log_level,
            telegram_bot_token,
            telegram_chat_id,
            helius_ws_url,
            helius_rpc_url,
            openai_api_key: optional_env("OPENAI_API_KEY"),
            x_api_bearer_token: optional_env("X_API_BEARER_TOKEN"),
            yellowstone_grpc_endpoint: optional_env("YELLOWSTONE_GRPC_ENDPOINT"),
            yellowstone_grpc_x_token: optional_env("YELLOWSTONE_GRPC_X_TOKEN"),
            yellowstone_grpc_username: optional_env("YELLOWSTONE_GRPC_USERNAME"),
            yellowstone_grpc_password: optional_env("YELLOWSTONE_GRPC_PASSWORD"),
            enable_yellowstone_grpc: optional_env("ENABLE_YELLOWSTONE_GRPC")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            use_grpc_pumpfun_detection: optional_env("USE_GRPC_PUMPFUN_DETECTION")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        };

        AppConfig { env, strategy }
    }
}
