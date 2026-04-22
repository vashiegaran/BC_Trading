//! Phase 0: Bonding-curve pre-checks.
//!
//! Runs token_safety, goplus, and rugcheck checks **before** graduation for
//! tokens that show promising bonding-curve activity (threshold-gated to
//! avoid burning API rate limits on the thousands of tokens that never graduate).
//!
//! Results are stored in a shared, TTL-evicting cache that the Phase 1 fast
//! gate can query at graduation time to skip redundant checks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::logger::SupabaseClient;
use super::goplus::GoPlusFilter;
use super::rugcheck::RugCheckFilter;
use super::token_safety::TokenSafetyFilter;
use super::types::FilterResult;

// ── Thresholds: only pre-check tokens that look promising ────

/// Minimum unique buyers before triggering pre-checks.
const MIN_UNIQUE_BUYERS: usize = 3;
/// Minimum buy-pressure % before triggering pre-checks.
const MIN_BUY_PRESSURE_PCT: f64 = 50.0;
/// Minimum SOL volume on bonding curve before triggering pre-checks.
const MIN_VOLUME_SOL: f64 = 0.5;
/// Maximum cache entries before LRU eviction.
const MAX_CACHE_SIZE: usize = 500;
/// Cache entries older than this are stale and ignored / evicted.
const CACHE_TTL: Duration = Duration::from_secs(600); // 10 minutes

// ── Cached result ────────────────────────────────────────────

/// Result of a single pre-check for a mint.
#[derive(Debug, Clone)]
pub struct PreCheckResult {
    pub token_safety: FilterResult,
    pub goplus: FilterResult,
    pub rugcheck: FilterResult,
    pub rugcheck_score: Option<f64>,
    pub inserted_at: Instant,
}

impl PreCheckResult {
    /// Whether ALL pre-checks passed.
    pub fn all_passed(&self) -> bool {
        self.token_safety.passed && self.goplus.passed && self.rugcheck.passed
    }

    /// Whether any result marks a critical danger (honeypot, mintable, etc.).
    pub fn has_critical_danger(&self) -> bool {
        Self::is_critical_fail(&self.goplus) || Self::is_critical_fail(&self.token_safety)
    }

    fn is_critical_fail(r: &FilterResult) -> bool {
        if r.passed {
            return false;
        }
        match r.fail_reason.as_deref() {
            Some(reason) => {
                reason.contains("honeypot")
                    || reason.contains("mintable")
                    || reason.contains("mint_authority_not_revoked")
                    || reason.contains("freeze_authority_not_revoked")
                    || reason.contains("transfer_pausable")
                    || reason.contains("blacklist")
            }
            None => false,
        }
    }
}

// ── Shared cache ─────────────────────────────────────────────

/// Thread-safe cache of Phase-0 pre-check results, keyed by mint address.
///
/// Shared between the detection engine (which writes) and the filter engine
/// (which reads) via `Arc<PreCheckCache>`.
#[derive(Clone)]
pub struct PreCheckCache {
    inner: Arc<RwLock<HashMap<String, PreCheckResult>>>,
    /// Set of mints already dispatched for pre-checking, so we don't
    /// fire duplicate requests when the threshold is crossed repeatedly.
    dispatched: Arc<RwLock<HashMap<String, Instant>>>,
}

impl PreCheckCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            dispatched: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Look up a cached pre-check result. Returns `None` if missing or stale.
    pub async fn get(&self, mint: &str) -> Option<PreCheckResult> {
        let cache = self.inner.read().await;
        cache.get(mint).and_then(|entry| {
            if entry.inserted_at.elapsed() < CACHE_TTL {
                Some(entry.clone())
            } else {
                None
            }
        })
    }

    /// Insert a completed pre-check result.
    async fn insert(&self, mint: String, result: PreCheckResult) {
        let mut cache = self.inner.write().await;
        cache.insert(mint, result);

        // Evict stale entries if cache is getting large
        if cache.len() > MAX_CACHE_SIZE {
            cache.retain(|_, v| v.inserted_at.elapsed() < CACHE_TTL);
        }
    }

    /// Check whether a mint has already been dispatched for pre-checking.
    async fn already_dispatched(&self, mint: &str) -> bool {
        let dispatched = self.dispatched.read().await;
        if let Some(ts) = dispatched.get(mint) {
            // Allow re-dispatch if the original dispatch was very old (stale)
            ts.elapsed() < CACHE_TTL
        } else {
            false
        }
    }

    /// Mark a mint as dispatched.
    async fn mark_dispatched(&self, mint: String) {
        let mut dispatched = self.dispatched.write().await;
        dispatched.insert(mint, Instant::now());

        // Evict stale dispatch records
        if dispatched.len() > MAX_CACHE_SIZE * 2 {
            dispatched.retain(|_, ts| ts.elapsed() < CACHE_TTL);
        }
    }
}

// ── Threshold check ──────────────────────────────────────────

/// Evaluate whether a bonding-curve token looks promising enough to
/// warrant spending API calls on pre-checks.
pub fn meets_precheck_threshold(
    unique_buyers: usize,
    buy_pressure_pct: f64,
    total_volume_sol: f64,
) -> bool {
    unique_buyers >= MIN_UNIQUE_BUYERS
        && buy_pressure_pct >= MIN_BUY_PRESSURE_PCT
        && total_volume_sol >= MIN_VOLUME_SOL
}

// ── Spawn pre-check ──────────────────────────────────────────

/// Check the threshold and, if met, spawn a background task that runs
/// token_safety + goplus + rugcheck and stores the result in the cache.
///
/// Called from `pumpfun_ws::handle_token_trade` on each trade update.
/// Returns immediately (non-blocking).
pub async fn maybe_spawn_precheck(
    mint: String,
    unique_buyers: usize,
    buy_pressure_pct: f64,
    total_volume_sol: f64,
    cfg: Arc<AppConfig>,
    cache: PreCheckCache,
    supabase: Arc<SupabaseClient>,
) {
    if !meets_precheck_threshold(unique_buyers, buy_pressure_pct, total_volume_sol) {
        return;
    }

    if cache.already_dispatched(&mint).await {
        return;
    }

    cache.mark_dispatched(mint.clone()).await;

    info!(
        mint = %mint,
        unique_buyers = unique_buyers,
        buy_pressure = format!("{:.1}%", buy_pressure_pct),
        volume_sol = format!("{:.2}", total_volume_sol),
        "🔬 Phase 0: bonding-curve token looks promising — spawning pre-checks"
    );

    tokio::spawn(async move {
        run_prechecks(mint, unique_buyers, buy_pressure_pct, total_volume_sol, cfg, cache, supabase).await;
    });
}

/// Actually run the three pre-checks and store the results.
async fn run_prechecks(
    mint: String,
    unique_buyers: usize,
    buy_pressure_pct: f64,
    total_volume_sol: f64,
    cfg: Arc<AppConfig>,
    cache: PreCheckCache,
    supabase: Arc<SupabaseClient>,
) {
    let start = Instant::now();

    let token_safety = TokenSafetyFilter::new();
    let goplus = GoPlusFilter::new();
    let rugcheck = RugCheckFilter::new();

    let (safety_result, goplus_result, (rugcheck_result, rugcheck_score)) = tokio::join!(
        token_safety.check(&mint, &cfg),
        goplus.check(&mint, &cfg),
        rugcheck.check(&mint, &cfg),
    );

    let elapsed = start.elapsed();

    let result = PreCheckResult {
        token_safety: safety_result,
        goplus: goplus_result,
        rugcheck: rugcheck_result,
        rugcheck_score,
        inserted_at: Instant::now(),
    };

    let all_passed = result.all_passed();
    let has_danger = result.has_critical_danger();

    info!(
        mint = %mint,
        elapsed_ms = elapsed.as_millis() as u64,
        all_passed = all_passed,
        has_critical_danger = has_danger,
        rugcheck_score = rugcheck_score,
        "🔬 Phase 0 pre-check complete"
    );

    if has_danger {
        warn!(
            mint = %mint,
            token_safety = %result.token_safety.fail_reason.as_deref().unwrap_or("pass"),
            goplus = %result.goplus.fail_reason.as_deref().unwrap_or("pass"),
            "⚠️ Phase 0: critical danger detected — token will be rejected at graduation"
        );
    }

    // ── Log to Supabase precheck_log (fire-and-forget) ──
    let payload = serde_json::json!({
        "mint": mint,
        "token_safety_passed": result.token_safety.passed,
        "token_safety_reason": result.token_safety.fail_reason,
        "goplus_passed": result.goplus.passed,
        "goplus_reason": result.goplus.fail_reason,
        "rugcheck_passed": result.rugcheck.passed,
        "rugcheck_reason": result.rugcheck.fail_reason,
        "rugcheck_score": result.rugcheck_score,
        "all_passed": all_passed,
        "has_critical_danger": has_danger,
        "elapsed_ms": elapsed.as_millis() as u64,
        "unique_buyers_at_trigger": unique_buyers,
        "buy_pressure_at_trigger": buy_pressure_pct,
        "volume_sol_at_trigger": total_volume_sol,
        "checked_at": chrono::Utc::now().to_rfc3339(),
    });
    let url = format!("{}/precheck_log", supabase.base_url);
    tokio::spawn(async move {
        let _ = supabase.client.post(&url).json(&payload).send().await;
    });

    cache.insert(mint, result).await;
}
