//! Rate limiter + circuit breaker for enrichment-sampler API calls.
//!
//! Each external API (Birdeye, Helius DAS, SolanaTracker, DexScreener, Jupiter)
//! gets its own `ApiGuard`:
//!   • a **semaphore** caps concurrent calls to avoid in-flight pile-up
//!   • a **min-interval** delay spaces calls to respect per-second limits
//!   • a **circuit breaker** trips on repeated failures (3x within 60s) and
//!     stays OPEN for 5min so a dead/throttled API doesn't burn retries
//!
//! Callers use `ApiGuard::acquire()` which returns `Some(permit)` if the call
//! should proceed, or `None` if the circuit is open (fail fast, log null).

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{debug, warn};

const FAILURE_WINDOW_MS: u64 = 60_000;   // 60 s
const FAILURE_THRESHOLD: u32 = 3;
const COOLDOWN_MS: u64 = 5 * 60 * 1000;  // 5 min
const DEFAULT_ACQUIRE_TIMEOUT_MS: u64 = 500;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Guarded access to a single upstream API.
pub struct ApiGuard {
    pub name: &'static str,
    sem: Arc<Semaphore>,
    /// Minimum milliseconds between call-start events for this API.
    min_interval_ms: u64,
    last_call_ms: AtomicU64,
    /// Number of failures observed in the current rolling window.
    failure_count: AtomicU32,
    /// Timestamp (ms) of the first failure in the current window.
    window_start_ms: AtomicU64,
    /// Timestamp (ms) when the circuit is allowed to try again. 0 = closed.
    open_until_ms: AtomicU64,
}

impl ApiGuard {
    /// `max_concurrent` — cap of in-flight requests.
    /// `min_interval_ms` — minimum spacing between calls (e.g. 200 for 5 RPS).
    pub fn new(name: &'static str, max_concurrent: usize, min_interval_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            name,
            sem: Arc::new(Semaphore::new(max_concurrent)),
            min_interval_ms,
            last_call_ms: AtomicU64::new(0),
            failure_count: AtomicU32::new(0),
            window_start_ms: AtomicU64::new(0),
            open_until_ms: AtomicU64::new(0),
        })
    }

    /// True if the circuit is currently OPEN (skip calls).
    pub fn is_open(&self) -> bool {
        now_ms() < self.open_until_ms.load(Ordering::Relaxed)
    }

    /// Acquire a permit. Returns `None` if:
    ///   • the circuit is open, OR
    ///   • no slot was free within `DEFAULT_ACQUIRE_TIMEOUT_MS`.
    /// Caller must drop the permit when finished.
    pub async fn acquire(&self) -> Option<SemaphorePermit<'_>> {
        if self.is_open() {
            debug!(api = self.name, "circuit OPEN — skipping");
            return None;
        }
        let permit = tokio::time::timeout(
            Duration::from_millis(DEFAULT_ACQUIRE_TIMEOUT_MS),
            self.sem.acquire(),
        )
        .await
        .ok()?
        .ok()?;

        // Enforce min interval between starts.
        let now = now_ms();
        let last = self.last_call_ms.load(Ordering::Relaxed);
        let since_last = now.saturating_sub(last);
        if since_last < self.min_interval_ms {
            let wait = self.min_interval_ms - since_last;
            tokio::time::sleep(Duration::from_millis(wait)).await;
        }
        self.last_call_ms.store(now_ms(), Ordering::Relaxed);
        Some(permit)
    }

    /// Record a successful response — resets failure counters.
    pub fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.window_start_ms.store(0, Ordering::Relaxed);
    }

    /// Record a failure (timeout, 429, 5xx, non-200).
    /// If threshold crossed within the rolling window, TRIP the circuit.
    pub fn record_failure(&self, reason: &str) {
        let now = now_ms();
        let window_start = self.window_start_ms.load(Ordering::Relaxed);

        // Reset window if expired.
        if window_start == 0 || now.saturating_sub(window_start) > FAILURE_WINDOW_MS {
            self.window_start_ms.store(now, Ordering::Relaxed);
            self.failure_count.store(1, Ordering::Relaxed);
            return;
        }

        let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= FAILURE_THRESHOLD {
            let until = now + COOLDOWN_MS;
            self.open_until_ms.store(until, Ordering::Relaxed);
            warn!(
                api = self.name,
                failures = count,
                reason,
                cooldown_secs = COOLDOWN_MS / 1000,
                "🔌 Circuit TRIPPED — pausing API calls"
            );
            // Reset window so post-cooldown starts fresh.
            self.failure_count.store(0, Ordering::Relaxed);
            self.window_start_ms.store(0, Ordering::Relaxed);
        }
    }
}

/// Bundle of all guards used by the enrichment sampler.
#[derive(Clone)]
pub struct SamplerGuards {
    pub helius_rpc: Arc<ApiGuard>,
    pub helius_das: Arc<ApiGuard>,
    pub birdeye: Arc<ApiGuard>,
    pub dexscreener: Arc<ApiGuard>,
    pub solana_tracker: Arc<ApiGuard>,
    pub jupiter: Arc<ApiGuard>,
}

impl SamplerGuards {
    pub fn new() -> Self {
        Self {
            // Helius paid plan 50 RPS → cap sampler to 8 concurrent, 150ms min spacing
            helius_rpc: ApiGuard::new("helius_rpc", 8, 150),
            // DAS 5 RPS → cap 2 concurrent, 250ms spacing
            helius_das: ApiGuard::new("helius_das", 2, 250),
            // Birdeye plan-dependent → cap 2 concurrent, 700ms spacing (~85 req/min)
            birdeye: ApiGuard::new("birdeye", 2, 700),
            // DexScreener 300 req/min → cap 3 concurrent, 250ms spacing
            dexscreener: ApiGuard::new("dexscreener", 3, 250),
            // SolanaTracker 100 req/min → cap 2 concurrent, 700ms spacing
            solana_tracker: ApiGuard::new("solana_tracker", 2, 700),
            // Jupiter free → cap 3 concurrent, 250ms spacing
            jupiter: ApiGuard::new("jupiter", 3, 250),
        }
    }
}

impl Default for SamplerGuards {
    fn default() -> Self {
        Self::new()
    }
}
