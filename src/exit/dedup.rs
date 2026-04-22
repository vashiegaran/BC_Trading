//! In-flight exit deduplication.
//!
//! The exit engine historically had no guard against two `ExitSignal`s for the
//! same position being processed concurrently. If monitoring fired TP1 and a
//! TrailingStop within the same few hundred milliseconds, both signals would
//! spawn their own `execute_real_exit` task, both would pass the
//! `status != "closed"` check (it runs *after* the swap submits), and both
//! would race to sell the wallet balance. The result was conflicting DB
//! writes and, in the worst case, a second sell attempt after the position
//! was already closed.
//!
//! [`DedupRegistry`] fixes that with a narrow, process-local lock keyed by
//! `position_id`. Callers acquire a guard before spawning the exit task; the
//! guard releases on drop so a legitimate follow-up (e.g. a retry after a
//! transient failure left the position open) can proceed.
//!
//! This is intentionally not a distributed lock. It only prevents
//! double-firing inside the same running bot instance. Duplicate recovery
//! across restarts is handled separately by the `recovery_closed` path.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Shared registry of positions with an exit currently in flight.
#[derive(Debug, Default, Clone)]
pub struct DedupRegistry {
    inner: Arc<Mutex<HashSet<i64>>>,
}

impl DedupRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to claim `position_id` for an exit attempt.
    ///
    /// Returns `Some(guard)` if this caller is the first to claim it. The
    /// guard releases the claim automatically when it is dropped.
    ///
    /// Returns `None` if another task already holds the claim — the caller
    /// should drop the signal rather than spawn a second exit.
    pub fn try_acquire(&self, position_id: i64) -> Option<DedupGuard> {
        let mut set = self.inner.lock().expect("exit dedup mutex poisoned");
        if set.insert(position_id) {
            Some(DedupGuard {
                position_id,
                inner: Arc::clone(&self.inner),
            })
        } else {
            None
        }
    }

    /// Count of positions currently held. Useful for health metrics.
    pub fn in_flight(&self) -> usize {
        self.inner.lock().map(|s| s.len()).unwrap_or(0)
    }
}

/// RAII guard that releases the dedup claim on drop.
#[derive(Debug)]
pub struct DedupGuard {
    position_id: i64,
    inner: Arc<Mutex<HashSet<i64>>>,
}

impl DedupGuard {
    pub fn position_id(&self) -> i64 {
        self.position_id
    }
}

impl Drop for DedupGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.inner.lock() {
            set.remove(&self.position_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_for_same_id_fails() {
        let reg = DedupRegistry::new();
        let g1 = reg.try_acquire(42);
        assert!(g1.is_some());
        assert!(reg.try_acquire(42).is_none());
        assert_eq!(reg.in_flight(), 1);
    }

    #[test]
    fn releases_on_drop() {
        let reg = DedupRegistry::new();
        {
            let _g = reg.try_acquire(7);
            assert_eq!(reg.in_flight(), 1);
        }
        assert_eq!(reg.in_flight(), 0);
        assert!(reg.try_acquire(7).is_some());
    }

    #[test]
    fn independent_positions_do_not_collide() {
        let reg = DedupRegistry::new();
        let _a = reg.try_acquire(1);
        let _b = reg.try_acquire(2);
        assert_eq!(reg.in_flight(), 2);
    }
}
