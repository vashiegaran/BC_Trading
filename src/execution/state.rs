use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::logger::SupabaseClient;

/// In-memory trading state that replaces Supabase reads on the critical buy path.
///
/// Safety checks (open position count, exposure, daily PnL, dedup) are read
/// from this cache instead of querying Supabase.  The cache is updated
/// synchronously on buy/exit, and Supabase writes happen in the background.
#[derive(Debug)]
pub struct TradingState {
    inner: RwLock<TradingStateInner>,
}

#[derive(Debug)]
struct TradingStateInner {
    /// Mints with open/partial positions (no exit_tx_sig yet).
    open_mints: HashSet<String>,
    /// When each open mint was added (for stale position detection).
    open_since: HashMap<String, Instant>,
    /// Total SOL exposure across all open positions.
    total_exposure_sol: f64,
    /// Today's realized PnL (accumulated from exits).
    today_pnl_sol: f64,
    /// Date string for PnL reset (e.g. "2026-03-21").
    pnl_date: String,
    /// Dev wallets known to dump — skip tokens from these creators.
    /// Seeded with known bad devs and updated dynamically on DevWalletDumping exits.
    blacklisted_devs: HashSet<String>,
    /// Mints recently exited — prevents immediate re-buy of the same token.
    /// Maps mint → exit timestamp. Entries older than REBUY_COOLDOWN_SECS are ignored.
    recently_exited: HashMap<String, Instant>,
    /// Mints with an in-flight buy reservation (between dedup check and record_buy).
    /// Prevents race where two near-simultaneous events both pass the dedup check
    /// before either has called `record_buy`. Cleared by `record_buy` on success
    /// or `release_reservation` on failure.
    pending_mints: HashSet<String>,
}

impl TradingState {
    /// Create a new empty TradingState.
    pub fn new() -> Arc<Self> {
        // Seed with known bad dev wallets from historical trade data.
        let mut blacklisted_devs = HashSet::new();
        blacklisted_devs.insert("D9gQ6RhKEpnobPBUdWY5bPQt2p3zGk3iVz6ChpUi2ArA".to_string());

        Arc::new(Self {
            inner: RwLock::new(TradingStateInner {
                open_mints: HashSet::new(),
                open_since: HashMap::new(),
                total_exposure_sol: 0.0,
                today_pnl_sol: 0.0,
                pnl_date: today_str(),
                blacklisted_devs,
                recently_exited: HashMap::new(),
                pending_mints: HashSet::new(),
            }),
        })
    }

    /// Hydrate state from Supabase on startup.
    /// Called once before the engines start.
    pub async fn hydrate_from_supabase(self: &Arc<Self>, supabase: &SupabaseClient, paper_trade: bool) {
        let status_filter = if paper_trade {
            "or=(status.eq.paper,status.eq.partial)"
        } else {
            "or=(status.eq.open,status.eq.partial)"
        };

        // Fetch open positions (mints + exposure)
        let url = format!(
            "{}/positions?select=mint,sol_spent&{}&is_paper_trade=eq.{}&exit_tx_sig=is.null",
            supabase.base_url, status_filter, paper_trade
        );

        let rows: Vec<serde_json::Value> = match supabase.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => resp.json().await.unwrap_or_default(),
            _ => {
                warn!("Failed to hydrate TradingState from Supabase — starting empty");
                vec![]
            }
        };

        let mut inner = self.inner.write().await;
        for row in &rows {
            if let Some(mint) = row.get("mint").and_then(|v| v.as_str()) {
                inner.open_mints.insert(mint.to_string());
            }
            if let Some(sol) = row.get("sol_spent").and_then(|v| v.as_f64()) {
                inner.total_exposure_sol += sol;
            }
        }

        // Fetch today's PnL
        let today = today_str();
        let pnl_url = format!(
            "{}/positions?select=pnl_sol,sol_received,exit_reason&status=eq.closed&is_paper_trade=eq.{}&exit_time=gte.{}",
            supabase.base_url, paper_trade, today
        );

        let pnl_rows: Vec<serde_json::Value> = match supabase.client.get(&pnl_url).send().await {
            Ok(resp) if resp.status().is_success() => resp.json().await.unwrap_or_default(),
            _ => vec![],
        };

        let today_pnl: f64 = pnl_rows
            .iter()
            .filter_map(|r| {
                let pnl = r.get("pnl_sol").and_then(|v| v.as_f64())?;
                let sol_received = r.get("sol_received").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let exit_reason = r.get("exit_reason").and_then(|v| v.as_str()).unwrap_or("");
                // Skip ghost positions
                if exit_reason.contains("recovery_closed") && sol_received <= 0.0 {
                    return None;
                }
                Some(pnl)
            })
            .sum();

        inner.today_pnl_sol = today_pnl;
        inner.pnl_date = today;

        info!(
            open_positions = inner.open_mints.len(),
            exposure_sol = format!("{:.4}", inner.total_exposure_sol),
            today_pnl = format!("{:.4}", inner.today_pnl_sol),
            "📊 TradingState hydrated from Supabase"
        );
    }

    /// How long (seconds) after exiting a mint before we can re-buy it.
    /// Prevents the re-buy bug where the same dying token gets bought repeatedly.
    const REBUY_COOLDOWN_SECS: u64 = 1800; // 30 minutes

    // ── Read methods (used by pre_execution_checks) ──────────

    pub async fn open_position_count(&self) -> i64 {
        self.inner.read().await.open_mints.len() as i64
    }

    pub async fn total_exposure(&self) -> f64 {
        self.inner.read().await.total_exposure_sol
    }

    pub async fn today_pnl(&self) -> f64 {
        let inner = self.inner.read().await;
        // Reset PnL if the date rolled over
        if inner.pnl_date != today_str() {
            drop(inner);
            let mut w = self.inner.write().await;
            if w.pnl_date != today_str() {
                w.today_pnl_sol = 0.0;
                w.pnl_date = today_str();
                debug!("Daily PnL reset (new day)");
            }
            return w.today_pnl_sol;
        }
        inner.today_pnl_sol
    }

    pub async fn has_position_for_mint(&self, mint: &str) -> bool {
        let inner = self.inner.read().await;
        if inner.open_mints.contains(mint) {
            return true;
        }
        if inner.pending_mints.contains(mint) {
            return true;
        }
        // Re-buy cooldown: reject if we exited this mint recently
        if let Some(exit_time) = inner.recently_exited.get(mint) {
            let elapsed = exit_time.elapsed().as_secs();
            if elapsed < Self::REBUY_COOLDOWN_SECS {
                info!(
                    mint = mint,
                    cooldown_remaining = Self::REBUY_COOLDOWN_SECS - elapsed,
                    "🕐 Re-buy cooldown active — skipping mint"
                );
                return true;
            }
        }
        false
    }

    /// Atomically check-and-reserve a mint for an in-flight buy.
    ///
    /// Returns `true` if the caller is granted the reservation and may proceed
    /// to execute a trade. Returns `false` if the mint is already open, already
    /// reserved by another in-flight buy, or within the re-buy cooldown.
    ///
    /// This is the **only** safe pre-trade dedup primitive — `has_position_for_mint`
    /// is a non-atomic read and is subject to TOCTOU races when two events for
    /// the same mint arrive within the trade-execution window (e.g. paper trades
    /// completing in <100ms, or a pump.fun migrate+complete double-event).
    ///
    /// On trade failure, callers MUST invoke [`release_reservation`] to clear
    /// the pending marker. On success, [`record_buy`] consumes the reservation.
    pub async fn try_reserve_for_mint(&self, mint: &str) -> bool {
        let mut inner = self.inner.write().await;
        if inner.open_mints.contains(mint) || inner.pending_mints.contains(mint) {
            return false;
        }
        if let Some(exit_time) = inner.recently_exited.get(mint) {
            let elapsed = exit_time.elapsed().as_secs();
            if elapsed < Self::REBUY_COOLDOWN_SECS {
                info!(
                    mint = mint,
                    cooldown_remaining = Self::REBUY_COOLDOWN_SECS - elapsed,
                    "🕐 Re-buy cooldown active — skipping mint"
                );
                return false;
            }
        }
        inner.pending_mints.insert(mint.to_string());
        debug!(mint = mint, pending = inner.pending_mints.len(), "🔒 Reserved mint for in-flight buy");
        true
    }

    /// Release a pending reservation made by [`try_reserve_for_mint`].
    /// Call this on every trade-execution failure path so the mint can be
    /// retried by a future signal.
    pub async fn release_reservation(&self, mint: &str) {
        let mut inner = self.inner.write().await;
        if inner.pending_mints.remove(mint) {
            debug!(mint = mint, pending = inner.pending_mints.len(), "🔓 Released mint reservation");
        }
    }

    /// Check if a dev wallet is blacklisted (known scammer/dumper).
    pub async fn is_dev_blacklisted(&self, dev_wallet: &str) -> bool {
        self.inner.read().await.blacklisted_devs.contains(dev_wallet)
    }

    /// Add a dev wallet to the blacklist (called when DevWalletDumping exit fires).
    pub async fn blacklist_dev(&self, dev_wallet: &str) {
        let mut inner = self.inner.write().await;
        if inner.blacklisted_devs.insert(dev_wallet.to_string()) {
            info!(
                dev_wallet = dev_wallet,
                total_blacklisted = inner.blacklisted_devs.len(),
                "🚫 Dev wallet added to blacklist"
            );
        }
    }

    // ── Write methods (called on buy/exit) ───────────────────

    /// Return mints that have been open longer than `max_age_secs`.
    pub async fn stale_mints(&self, max_age_secs: u64) -> Vec<String> {
        let inner = self.inner.read().await;
        inner.open_since
            .iter()
            .filter(|(_, opened_at)| opened_at.elapsed().as_secs() > max_age_secs)
            .map(|(mint, _)| mint.clone())
            .collect()
    }

    /// Force-remove a mint from open positions (used by stale sweeper).
    /// Returns true if the mint was actually removed.
    pub async fn force_close_mint(&self, mint: &str, sol_spent: f64) {
        let mut inner = self.inner.write().await;
        if inner.open_mints.remove(mint) {
            inner.open_since.remove(mint);
            inner.total_exposure_sol -= sol_spent;
            if inner.total_exposure_sol < 0.0 {
                inner.total_exposure_sol = 0.0;
            }
            inner.recently_exited.insert(mint.to_string(), Instant::now());
            warn!(
                mint = mint,
                open = inner.open_mints.len(),
                "🧹 Stale position force-closed from TradingState"
            );
        }
    }

    /// Record a new position opened (buy confirmed).
    pub async fn record_buy(&self, mint: &str, sol_spent: f64) {
        let mut inner = self.inner.write().await;
        inner.pending_mints.remove(mint);
        inner.open_mints.insert(mint.to_string());
        inner.open_since.insert(mint.to_string(), Instant::now());
        inner.total_exposure_sol += sol_spent;
        debug!(
            mint = mint,
            exposure = format!("{:.4}", inner.total_exposure_sol),
            open = inner.open_mints.len(),
            "📊 State: position opened"
        );
    }

    /// Record a position closed (exit confirmed).
    pub async fn record_exit(&self, mint: &str, sol_spent: f64, pnl_sol: f64, is_full_exit: bool) {
        let mut inner = self.inner.write().await;
        if is_full_exit {
            inner.open_mints.remove(mint);
            inner.open_since.remove(mint);
            inner.total_exposure_sol -= sol_spent;
            if inner.total_exposure_sol < 0.0 {
                inner.total_exposure_sol = 0.0;
            }
            // Track exit time for re-buy cooldown
            inner.recently_exited.insert(mint.to_string(), Instant::now());
            // Prune stale entries (older than 2× cooldown) to prevent unbounded growth
            inner.recently_exited.retain(|_, t| t.elapsed().as_secs() < Self::REBUY_COOLDOWN_SECS * 2);
        }
        // Reset PnL if date rolled
        if inner.pnl_date != today_str() {
            inner.today_pnl_sol = 0.0;
            inner.pnl_date = today_str();
        }
        inner.today_pnl_sol += pnl_sol;
        debug!(
            mint = mint,
            pnl = format!("{:.4}", pnl_sol),
            today_pnl = format!("{:.4}", inner.today_pnl_sol),
            exposure = format!("{:.4}", inner.total_exposure_sol),
            open = inner.open_mints.len(),
            "📊 State: position exited"
        );
    }
}

fn today_str() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}
