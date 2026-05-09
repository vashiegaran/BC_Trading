use std::collections::VecDeque;
use std::time::Instant;

use tracing::{debug, warn};

/// A single trade event derived from pool vault balance changes.
#[derive(Debug, Clone)]
pub struct Tick {
    /// Buy = tokens left vault (someone bought), Sell = tokens entered vault
    pub direction: TickDirection,
    /// Absolute token amount that moved (in raw units).
    pub token_delta: u64,
    /// Estimated SOL value of the trade (from SOL vault delta).
    /// 0.0 if SOL vault update hasn't arrived yet for this tick.
    pub sol_amount: f64,
    /// When this tick was received locally.
    pub timestamp: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickDirection {
    Buy,
    Sell,
}

/// Rolling window of recent ticks with computed momentum signals.
pub struct TickWindow {
    ticks: VecDeque<Tick>,
    /// Time-based window duration in seconds.
    window_secs: f64,
    /// Minimum total SOL volume in window to consider signals valid.
    min_volume_sol: f64,
}

impl TickWindow {
    pub fn new(window_secs: f64, min_volume_sol: f64) -> Self {
        Self {
            ticks: VecDeque::with_capacity(256),
            window_secs,
            min_volume_sol,
        }
    }

    /// Add a new tick and prune old ticks outside the time window.
    pub fn push(&mut self, tick: Tick) {
        self.ticks.push_back(tick);
        self.prune();
    }

    /// Remove ticks older than the window.
    fn prune(&mut self) {
        let cutoff = self.window_secs;
        let now = Instant::now();
        while let Some(front) = self.ticks.front() {
            if now.duration_since(front.timestamp).as_secs_f64() > cutoff {
                self.ticks.pop_front();
            } else {
                break;
            }
        }
    }

    /// Compute momentum snapshot from the current tick window.
    pub fn snapshot(&mut self) -> MomentumSnapshot {
        self.prune();

        let mut buy_volume_sol = 0.0_f64;
        let mut sell_volume_sol = 0.0_f64;
        let mut buy_count = 0_u32;
        let mut sell_count = 0_u32;
        let mut max_single_sell_sol = 0.0_f64;

        for tick in &self.ticks {
            match tick.direction {
                TickDirection::Buy => {
                    buy_count += 1;
                    buy_volume_sol += tick.sol_amount;
                }
                TickDirection::Sell => {
                    sell_count += 1;
                    sell_volume_sol += tick.sol_amount;
                    if tick.sol_amount > max_single_sell_sol {
                        max_single_sell_sol = tick.sol_amount;
                    }
                }
            }
        }

        let total_volume = buy_volume_sol + sell_volume_sol;
        let momentum_ratio = if total_volume > 0.0 {
            buy_volume_sol / total_volume
        } else {
            0.5 // neutral when no data
        };

        let avg_trade_sol = if (buy_count + sell_count) > 0 {
            total_volume / (buy_count + sell_count) as f64
        } else {
            0.0
        };

        let ticks_per_second = if !self.ticks.is_empty() {
            let elapsed = if self.ticks.len() > 1 {
                let first = self.ticks.front().unwrap().timestamp;
                let last = self.ticks.back().unwrap().timestamp;
                last.duration_since(first).as_secs_f64().max(0.1)
            } else {
                self.window_secs
            };
            self.ticks.len() as f64 / elapsed
        } else {
            0.0
        };

        // Consecutive direction counting (from most recent tick backwards)
        let mut consecutive_buys = 0_u32;
        let mut consecutive_sells = 0_u32;
        if let Some(latest) = self.ticks.back() {
            let target_dir = latest.direction;
            for tick in self.ticks.iter().rev() {
                if tick.direction == target_dir {
                    match target_dir {
                        TickDirection::Buy => consecutive_buys += 1,
                        TickDirection::Sell => consecutive_sells += 1,
                    }
                } else {
                    break;
                }
            }
        }

        // Sell acceleration: compare sell volume in recent half vs older half of window
        let sell_accelerating = self.is_sell_accelerating();

        let has_enough_data = total_volume >= self.min_volume_sol;

        MomentumSnapshot {
            momentum_ratio,
            buy_volume_sol,
            sell_volume_sol,
            buy_count,
            sell_count,
            consecutive_buys,
            consecutive_sells,
            max_single_sell_sol,
            avg_trade_sol,
            ticks_per_second,
            sell_accelerating,
            has_enough_data,
            total_ticks: self.ticks.len() as u32,
        }
    }

    /// Check if sell volume is accelerating (recent half > older half).
    fn is_sell_accelerating(&self) -> bool {
        if self.ticks.len() < 4 {
            return false;
        }
        let mid = self.ticks.len() / 2;
        let older_sell_vol: f64 = self
            .ticks
            .iter()
            .take(mid)
            .filter(|t| t.direction == TickDirection::Sell)
            .map(|t| t.sol_amount)
            .sum();
        let recent_sell_vol: f64 = self
            .ticks
            .iter()
            .skip(mid)
            .filter(|t| t.direction == TickDirection::Sell)
            .map(|t| t.sol_amount)
            .sum();
        // Accelerating if recent half has 50%+ more sell volume
        recent_sell_vol > older_sell_vol * 1.5
    }

    /// Returns true if no ticks have been received for the given duration.
    pub fn is_inactive(&self, silence_secs: f64) -> bool {
        match self.ticks.back() {
            Some(tick) => tick.timestamp.elapsed().as_secs_f64() > silence_secs,
            None => true,
        }
    }

    pub fn tick_count(&self) -> usize {
        self.ticks.len()
    }

    /// Update the sol_amount of the most recent tick (called when SOL vault
    /// notification arrives shortly after the token vault notification).
    pub fn update_last_tick_sol(&mut self, sol_amount: f64) {
        if let Some(tick) = self.ticks.back_mut() {
            // Only update if the tick is recent (within 2 seconds) and doesn't have SOL data yet
            if tick.sol_amount == 0.0 && tick.timestamp.elapsed().as_secs_f64() < 2.0 {
                tick.sol_amount = sol_amount;
            }
        }
    }
}

/// Computed signals from the tick window, used by the dip state machine
/// and trigger evaluator.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MomentumSnapshot {
    /// buy_volume / (buy_volume + sell_volume). 0.5 = neutral, >0.65 = bullish, <0.35 = bearish.
    pub momentum_ratio: f64,
    pub buy_volume_sol: f64,
    pub sell_volume_sol: f64,
    pub buy_count: u32,
    pub sell_count: u32,
    /// Consecutive buys from most recent tick backwards.
    pub consecutive_buys: u32,
    /// Consecutive sells from most recent tick backwards.
    pub consecutive_sells: u32,
    /// Largest single sell in the window (SOL).
    pub max_single_sell_sol: f64,
    /// Average trade size in SOL.
    pub avg_trade_sol: f64,
    /// Trade frequency.
    pub ticks_per_second: f64,
    /// Whether sell volume is increasing (recent > older).
    pub sell_accelerating: bool,
    /// Whether there's enough volume data to trust the signals.
    pub has_enough_data: bool,
    /// Total number of ticks in the window.
    pub total_ticks: u32,
}

impl Default for MomentumSnapshot {
    fn default() -> Self {
        Self {
            momentum_ratio: 0.5,
            buy_volume_sol: 0.0,
            sell_volume_sol: 0.0,
            buy_count: 0,
            sell_count: 0,
            consecutive_buys: 0,
            consecutive_sells: 0,
            max_single_sell_sol: 0.0,
            avg_trade_sol: 0.0,
            ticks_per_second: 0.0,
            sell_accelerating: false,
            has_enough_data: false,
            total_ticks: 0,
        }
    }
}

// ─── Dip State Machine ─────────────────────────────────────

/// The dip state machine determines whether a drawdown is a recoverable
/// dip or a death spiral.  It sits between price updates and the trigger
/// evaluator, suppressing the trailing stop during a grace period when
/// recovery signals are present.
#[derive(Debug, Clone, PartialEq)]
pub enum DipState {
    /// Price is trending up or flat.  Triggers work normally.
    Normal,
    /// Price dropped past dip_threshold_pct from peak.  Grace period active.
    /// Trailing stop is PAUSED.  Hard stop-loss still active.
    DipWatch {
        entered_at: Instant,
        /// Price when DipWatch was entered.
        dip_entry_price: f64,
        /// Lowest price seen during this dip.
        dip_floor_price: f64,
    },
    /// Recovery signals detected during DipWatch.  Still watching.
    Recovering {
        entered_at: Instant,
        dip_entry_price: f64,
        dip_floor_price: f64,
    },
}

/// Configuration for the dip state machine.
#[derive(Debug, Clone)]
pub struct DipConfig {
    /// Drawdown % from peak to enter DIP_WATCH.
    pub dip_threshold_pct: f64,
    /// How long to wait for recovery before selling (seconds).
    pub grace_period_secs: f64,
    /// Momentum ratio threshold for recovery detection.
    pub recovery_buy_ratio: f64,
    /// Minimum volume in window (SOL) to trust signals.
    pub min_volume_sol: f64,
    /// Single sell > this multiple of avg trade = whale exit (death signal).
    pub whale_sell_multiplier: f64,
    /// Sells accelerating for this many consecutive seconds = death _(detected in tick window)_.
    pub no_trades_timeout_secs: f64,
}

impl DipConfig {
    pub fn from_monitoring_config(cfg: &crate::config::MonitoringConfig) -> Self {
        Self {
            dip_threshold_pct: cfg.dip_threshold_pct,
            grace_period_secs: cfg.dip_grace_period_secs,
            recovery_buy_ratio: cfg.dip_recovery_buy_ratio,
            min_volume_sol: cfg.dip_min_volume_sol,
            whale_sell_multiplier: cfg.dip_whale_sell_multiplier,
            no_trades_timeout_secs: cfg.dip_no_trades_timeout_secs,
        }
    }
}

/// Result of evaluating the dip state machine on each monitoring cycle.
#[derive(Debug, Clone, PartialEq)]
pub enum DipAction {
    /// No dip — proceed with normal trigger evaluation.
    Normal,
    /// In dip grace period — suppress trailing stop, keep hard stop-loss + TP.
    SuppressTrailingStop,
    /// Death signal detected — exit immediately.
    ImmediateExit { reason: &'static str },
}

/// Evaluate the dip state machine and return the action for this cycle.
///
/// This function is called every monitoring cycle. It updates the dip state
/// and returns what the monitoring loop should do.
///
/// `age_secs`: seconds since the position was opened (younger = more patient).
/// `profit_pct`: current PnL% relative to entry (positive = in profit).
/// `consecutive_death_ticks`: counter for soft death signals (grace expired, no trades).
///   Hard death signals (whale_sell, sell_acceleration) fire immediately.
///   Soft signals require 3 consecutive ticks before killing the position.
pub fn evaluate_dip(
    state: &mut DipState,
    current_price: f64,
    peak_price: f64,
    entry_price_usd: f64,
    momentum: &MomentumSnapshot,
    dip_cfg: &DipConfig,
    age_secs: u64,
    profit_pct: f64,
    consecutive_death_ticks: &mut u32,
) -> DipAction {
    if peak_price <= 0.0 || entry_price_usd < 0.000001 {
        return DipAction::Normal;
    }

    // ── Age-aware grace scaling ──
    // Young post-graduation tokens often dip then rip. Give them more room.
    let age_grace_multiplier: f64 = if age_secs < 90 {
        2.5 // 0-90s: 2.5× grace (8s → 20s)
    } else if age_secs < 180 {
        1.5 // 90-180s: 1.5× grace (8s → 12s)
    } else {
        1.0 // >180s: normal
    };

    // ── Profit-aware death signal threshold ──
    // If we're >10% profitable, the token proved strength — require harder
    // evidence of death. Weak signals (grace timeout, no-trades) are skipped.
    let in_profit = profit_pct > 10.0;

    let effective_grace = dip_cfg.grace_period_secs * age_grace_multiplier;
    let effective_no_trades = dip_cfg.no_trades_timeout_secs * age_grace_multiplier;

    let drawdown_from_peak = (peak_price - current_price) / peak_price * 100.0;

    match state {
        DipState::Normal => {
            *consecutive_death_ticks = 0; // reset on normal state
                                          // Check if we've entered a dip
            if drawdown_from_peak >= dip_cfg.dip_threshold_pct {
                debug!(
                    drawdown_pct = format!("{:.1}", drawdown_from_peak),
                    "📉 Entered DIP_WATCH — grace period started"
                );
                *state = DipState::DipWatch {
                    entered_at: Instant::now(),
                    dip_entry_price: current_price,
                    dip_floor_price: current_price,
                };
                return DipAction::SuppressTrailingStop;
            }
            DipAction::Normal
        }

        DipState::DipWatch {
            entered_at,
            dip_entry_price,
            dip_floor_price,
        } => {
            // Update floor
            if current_price < *dip_floor_price {
                *dip_floor_price = current_price;
            }

            let elapsed = entered_at.elapsed().as_secs_f64();

            // === DEATH SIGNALS (any one = immediate exit) ===

            // 1. Whale sell: single sell > whale_multiplier × average
            if momentum.has_enough_data
                && momentum.avg_trade_sol > 0.0
                && momentum.max_single_sell_sol
                    > dip_cfg.whale_sell_multiplier * momentum.avg_trade_sol
            {
                warn!(
                    whale_sell = format!("{:.4}", momentum.max_single_sell_sol),
                    avg_trade = format!("{:.4}", momentum.avg_trade_sol),
                    "🚨 Whale sell detected during dip — death signal"
                );
                *state = DipState::Normal;
                return DipAction::ImmediateExit {
                    reason: "whale_sell_during_dip",
                };
            }

            // 2. Sell volume accelerating
            if momentum.has_enough_data
                && momentum.sell_accelerating
                && momentum.consecutive_sells >= 3
            {
                warn!("🚨 Sell acceleration during dip — death signal");
                *state = DipState::Normal;
                return DipAction::ImmediateExit {
                    reason: "sell_acceleration",
                };
            }

            // 3. No trades (everyone left)
            //    Young or profitable tokens get extended no-trades timeout.
            if momentum.total_ticks == 0 && elapsed > effective_no_trades {
                if in_profit {
                    debug!(
                        "🤔 No trades during dip but position is profitable — extending patience"
                    );
                    // Don't kill profitable positions on no-trades alone;
                    // let grace expiry handle it (which is also extended).
                } else {
                    *consecutive_death_ticks += 1;
                    if *consecutive_death_ticks >= 3 {
                        warn!("🚨 No trades during dip grace (3 consecutive checks) — token dead");
                        *state = DipState::Normal;
                        return DipAction::ImmediateExit {
                            reason: "no_trades_during_dip",
                        };
                    }
                    debug!(
                        consecutive_death_ticks,
                        "⚠️ No trades during dip — waiting for 3 consecutive"
                    );
                }
            }

            // === RECOVERY SIGNALS ===
            let mut recovery_signals = 0_u32;

            // Signal 1: Buy volume dominance returning
            if momentum.has_enough_data && momentum.momentum_ratio > dip_cfg.recovery_buy_ratio {
                recovery_signals += 1;
            }

            // Signal 2: Consecutive buys (buyers stepping in)
            if momentum.consecutive_buys >= 3 {
                recovery_signals += 1;
            }

            // Signal 3: Price stabilizing (current >= dip_floor, meaning not making new lows)
            if current_price > *dip_floor_price {
                recovery_signals += 1;
            }

            if recovery_signals >= 2 {
                *consecutive_death_ticks = 0; // recovery detected — reset counter
                debug!(
                    recovery_signals,
                    "📈 Recovery signals detected — entering RECOVERING"
                );
                *state = DipState::Recovering {
                    entered_at: *entered_at,
                    dip_entry_price: *dip_entry_price,
                    dip_floor_price: *dip_floor_price,
                };
                return DipAction::SuppressTrailingStop;
            }

            // === GRACE PERIOD EXPIRED (age-aware) ===
            if elapsed >= effective_grace {
                if in_profit && recovery_signals >= 1 {
                    // Profitable + at least 1 recovery signal → don't kill,
                    // transition to Recovering for extended observation.
                    debug!(
                        elapsed_secs = format!("{:.1}", elapsed),
                        recovery_signals,
                        profit_pct = format!("{:.1}", profit_pct),
                        "📈 Grace expired but profitable with recovery signals — extending"
                    );
                    *state = DipState::Recovering {
                        entered_at: *entered_at,
                        dip_entry_price: *dip_entry_price,
                        dip_floor_price: *dip_floor_price,
                    };
                    return DipAction::SuppressTrailingStop;
                }
                *consecutive_death_ticks += 1;
                if *consecutive_death_ticks >= 3 {
                    debug!(
                        elapsed_secs = format!("{:.1}", elapsed),
                        recovery_signals,
                        age_grace_mult = format!("{:.1}", age_grace_multiplier),
                        "⏰ Dip grace expired — no recovery (3 consecutive checks)"
                    );
                    *state = DipState::Normal;
                    return DipAction::ImmediateExit {
                        reason: "dip_grace_expired",
                    };
                }
                debug!(
                    elapsed_secs = format!("{:.1}", elapsed),
                    consecutive_death_ticks,
                    "⏰ Dip grace expired tick — waiting for 3 consecutive"
                );
            }

            DipAction::SuppressTrailingStop
        }

        DipState::Recovering {
            entered_at,
            dip_entry_price: _,
            dip_floor_price: _,
        } => {
            // Check if price has recovered above dip threshold → back to normal
            if drawdown_from_peak < dip_cfg.dip_threshold_pct * 0.5 {
                debug!("✅ Price recovered from dip — back to NORMAL");
                *state = DipState::Normal;
                return DipAction::Normal;
            }

            // Even in recovery, still check for death signals
            if momentum.has_enough_data
                && momentum.avg_trade_sol > 0.0
                && momentum.max_single_sell_sol
                    > dip_cfg.whale_sell_multiplier * momentum.avg_trade_sol
            {
                warn!("🚨 Whale sell during recovery — death signal");
                *state = DipState::Normal;
                return DipAction::ImmediateExit {
                    reason: "whale_sell_during_recovery",
                };
            }

            if momentum.has_enough_data
                && momentum.sell_accelerating
                && momentum.consecutive_sells >= 3
            {
                warn!("🚨 Sell acceleration during recovery — death signal");
                *state = DipState::Normal;
                return DipAction::ImmediateExit {
                    reason: "sell_acceleration_in_recovery",
                };
            }

            // Extended grace: recovery gets more time (2x normal grace period)
            let elapsed = entered_at.elapsed().as_secs_f64();
            if elapsed >= dip_cfg.grace_period_secs * 2.0 {
                // If still not fully recovered after extended grace, check momentum
                if momentum.has_enough_data && momentum.momentum_ratio < 0.4 {
                    *consecutive_death_ticks += 1;
                    if *consecutive_death_ticks >= 3 {
                        debug!("⏰ Extended grace expired with weak momentum (3 consecutive) — exiting");
                        *state = DipState::Normal;
                        return DipAction::ImmediateExit {
                            reason: "extended_grace_expired_weak",
                        };
                    }
                    debug!(
                        consecutive_death_ticks,
                        "⏰ Extended grace weak tick — waiting for 3 consecutive"
                    );
                } else {
                    // Momentum is OK — go back to normal, trailing stop will handle it
                    *consecutive_death_ticks = 0;
                    debug!("⏰ Extended grace expired but momentum OK — back to normal");
                    *state = DipState::Normal;
                    return DipAction::Normal;
                }
            }

            DipAction::SuppressTrailingStop
        }
    }
}
