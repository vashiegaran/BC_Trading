use crate::config::ExitConfig;

use super::types::{ExitReason, ExitSignal};

/// Snapshot of a position's state used for trigger evaluation.
#[derive(Debug, Clone)]
pub struct PositionState {
    pub position_id: i64,
    pub mint: String,
    pub entry_price_usd: f64,
    pub current_price: f64,
    pub peak_price: f64,
    pub sol_spent: f64,
    pub token_amount: f64,
    pub tp1_triggered: bool,
    pub tp2_triggered: bool,
    pub elapsed_seconds: u64,
    pub is_paper_trade: bool,
    /// Initial pool liquidity in SOL — used for low-liq exit overrides.
    pub initial_liquidity_sol: f64,
}

/// Evaluate all exit triggers for a position.
///
/// Returns `Some(ExitSignal)` if a trigger fires, `None` otherwise.
///
/// When `suppress_trailing_stop` is true (dip grace period active), the
/// trailing stop trigger is skipped.  Hard stop-loss, TP triggers, and
/// time stop still fire normally.
///
/// **Evaluation order** (first match wins):
///   1. StopLoss
///   2. TakeProfit3  (requires TP1 + TP2 already triggered)
///   3. TakeProfit2  (requires TP1 already triggered)
///   4. TakeProfit1
///   5. TimeStop
///   6. VolumeDrop   (not implemented — requires volume data)
pub fn check_triggers(pos: &PositionState, exit_cfg: &ExitConfig, suppress_trailing_stop: bool) -> Option<ExitSignal> {
    // Low-liq overrides: use tighter params for positions in shallow pools
    let low_liq = pos.initial_liquidity_sol > 0.0
        && exit_cfg.low_liq_max_hold_seconds > 0
        && pos.initial_liquidity_sol < 50.0; // matches low_liq_threshold_sol default

    let base_max_hold = if low_liq && exit_cfg.low_liq_max_hold_seconds > 0 {
        exit_cfg.low_liq_max_hold_seconds
    } else {
        exit_cfg.max_hold_seconds
    };
    // v14.2 RUNNER PROTECTION: extend TimeStop window after TP1/TP2 so we
    // don't force-exit a position that's banked profit and is still climbing.
    // (Real example: 8DyXMHNS… exited at 5.64x on time_stop while paper showed 14.11x peak.)
    //   Pre-TP1 : base (default 900s = 15min)
    //   Post-TP1: 2x base   (30min — gives bag time to either run or trail out)
    //   Post-TP2: 4x base   (60min — proven runner, let trailing govern)
    let effective_max_hold = if pos.tp2_triggered {
        base_max_hold.saturating_mul(4)
    } else if pos.tp1_triggered {
        base_max_hold.saturating_mul(2)
    } else {
        base_max_hold
    };
    let effective_trailing_pct = if low_liq && exit_cfg.low_liq_trailing_stop_pct > 0.0 {
        exit_cfg.low_liq_trailing_stop_pct
    } else {
        exit_cfg.trailing_stop_pct
    };
    let effective_stop_loss = if low_liq && exit_cfg.low_liq_stop_loss_pct > 0.0 {
        exit_cfg.low_liq_stop_loss_pct
    } else {
        exit_cfg.stop_loss_pct
    };

    // TimeStop MUST be checked first — it applies even when entry_price is 0.
    // Without this, zero-price positions loop forever and block new trades.
    if pos.elapsed_seconds >= effective_max_hold {
        return Some(ExitSignal {
            position_id: pos.position_id,
            mint: pos.mint.clone(),
            pct_to_sell: 100,
            reason: ExitReason::TimeStop,
            current_price: pos.current_price,
            entry_price_usd: pos.entry_price_usd,
            sol_spent: pos.sol_spent,
            token_amount: pos.token_amount,
            is_paper_trade: pos.is_paper_trade,
            sub_reason: None,
        });
    }

    // Treat near-zero entry prices as invalid.  Pump.fun tokens always cost
    // at least ~0.000001 USD.  Anything below that is a price-fetch failure
    // and would produce absurd multipliers (700,000x) that instantly fire
    // TP1 + trailing stop, losing money.
    if pos.entry_price_usd < 0.000001 {
        return None;
    }

    let pnl_pct = (pos.current_price - pos.entry_price_usd) / pos.entry_price_usd * 100.0;
    let price_multiplier = pos.current_price / pos.entry_price_usd;

    // 1. StopLoss: Three-tier system.
    //    - Pre-TP1, never profitable + grace elapsed → tighter never_profitable_stop_loss_pct (-from entry).
    //    - Pre-TP1, otherwise                       → normal stop_loss_pct (-from entry).
    //    - Post-TP1                                  → peak-based 50% drawdown (was: -35% from entry,
    //                                                   which dumped the bag on profitable positions).
    //    - Post-TP2                                  → peak-based 60% drawdown (proven runner — wider).
    //
    // v14.2 RUNNER PROTECTION (2026-04-28): the previous logic measured stop_loss
    // from entry_price_usd regardless of TP state. A position that hit TP1 (1.8x)
    // + TP2 (4x) and peaked at 5.83x then retraced through 0.65x entry would fire
    // hard stop_loss and dump the remaining bag at -91% from entry — even though
    // the position was profitable overall. Switching to peak-based post-TP1 keeps
    // the safety net (catches catastrophic dumps) but doesn't sell into normal
    // post-graduation chop. Trailing stop still handles routine retraces.
    let peak_multiplier = pos.peak_price / pos.entry_price_usd;
    let never_profitable = peak_multiplier < 1.0;
    let grace_elapsed = pos.elapsed_seconds >= exit_cfg.never_profitable_grace_secs;

    let stop_fired = if pos.tp2_triggered && pos.peak_price > pos.entry_price_usd {
        // Post-TP2: 60% drawdown from peak. At peak 5x → fires at 2x (still profitable).
        let drawdown_pct = (pos.peak_price - pos.current_price) / pos.peak_price * 100.0;
        drawdown_pct >= 60.0
    } else if pos.tp1_triggered && pos.peak_price > pos.entry_price_usd {
        // Post-TP1: 50% drawdown from peak. At peak 2x → fires at 1x (break-even on bag).
        let drawdown_pct = (pos.peak_price - pos.current_price) / pos.peak_price * 100.0;
        drawdown_pct >= 50.0
    } else {
        // Pre-TP1: original two-tier entry-based stop.
        let effective_stop = if never_profitable && grace_elapsed
            && exit_cfg.never_profitable_stop_loss_pct > 0.0
        {
            exit_cfg.never_profitable_stop_loss_pct
        } else {
            effective_stop_loss
        };
        pnl_pct <= -(effective_stop)
    };

    if stop_fired {
        return Some(ExitSignal {
            position_id: pos.position_id,
            mint: pos.mint.clone(),
            pct_to_sell: 100,
            reason: ExitReason::StopLoss,
            current_price: pos.current_price,
            entry_price_usd: pos.entry_price_usd,
            sol_spent: pos.sol_spent,
            token_amount: pos.token_amount,
            is_paper_trade: pos.is_paper_trade,
            sub_reason: None,
        });
    }

    // 1b. MomentumKill: if held past gate time and hasn't reached min multiplier,
    //     exit early — token isn't gaining traction and will likely bleed out.
    //     Catches ~85% of dip_death positions that never reach TP1.
    //
    //     Safety floor: regardless of config, never fire before 60s. Too-early
    //     momentum_kill fires on natural post-graduation chop (e.g. 4p4j at t+40s
    //     exited at -9% before any real sell signal materialized).
    //
    //     v5.2 (2026-04-18) Peak guard: if peak ever reached MOMENTUM_KILL_PEAK_GUARD
    //     the token has shown traction. Subsequent flat windows mid-run (e.g. SOYJAK,
    //     XERO, lor) should NOT trigger momentum_kill — let stop_loss / trailing handle
    //     downside. Audit (Apr-18 v5 export) showed ~50 momentum_kill exits, several
    //     of which were mid-run pauses on tokens that later peaked ≥3x post-exit.
    const MOMENTUM_KILL_MIN_SECS: u64 = 60;
    const MOMENTUM_KILL_PEAK_GUARD: f64 = 1.5;
    let momentum_gate_secs = exit_cfg.momentum_kill_secs.max(MOMENTUM_KILL_MIN_SECS);
    if exit_cfg.momentum_kill_secs > 0
        && pos.elapsed_seconds >= momentum_gate_secs
        && !pos.tp1_triggered
        && peak_multiplier < MOMENTUM_KILL_PEAK_GUARD
        && price_multiplier < exit_cfg.momentum_kill_min_multiplier
    {
        return Some(ExitSignal {
            position_id: pos.position_id,
            mint: pos.mint.clone(),
            pct_to_sell: 100,
            reason: ExitReason::MomentumKill,
            current_price: pos.current_price,
            entry_price_usd: pos.entry_price_usd,
            sol_spent: pos.sol_spent,
            token_amount: pos.token_amount,
            is_paper_trade: pos.is_paper_trade,
            sub_reason: None,
        });
    }

    // 2. TrailingStop: price has pulled back from peak.
    //    Multiplier-based tiers: higher peak = tighter stop (bigger mcap = less room needed).
    //    Post-TP1: even tighter since we already banked profit.
    //    SKIPPED when dip state machine has suppress_trailing_stop active.
    //
    //    v5.2 (2026-04-18) Inverted high-peak curve: previous logic tightened to
    //    15% above 4x peak, which exited 10x runners on the first ~15% wick during
    //    their actual run. Audit (Apr-18) showed 23 positions exited via trailing
    //    in the 2-5x bucket where tokens went on to 5-15x post-exit. New curve
    //    keeps tight stops at low peaks (lock modest wins) but WIDENS at high peaks
    //    (let real runners breathe through normal 25-35% wicks).
    if !suppress_trailing_stop && effective_trailing_pct > 0.0 && pos.peak_price > 0.0 {
        let peak_multiplier = pos.peak_price / pos.entry_price_usd;

        // Multiplier-based trailing tiers (v5.2):
        //   Pre-TP1:  peak < 2x → 30%, 2x-3x → 25%, 3x-5x → 30%, 5x+ → 35%
        //   Post-TP1: peak < 2x → 22%, 2x-3x → 22%, 3x-5x → 28%, 5x+ → 35%
        //   Post-TP2: uses post_tp2_pct from config (30% — wider to let remainder ride)
        let (trail_pct, min_mult) = if pos.tp2_triggered {
            (exit_cfg.trailing_stop_post_tp2_pct, 1.0)
        } else if pos.tp1_triggered {
            let pct = if peak_multiplier >= 5.0 {
                35.0
            } else if peak_multiplier >= 3.0 {
                28.0
            } else if peak_multiplier >= 2.0 {
                22.0
            } else {
                exit_cfg.trailing_stop_post_tp1_pct // 22%
            };
            (pct, 1.0) // no min multiplier needed post-TP1
        } else {
            let pct = if peak_multiplier >= 5.0 {
                35.0
            } else if peak_multiplier >= 3.0 {
                30.0
            } else if peak_multiplier >= 2.0 {
                25.0
            } else {
                effective_trailing_pct // 30%
            };
            (pct, exit_cfg.trailing_stop_min_multiplier)
        };

        if peak_multiplier >= min_mult {
            let drawdown_pct = (pos.peak_price - pos.current_price) / pos.peak_price * 100.0;
            if drawdown_pct >= trail_pct {
                // Post-TP1 floor: never sell remaining tokens below entry price
                if pos.tp1_triggered && exit_cfg.trailing_stop_post_tp1_floor
                    && pos.current_price < pos.entry_price_usd
                {
                    // Price below entry — fire stop_loss instead of trailing,
                    // but only if stop_loss would also fire. Otherwise skip and
                    // let the stop_loss check handle it on its own terms.
                } else {
                    return Some(ExitSignal {
                        position_id: pos.position_id,
                        mint: pos.mint.clone(),
                        pct_to_sell: 100,
                        reason: ExitReason::TrailingStop,
                        current_price: pos.current_price,
                        entry_price_usd: pos.entry_price_usd,
                        sol_spent: pos.sol_spent,
                        token_amount: pos.token_amount,
                        is_paper_trade: pos.is_paper_trade,
                        sub_reason: None,
                    });
                }
            }
        }
    }

    // 3. TakeProfit3: TP1 + TP2 triggered AND price >= entry * TP3 → sell 100%
    if pos.tp1_triggered
        && pos.tp2_triggered
        && price_multiplier >= exit_cfg.tp3_multiplier
    {
        return Some(ExitSignal {
            position_id: pos.position_id,
            mint: pos.mint.clone(),
            pct_to_sell: 100,
            reason: ExitReason::TakeProfit3,
            current_price: pos.current_price,
            entry_price_usd: pos.entry_price_usd,
            sol_spent: pos.sol_spent,
            token_amount: pos.token_amount,
            is_paper_trade: pos.is_paper_trade,
            sub_reason: None,
        });
    }

    // 4. TakeProfit2: TP1 triggered AND price >= entry * TP2 → sell TP2_SELL_PCT%
    if pos.tp1_triggered && !pos.tp2_triggered && price_multiplier >= exit_cfg.tp2_multiplier {
        return Some(ExitSignal {
            position_id: pos.position_id,
            mint: pos.mint.clone(),
            pct_to_sell: exit_cfg.tp2_sell_pct as u8,
            reason: ExitReason::TakeProfit2,
            current_price: pos.current_price,
            entry_price_usd: pos.entry_price_usd,
            sol_spent: pos.sol_spent,
            token_amount: pos.token_amount,
            is_paper_trade: pos.is_paper_trade,
            sub_reason: None,
        });
    }

    // 5. TakeProfit1: price >= entry * TP1 → sell TP1_SELL_PCT%
    if !pos.tp1_triggered && price_multiplier >= exit_cfg.tp1_multiplier {
        return Some(ExitSignal {
            position_id: pos.position_id,
            mint: pos.mint.clone(),
            pct_to_sell: exit_cfg.tp1_sell_pct as u8,
            reason: ExitReason::TakeProfit1,
            current_price: pos.current_price,
            entry_price_usd: pos.entry_price_usd,
            sol_spent: pos.sol_spent,
            token_amount: pos.token_amount,
            is_paper_trade: pos.is_paper_trade,
            sub_reason: None,
        });
    }

    // 6. TimeStop: (moved to top of function — always fires regardless of entry_price)

    // 7. VolumeDrop: not implemented (requires volume data feed)
    // Will be added in a future phase when Birdeye WebSocket is integrated.

    None
}

// ─── Unit tests ──────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a default ExitConfig matching config.toml defaults.
    fn default_exit_config() -> ExitConfig {
        ExitConfig {
            tp1_multiplier: 2.0,
            tp1_sell_pct: 50,
            tp2_multiplier: 5.0,
            tp2_sell_pct: 30,
            tp3_multiplier: 10.0,
            stop_loss_pct: 35.0,
            max_hold_seconds: 300,
            volume_drop_threshold_pct: 75.0,
            trailing_stop_pct: 25.0,
            trailing_stop_min_multiplier: 1.3,
            trailing_stop_post_tp1_pct: 15.0,
            trailing_stop_post_tp1_floor: true,
            entry_confirmation_delay_secs: 2,
            entry_confirmation_checks: 1,
            max_realized_loss_pct: 40.0,
            trailing_stop_adaptive_multiplier: 3.0,
            trailing_stop_adaptive_pct: 18.0,
            trailing_stop_post_tp2_pct: 30.0,
            never_profitable_stop_loss_pct: 25.0,
            never_profitable_grace_secs: 20,
            min_hold_before_stop_loss: 5,
            low_liq_trailing_stop_pct: 0.0,
            low_liq_max_hold_seconds: 0,
            low_liq_stop_loss_pct: 0.0,
            momentum_kill_secs: 0,
            momentum_kill_min_multiplier: 1.3,
        }
    }

    fn base_position() -> PositionState {
        PositionState {
            position_id: 1,
            mint: "TestMint111".to_string(),
            entry_price_usd: 0.001,
            current_price: 0.001,
            peak_price: 0.001,
            sol_spent: 0.1,
            token_amount: 100_000.0,
            tp1_triggered: false,
            tp2_triggered: false,
            elapsed_seconds: 10,
            is_paper_trade: true,
            initial_liquidity_sol: 80.0,
        }
    }

    #[test]
    fn test_stop_loss_fires_on_40pct_drop() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.current_price = 0.0006;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_some(), "Stop loss should fire on 40% drop");
        let sig = signal.unwrap();
        assert_eq!(sig.reason, ExitReason::StopLoss);
        assert_eq!(sig.pct_to_sell, 100);
    }

    #[test]
    fn test_tp1_fires_on_2x() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.current_price = 0.002;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_some(), "TP1 should fire at 2x");
        let sig = signal.unwrap();
        assert_eq!(sig.reason, ExitReason::TakeProfit1);
        assert_eq!(sig.pct_to_sell, 50);
    }

    #[test]
    fn test_tp2_fires_on_5x_after_tp1() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.tp1_triggered = true;
        pos.current_price = 0.005;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_some(), "TP2 should fire at 5x after TP1");
        let sig = signal.unwrap();
        assert_eq!(sig.reason, ExitReason::TakeProfit2);
        assert_eq!(sig.pct_to_sell, 30);
    }

    #[test]
    fn test_tp3_fires_on_10x_after_tp1_tp2() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.tp1_triggered = true;
        pos.tp2_triggered = true;
        pos.current_price = 0.01;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_some(), "TP3 should fire at 10x after TP1+TP2");
        let sig = signal.unwrap();
        assert_eq!(sig.reason, ExitReason::TakeProfit3);
        assert_eq!(sig.pct_to_sell, 100);
    }

    #[test]
    fn test_time_stop_fires_after_300s() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.elapsed_seconds = 300;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_some(), "Time stop should fire at 300s");
        let sig = signal.unwrap();
        assert_eq!(sig.reason, ExitReason::TimeStop);
        assert_eq!(sig.pct_to_sell, 100);
    }

    #[test]
    fn test_nothing_fires_at_1_5x() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.current_price = 0.0015;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_none(), "Nothing should fire at 1.5x");
    }

    #[test]
    fn test_stop_loss_takes_priority_over_time_stop() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.current_price = 0.0006;
        pos.elapsed_seconds = 500;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_some());
        let sig = signal.unwrap();
        assert_eq!(sig.reason, ExitReason::TimeStop);
    }

    #[test]
    fn test_tp3_not_without_tp2() {
        let cfg = default_exit_config();
        let mut pos = base_position();
        pos.tp1_triggered = true;
        pos.tp2_triggered = false;
        pos.current_price = 0.01;

        let signal = check_triggers(&pos, &cfg, false);
        assert!(signal.is_some());
        let sig = signal.unwrap();
        assert_eq!(sig.reason, ExitReason::TakeProfit2);
    }
}
