-- Bonding-curve gate PnL backtest results.
-- Research-only table. Populated by scripts/backfill_bc_gate_pnl.py.
-- Not read by the live sniper bot.
--
-- Purpose: for each bonding-curve signal row, pull post-signal price history
-- from Birdeye and compute what PnL a hypothetical entry at signal-time would
-- have produced under several TP/SL combinations. Answers the open question
-- in docs/strategies/bonding_curve.md: "does the 2.30x gate actually make
-- money, or just predict graduation?"

CREATE TABLE IF NOT EXISTS bc_gate_backtest (
    mint TEXT PRIMARY KEY,
    symbol TEXT,
    signal_recorded_at TIMESTAMPTZ NOT NULL,

    -- Gate match flags (computed from bonding_curve_signals row at backfill time)
    matches_gate_v1 BOOLEAN,      -- BSR>=2 AND unique_buyers>=40 AND NOT creator_rebuy
    graduated BOOLEAN,

    -- Signal-time snapshot (copied from bonding_curve_signals for self-containment)
    bsr DOUBLE PRECISION,
    unique_buyers INTEGER,
    creator_rebuy BOOLEAN,
    total_volume_sol DOUBLE PRECISION,
    token_age_secs DOUBLE PRECISION,

    -- Price metrics (24h window from signal_recorded_at)
    price_at_signal DOUBLE PRECISION,
    price_peak DOUBLE PRECISION,
    peak_multiplier DOUBLE PRECISION,
    time_to_peak_mins DOUBLE PRECISION,
    max_drawdown_pct DOUBLE PRECISION,   -- max % drop from signal price within window
    price_24h DOUBLE PRECISION,

    -- TP/SL simulation results — each is pnl_pct realized under that rule.
    -- Exit reason: 'tp' | 'sl' | 'timeout' | 'no_data'
    sim_tp30_sl20_pnl_pct DOUBLE PRECISION,
    sim_tp30_sl20_reason TEXT,
    sim_tp50_sl30_pnl_pct DOUBLE PRECISION,
    sim_tp50_sl30_reason TEXT,
    sim_tp100_sl30_pnl_pct DOUBLE PRECISION,
    sim_tp100_sl30_reason TEXT,
    sim_tp100_sl50_pnl_pct DOUBLE PRECISION,
    sim_tp100_sl50_reason TEXT,
    sim_tp200_sl50_pnl_pct DOUBLE PRECISION,
    sim_tp200_sl50_reason TEXT,

    -- Diagnostics
    candles_used INTEGER,
    backfill_source TEXT,          -- 'birdeye_1m' | 'birdeye_15m' | 'failed'
    backfill_error TEXT,
    backfilled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bcgb_signal_at ON bc_gate_backtest(signal_recorded_at);
CREATE INDEX IF NOT EXISTS idx_bcgb_gate ON bc_gate_backtest(matches_gate_v1);
CREATE INDEX IF NOT EXISTS idx_bcgb_grad ON bc_gate_backtest(graduated);
