-- ═══════════════════════════════════════════════════════════════════
--  Migration 025 — bonding_curve_signals: BC progress columns
--
--  ROOT CAUSE:
--    src/detection/pumpfun_ws.rs build_signal_payload() writes 7 BC-state
--    columns to bonding_curve_signals, but the canonical table schema in
--    migrations/000_all_tables.sql never defined them. Every INSERT since
--    the WS-derived BC state was added has been failing with PGRST204
--    "Could not find the 'bc_market_cap_sol' column" — silently dropping
--    every signal write (0 rows in last 24h vs 11,428 historical).
--
--  This migration adds the missing columns. ADD COLUMN IF NOT EXISTS keeps
--  it idempotent.
-- ═══════════════════════════════════════════════════════════════════

ALTER TABLE bonding_curve_signals
    ADD COLUMN IF NOT EXISTS bc_progress_pct          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bc_virtual_sol_reserves  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bc_virtual_token_reserves DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bc_market_cap_sol        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bc_price_sol_per_token   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_sold_during_bc   BOOLEAN,
    ADD COLUMN IF NOT EXISTS buy_pressure_at_entry_pct DOUBLE PRECISION;

-- Helpful indexes for analytics
CREATE INDEX IF NOT EXISTS idx_bcs_bc_progress
    ON bonding_curve_signals(bc_progress_pct);
CREATE INDEX IF NOT EXISTS idx_bcs_graduated_created
    ON bonding_curve_signals(graduated, created_at DESC);
