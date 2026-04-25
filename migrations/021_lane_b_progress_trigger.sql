-- =====================================================================
--  Migration 021: Lane B (90% bonding-curve trigger) support
-- =====================================================================
--  Adds columns to bc_paper_trades so we can record TWO trigger types
--  per token without breaking the existing 50-SOL-volume signal flow.
--
--  Trigger types:
--    'volume_50sol'   — original signal, fires once at 50 SOL volume
--                       (≈ 23% BC progress). This is what we have today.
--    'progress_90pct' — Lane B signal, fires once when bc_progress_pct
--                       crosses 90%. Used to test whether buying late on
--                       the BC + riding the graduation pump is profitable.
--
--  Both rows are auto-marked graduated/peak when graduation fires.
--  Rows are NOT unique on mint — a single token can have one row per
--  trigger (used for A/B comparison of entry timings).
--
--  entry_score      : the bc_score computed from buy/sell flow at trigger time
--  entry_api_checks : results of post-trigger API enrichment (GoPlus,
--                     mint authority, freeze authority). Filled async by
--                     a background task so the trigger write itself stays
--                     non-blocking. Used to test the hypothesis:
--                     "filter at 90% with score+API checks, then buy →
--                     does it beat the volume_50sol baseline?"
-- =====================================================================

ALTER TABLE bc_paper_trades
    ADD COLUMN IF NOT EXISTS entry_trigger     TEXT DEFAULT 'volume_50sol',
    ADD COLUMN IF NOT EXISTS entry_score       DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS entry_api_checks  JSONB;

-- Backfill: every existing row was the volume_50sol trigger.
UPDATE bc_paper_trades
SET    entry_trigger = 'volume_50sol'
WHERE  entry_trigger IS NULL;

CREATE INDEX IF NOT EXISTS idx_bc_paper_trades_trigger
    ON bc_paper_trades(entry_trigger);

CREATE INDEX IF NOT EXISTS idx_bc_paper_trades_mint_trigger
    ON bc_paper_trades(mint, entry_trigger);

COMMENT ON COLUMN bc_paper_trades.entry_trigger IS
    'Which signal fired this row: volume_50sol (default) or progress_90pct (Lane B)';
COMMENT ON COLUMN bc_paper_trades.entry_score IS
    'compute_bc_score(...) value at trigger time — used to backtest filter thresholds';
COMMENT ON COLUMN bc_paper_trades.entry_api_checks IS
    'Async-filled API enrichment outcome at trigger time: '
    '{ goplus_honeypot, goplus_buy_tax, goplus_sell_tax, mint_authority_revoked, '
    '  freeze_authority_revoked, fast_track_passed, rejection_reason, ms_total }. '
    'Used to test "score + API filter at 90% → profitability" hypothesis.';
