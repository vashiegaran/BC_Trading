-- 022: v14 multi-lane research columns.
--
-- New entry_trigger values used by writers (no CHECK constraint, just text):
--   progress_60pct      — BC paper-trade fired at >=60% bonding-curve progress
--   progress_75pct      — BC paper-trade fired at >=75% (was logged as
--                         progress_90pct in older rows; kept for back-compat)
--   progress_90pct      — BC paper-trade fired at >=90% (with GoPlus check)
--   graduation_raw      — paper-trade row at graduation, no API checks
--   graduation_goplus   — paper-trade row at graduation, with async GoPlus check
--
-- Three new feature columns let post-hoc analysis slice without adding lanes.
-- All are optional; legacy rows stay NULL.

ALTER TABLE bc_paper_trades
  ADD COLUMN IF NOT EXISTS creator_sold_during_bc BOOLEAN,
  ADD COLUMN IF NOT EXISTS buy_pressure_at_entry_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS initial_liquidity_sol DOUBLE PRECISION;

COMMENT ON COLUMN bc_paper_trades.creator_sold_during_bc IS
  'True if the creator wallet appears as a seller in trade_log before the entry fired.';
COMMENT ON COLUMN bc_paper_trades.buy_pressure_at_entry_pct IS
  'buy_count / (buy_count + sell_count) * 100 captured at the moment the entry fired.';
COMMENT ON COLUMN bc_paper_trades.initial_liquidity_sol IS
  'Bonding-curve cumulative SOL volume at graduation (approximates pool seed liquidity). Only populated on graduation_* lanes.';
