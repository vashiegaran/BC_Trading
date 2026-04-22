-- 008: Add strategy_version column to positions and moonbag_positions.
-- Tracks which config/filter version produced each trade for clean A/B analysis.
-- Existing rows remain NULL (= pre-versioning baseline).

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS strategy_version TEXT;

ALTER TABLE moonbag_positions
  ADD COLUMN IF NOT EXISTS strategy_version TEXT;
