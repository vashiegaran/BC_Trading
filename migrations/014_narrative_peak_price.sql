-- ─────────────────────────────────────────────────────────────
--  Migration 014: Backfill peak tracking for narrative_signals
-- ─────────────────────────────────────────────────────────────
--  narrative_signals already has peak_multiplier but no peak_price
--  column, so the shadow price_updater.rs update_peaks() loop
--  skipped it. Adding the column + wiring it in lets us finally
--  capture how high narrative-tagged tokens actually ran.
-- ─────────────────────────────────────────────────────────────

ALTER TABLE narrative_signals
    ADD COLUMN IF NOT EXISTS peak_price DOUBLE PRECISION;
