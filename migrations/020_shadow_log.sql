-- =====================================================================
--  Migration 020: shadow_log table (price curve from entry through 24h)
-- =====================================================================
--  Created when migration history was lost; the bot's monitoring loop
--  inserts/PATCHes into this table from src/monitoring/mod.rs
--  (shadow_log_loop). Holds per-position price snapshots so we can
--  measure each token's TRUE 24h peak — independent of the bot's exit
--  decisions — and back-test moonbag promotion rules.
--
--  Columns mirror the JSON payloads built in shadow_log_loop:
--    INSERT  : position_id, mint, entry_price_usd
--    PATCH 1+: snapshots, shadow_peak_usd, shadow_peak_multiplier,
--              shadow_low_usd, total_ticks, exit_at_secs
--    Final   : + exit_reason, duration_secs, completed_at
--
--  `snapshots` is JSONB array of { t, p, m, phase } objects — one per
--  poll tick (5s active / 30s post-exit). Up to ~3,000 elements over 24h.
-- =====================================================================

CREATE TABLE IF NOT EXISTS shadow_log (
    id                       BIGSERIAL PRIMARY KEY,
    position_id              BIGINT NOT NULL UNIQUE,
    mint                     TEXT NOT NULL,
    entry_price_usd          DOUBLE PRECISION NOT NULL,

    -- Updated in flushes
    snapshots                JSONB DEFAULT '[]'::jsonb,
    shadow_peak_usd          DOUBLE PRECISION,
    shadow_peak_multiplier   DOUBLE PRECISION,
    shadow_low_usd           DOUBLE PRECISION,
    total_ticks              BIGINT DEFAULT 0,

    -- Exit timing (secs from entry; null while still active)
    exit_at_secs             BIGINT,
    exit_reason              TEXT,
    duration_secs            BIGINT,

    -- Lifecycle
    started_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_shadow_log_position ON shadow_log(position_id);
CREATE INDEX IF NOT EXISTS idx_shadow_log_mint ON shadow_log(mint);
CREATE INDEX IF NOT EXISTS idx_shadow_log_completed ON shadow_log(completed_at);
CREATE INDEX IF NOT EXISTS idx_shadow_log_peak_mult ON shadow_log(shadow_peak_multiplier);

COMMENT ON TABLE shadow_log IS
  'Per-position 24h price curve captured by src/monitoring/mod.rs::shadow_log_loop. '
  'Used to measure TRUE peak independent of bot exit logic, for moonbag promotion training.';
