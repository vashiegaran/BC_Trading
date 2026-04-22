-- ═══════════════════════════════════════════════════════════════════
--  Fast-runner auto-promote + latency tracking — Supabase migration
--  Run once in the Supabase SQL editor.
-- ═══════════════════════════════════════════════════════════════════

-- ── 1. moonbag_positions: promotion source + fast-runner tracking ──

-- How the moonbag was promoted: narrative_tp1, narrative_tp2, fast_runner, cto_strong, cto_moderate
ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS promotion_source TEXT;

COMMENT ON COLUMN moonbag_positions.promotion_source IS
    'How this moonbag was promoted: narrative_tp1 | narrative_tp2 | fast_runner | cto_strong | cto_moderate';

-- Price at the exact moment of promotion (for drift analysis)
ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS price_at_promotion DOUBLE PRECISION;

COMMENT ON COLUMN moonbag_positions.price_at_promotion IS
    'Token price (USD) at the moment of moonbag promotion';

-- Fast-runner flag
ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS is_fast_runner BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN moonbag_positions.is_fast_runner IS
    'TRUE if auto-promoted as fast runner (hit TP2 in < fast_runner_threshold_secs with no narrative)';

-- Background narrative check result (only populated for fast runners)
ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS fast_runner_check_score INTEGER;

COMMENT ON COLUMN moonbag_positions.fast_runner_check_score IS
    'OpenAI score from background narrative check (fast runners only)';

ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS fast_runner_check_latency_ms BIGINT;

COMMENT ON COLUMN moonbag_positions.fast_runner_check_latency_ms IS
    'Total wall time (ms) for the background narrative check';

ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS fast_runner_price_at_result DOUBLE PRECISION;

COMMENT ON COLUMN moonbag_positions.fast_runner_price_at_result IS
    'Token price (USD) when background narrative check returned';

ALTER TABLE moonbag_positions
    ADD COLUMN IF NOT EXISTS fast_runner_price_drift_pct DOUBLE PRECISION;

COMMENT ON COLUMN moonbag_positions.fast_runner_price_drift_pct IS
    'Price change (%) between promotion and background check result — measures risk window';

-- Index for fast-runner analysis queries
CREATE INDEX IF NOT EXISTS idx_moonbag_positions_fast_runner
    ON moonbag_positions (is_fast_runner) WHERE is_fast_runner = TRUE;

CREATE INDEX IF NOT EXISTS idx_moonbag_positions_promotion_source
    ON moonbag_positions (promotion_source);
