-- ═══════════════════════════════════════════════════════════════════
--  Migration 019: Re-entry shadow-mode tracking
--  Records would-be re-entries after moonbag exit so we can
--  correlate narrative score + dip + hype vs realized outcome
--  before flipping any live re-entry execution on.
--  Run in Supabase SQL Editor.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS reentry_candidates (
    id                       BIGSERIAL PRIMARY KEY,

    -- Token identity
    mint                     TEXT NOT NULL,
    symbol                   TEXT,
    token_name               TEXT,
    position_id              BIGINT NOT NULL,
    reentry_attempt          SMALLINT NOT NULL,   -- 1, 2, 3… per mint

    -- Snapshot of the moonbag exit that triggered this watcher
    moonbag_exit_price_usd   DOUBLE PRECISION NOT NULL,
    moonbag_exit_time        TIMESTAMPTZ       NOT NULL,
    moonbag_exit_pnl_pct     DOUBLE PRECISION NOT NULL,
    moonbag_exit_reason      TEXT,
    exit_was_profitable      BOOLEAN           NOT NULL,

    -- Check-time state
    check_time               TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    current_price_usd        DOUBLE PRECISION NOT NULL,
    dip_pct_from_exit        DOUBLE PRECISION NOT NULL,
    seconds_since_exit       INTEGER           NOT NULL,

    -- Narrative score (from check_narrative)
    narrative_score          SMALLINT,          -- 0..100
    narrative_state          TEXT,
    narrative_tier           TEXT,
    narrative_result         JSONB,             -- full NarrativeResult
    narrative_latency_ms     INTEGER,
    previous_attempt_score   SMALLINT,          -- narrative_score from attempt n-1 (null on #1)

    -- Gate evaluation
    gates_passed             JSONB NOT NULL,    -- { "window": true, "dip": true, ... }
    would_enter_strict       BOOLEAN NOT NULL,  -- all gates + exit_was_profitable
    would_enter_permissive   BOOLEAN NOT NULL,  -- all gates regardless of exit profitability
    block_reason             TEXT,              -- first failing gate (informational)

    -- Outcome backfill (filled by outcome job)
    outcome_checked_at       TIMESTAMPTZ,
    price_30m_after          DOUBLE PRECISION,
    price_2h_after           DOUBLE PRECISION,
    price_6h_after           DOUBLE PRECISION,
    peak_price_6h            DOUBLE PRECISION,
    hypothetical_pnl_6h_pct  DOUBLE PRECISION,

    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reentry_mint_created
    ON reentry_candidates (mint, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_reentry_position_attempt
    ON reentry_candidates (position_id, reentry_attempt);

CREATE INDEX IF NOT EXISTS idx_reentry_outcome_pending
    ON reentry_candidates (created_at)
    WHERE outcome_checked_at IS NULL;

COMMENT ON TABLE reentry_candidates IS
  'Shadow-mode log of every re-entry evaluation after a moonbag exit. '
  'Used to validate gate + narrative correlation before enabling live re-entry trades.';
