-- 031: Moonbag split/tail exit event log (v18.7.7)
-- Run once in Supabase before deploying v18.7.7 if you want partial-exit analytics.

CREATE TABLE IF NOT EXISTS moonbag_exit_events (
    id                         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id                BIGINT NOT NULL,
    mint                       TEXT NOT NULL,
    event_type                 TEXT NOT NULL, -- partial_exit / tail_exit / permanent_failed
    stage                      TEXT NOT NULL, -- moonbag_partial_3x / moonbag_partial_5x / trailing_stop / etc.
    pct_requested              INTEGER NOT NULL DEFAULT 0,
    token_amount_before        DOUBLE PRECISION NOT NULL DEFAULT 0,
    token_amount_requested     DOUBLE PRECISION NOT NULL DEFAULT 0,
    token_amount_after_est     DOUBLE PRECISION NOT NULL DEFAULT 0,
    price_usd                  DOUBLE PRECISION NOT NULL DEFAULT 0,
    multiplier                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_multiplier            DOUBLE PRECISION NOT NULL DEFAULT 0,
    estimated_sol_value        DOUBLE PRECISION NOT NULL DEFAULT 0,
    success                    BOOLEAN NOT NULL DEFAULT FALSE,
    is_paper_trade             BOOLEAN NOT NULL DEFAULT TRUE,
    strategy_version           TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_moonbag_exit_events_position_id
    ON moonbag_exit_events (position_id);

CREATE INDEX IF NOT EXISTS idx_moonbag_exit_events_mint
    ON moonbag_exit_events (mint);

CREATE INDEX IF NOT EXISTS idx_moonbag_exit_events_created_at
    ON moonbag_exit_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_moonbag_exit_events_stage
    ON moonbag_exit_events (stage);

ALTER TABLE moonbag_exit_events ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename = 'moonbag_exit_events'
          AND policyname = 'service_role_all'
    ) THEN
        CREATE POLICY "service_role_all" ON moonbag_exit_events
            FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
