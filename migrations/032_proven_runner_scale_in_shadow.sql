-- 032: Proven-runner scale-in shadow log (v18.8)
-- Observe-only table for moonbag add-on buy candidates. The bot writes a row
-- when an already-promoted moonbag reaches the configured proven-runner window
-- (default 2.2x-2.8x, <=12% drawdown), then patches outcome fields when the
-- runner hits 3x/5x or exits. No live add-on buy is executed by this migration.

CREATE TABLE IF NOT EXISTS position_scale_in_shadow (
    id                          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id                 BIGINT NOT NULL,
    mint                        TEXT NOT NULL,
    token_name                  TEXT,
    token_symbol                TEXT,
    strategy_version            TEXT,
    is_paper_trade              BOOLEAN NOT NULL DEFAULT TRUE,

    trigger                     TEXT NOT NULL DEFAULT 'moonbag_proven_runner_scale_in',
    shadow_only                 BOOLEAN NOT NULL DEFAULT TRUE,
    would_add_live              BOOLEAN NOT NULL DEFAULT TRUE,
    addon_amount_sol            DOUBLE PRECISION NOT NULL DEFAULT 0,

    entry_price_usd             DOUBLE PRECISION NOT NULL DEFAULT 0,
    trigger_price_usd           DOUBLE PRECISION NOT NULL DEFAULT 0,
    trigger_multiplier          DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_price_usd_at_trigger   DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_multiplier_at_trigger  DOUBLE PRECISION NOT NULL DEFAULT 0,
    drawdown_from_peak_pct      DOUBLE PRECISION NOT NULL DEFAULT 0,

    promotion_source            TEXT,
    narrative_state             TEXT,
    promoted_age_secs           BIGINT NOT NULL DEFAULT 0,
    partial_3x_done             BOOLEAN NOT NULL DEFAULT FALSE,
    partial_5x_done             BOOLEAN NOT NULL DEFAULT FALSE,

    outcome_hit_3x              BOOLEAN,
    outcome_hit_5x              BOOLEAN,
    outcome_peak_multiplier     DOUBLE PRECISION,
    outcome_exit_reason         TEXT,
    outcome_exit_multiplier     DOUBLE PRECISION,
    outcome_completed_at        TIMESTAMPTZ,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_position_scale_in_shadow_position_id
    ON position_scale_in_shadow (position_id);

CREATE INDEX IF NOT EXISTS idx_position_scale_in_shadow_mint
    ON position_scale_in_shadow (mint);

CREATE INDEX IF NOT EXISTS idx_position_scale_in_shadow_created_at
    ON position_scale_in_shadow (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_position_scale_in_shadow_trigger_multiplier
    ON position_scale_in_shadow (trigger_multiplier DESC);

ALTER TABLE position_scale_in_shadow ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename = 'position_scale_in_shadow'
          AND policyname = 'service_role_all'
    ) THEN
        CREATE POLICY "service_role_all" ON position_scale_in_shadow
            FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;