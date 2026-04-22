-- ═══════════════════════════════════════════════════════════════════
--  Narrative moonbag system — Supabase migration
--  Run once in the Supabase SQL editor for your project.
-- ═══════════════════════════════════════════════════════════════════

-- ── 1. moonbag_positions ─────────────────────────────────────────
-- Full lifecycle log for every moonbag promotion.
CREATE TABLE IF NOT EXISTS moonbag_positions (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id           BIGINT NOT NULL,
    mint                  TEXT NOT NULL,
    token_name            TEXT,
    token_symbol          TEXT,

    -- Narrative state at promotion time
    narrative_state       TEXT NOT NULL,           -- EarlyAttention / ExpandingAttention / RunnerConfirmed

    -- Pricing
    entry_price_usd       DOUBLE PRECISION NOT NULL DEFAULT 0,
    token_amount          DOUBLE PRECISION NOT NULL DEFAULT 0,
    sol_value             DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_price_usd        DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_multiplier       DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Trailing stop config at promotion
    initial_trailing_pct  DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_hold_secs         BIGINT NOT NULL DEFAULT 0,
    profit_gate_multiplier DOUBLE PRECISION NOT NULL DEFAULT 2.0,

    -- Runtime state
    profit_gate_reached   BOOLEAN NOT NULL DEFAULT FALSE,
    extension_checked     BOOLEAN NOT NULL DEFAULT FALSE,
    narrative_recheck_count INTEGER NOT NULL DEFAULT 0,

    -- Exit data (filled when moonbag exits)
    exit_reason           TEXT,                    -- trailing_stop / max_hold / narrative_downgrade / floor_breach
    exit_price_usd        DOUBLE PRECISION,
    exit_multiplier       DOUBLE PRECISION,
    final_trailing_pct    DOUBLE PRECISION,
    hold_duration_secs    BIGINT,

    -- Timestamps
    promoted_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exited_at             TIMESTAMPTZ,
    is_paper_trade        BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_moonbag_positions_mint
    ON moonbag_positions (mint);
CREATE INDEX IF NOT EXISTS idx_moonbag_positions_position_id
    ON moonbag_positions (position_id);
CREATE INDEX IF NOT EXISTS idx_moonbag_positions_promoted_at
    ON moonbag_positions (promoted_at);
CREATE INDEX IF NOT EXISTS idx_moonbag_positions_narrative_state
    ON moonbag_positions (narrative_state);

-- Enable RLS (Supabase best practice)
ALTER TABLE moonbag_positions ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "service_role_all" ON moonbag_positions
    FOR ALL USING (true) WITH CHECK (true);


-- ── 2. narrative_checks ──────────────────────────────────────────
-- Every OpenAI + DexScreener narrative check result for analysis.
CREATE TABLE IF NOT EXISTS narrative_checks (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id           BIGINT,
    mint                  TEXT NOT NULL,
    token_name            TEXT,
    token_symbol          TEXT,

    -- Check context
    check_phase           TEXT NOT NULL,           -- monitoring / moonbag_recheck / extension_check
    check_index           INTEGER,                 -- 0-based index within monitoring intervals

    -- Result
    narrative_state       TEXT NOT NULL,            -- NoSignal / EarlyAttention / ExpandingAttention / RunnerConfirmed
    score                 INTEGER NOT NULL DEFAULT 0,
    narrative_strength    DOUBLE PRECISION NOT NULL DEFAULT 0,
    market_strength       DOUBLE PRECISION NOT NULL DEFAULT 0,
    web_sources_found     INTEGER NOT NULL DEFAULT 0,

    -- Detail payloads
    reasons               JSONB DEFAULT '[]'::jsonb,
    risk_flags            JSONB DEFAULT '[]'::jsonb,

    -- Token state at check time
    current_price_usd     DOUBLE PRECISION,
    entry_price_usd       DOUBLE PRECISION,
    peak_multiplier       DOUBLE PRECISION,
    hold_seconds          BIGINT,
    momentum_ratio        DOUBLE PRECISION,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_narrative_checks_mint
    ON narrative_checks (mint);
CREATE INDEX IF NOT EXISTS idx_narrative_checks_position_id
    ON narrative_checks (position_id);
CREATE INDEX IF NOT EXISTS idx_narrative_checks_created_at
    ON narrative_checks (created_at);

-- Enable RLS
ALTER TABLE narrative_checks ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON narrative_checks
    FOR ALL USING (true) WITH CHECK (true);


-- ── 3. Add narrative/moonbag columns to existing positions table ─
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'narrative_state'
    ) THEN
        ALTER TABLE positions ADD COLUMN narrative_state TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'narrative_score'
    ) THEN
        ALTER TABLE positions ADD COLUMN narrative_score INTEGER;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'moonbag_promoted'
    ) THEN
        ALTER TABLE positions ADD COLUMN moonbag_promoted BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'moonbag_exit_reason'
    ) THEN
        ALTER TABLE positions ADD COLUMN moonbag_exit_reason TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'moonbag_exit_multiplier'
    ) THEN
        ALTER TABLE positions ADD COLUMN moonbag_exit_multiplier DOUBLE PRECISION;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'moonbag_hold_duration_secs'
    ) THEN
        ALTER TABLE positions ADD COLUMN moonbag_hold_duration_secs BIGINT;
    END IF;
END $$;


-- ── 4. Helpful views ─────────────────────────────────────────────

-- View: moonbag performance summary
CREATE OR REPLACE VIEW moonbag_performance AS
SELECT
    mp.mint,
    mp.token_name,
    mp.token_symbol,
    mp.narrative_state,
    mp.entry_price_usd,
    mp.peak_price_usd,
    mp.peak_multiplier,
    mp.exit_price_usd,
    mp.exit_multiplier,
    mp.exit_reason,
    mp.initial_trailing_pct,
    mp.final_trailing_pct,
    mp.profit_gate_reached,
    mp.hold_duration_secs,
    ROUND(mp.hold_duration_secs / 3600.0, 1) AS hold_hours,
    mp.is_paper_trade,
    mp.promoted_at,
    mp.exited_at
FROM moonbag_positions mp
ORDER BY mp.promoted_at DESC;

-- View: narrative check hit rate by state
CREATE OR REPLACE VIEW narrative_state_distribution AS
SELECT
    narrative_state,
    COUNT(*) AS total_checks,
    AVG(score) AS avg_score,
    AVG(narrative_strength) AS avg_narrative_strength,
    AVG(market_strength) AS avg_market_strength,
    AVG(web_sources_found) AS avg_web_sources
FROM narrative_checks
GROUP BY narrative_state
ORDER BY total_checks DESC;
