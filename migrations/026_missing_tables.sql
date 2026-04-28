-- ═══════════════════════════════════════════════════════════════════
--  Migration 026 — missing tables that the bot writes to
--
--  AUDIT FINDINGS (2026-04-28):
--  Code writes to several tables that don't exist in the live DB. Some
--  have migrations that were never applied; precheck_log has no migration
--  at all. Each fire-and-forget POST silently 404s.
--
--    • bc_price_ticks               — mig 023 was never applied
--    • position_enrichment_snapshots — mig 010 was never applied
--    • st_trade_snapshots           — mig 004 was never applied
--    • precheck_log                 — no migration ever existed
--
--  This migration consolidates them all so we don't need to re-discover
--  the order or worry about whether a previous migration partially ran.
-- ═══════════════════════════════════════════════════════════════════

-- ── bc_price_ticks (was migrations/023_bc_price_ticks.sql) ───────
CREATE TABLE IF NOT EXISTS bc_price_ticks (
    id              BIGSERIAL PRIMARY KEY,
    mint            TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    elapsed_secs    DOUBLE PRECISION,
    bc_progress_pct DOUBLE PRECISION,
    bc_price_sol_per_token DOUBLE PRECISION,
    bc_price_usd    DOUBLE PRECISION,
    v_sol           DOUBLE PRECISION,
    v_tok           DOUBLE PRECISION,
    market_cap_sol  DOUBLE PRECISION,
    buy_count       INTEGER,
    sell_count      INTEGER,
    unique_buyers   INTEGER,
    total_volume_sol DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bcpt_mint ON bc_price_ticks(mint);
CREATE INDEX IF NOT EXISTS idx_bcpt_mint_seq ON bc_price_ticks(mint, seq);
CREATE INDEX IF NOT EXISTS idx_bcpt_created ON bc_price_ticks(created_at DESC);


-- ── position_enrichment_snapshots (was mig 010) ──────────────────
CREATE TABLE IF NOT EXISTS position_enrichment_snapshots (
    id                BIGSERIAL PRIMARY KEY,
    position_id       BIGINT NOT NULL,
    mint              TEXT NOT NULL,
    trigger           TEXT NOT NULL,    -- 'periodic' | 'tp1' | 'tp2' | 'pre_exit' | 'post_exit_1h'
    elapsed_secs      DOUBLE PRECISION,
    snapshot          JSONB NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pes_position ON position_enrichment_snapshots(position_id);
CREATE INDEX IF NOT EXISTS idx_pes_mint ON position_enrichment_snapshots(mint);
CREATE INDEX IF NOT EXISTS idx_pes_trigger ON position_enrichment_snapshots(trigger);
CREATE INDEX IF NOT EXISTS idx_pes_created ON position_enrichment_snapshots(created_at DESC);


-- ── st_trade_snapshots (was mig 004) ─────────────────────────────
CREATE TABLE IF NOT EXISTS st_trade_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    position_id     BIGINT,
    mint            TEXT NOT NULL,
    elapsed_secs    DOUBLE PRECISION,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sts_mint ON st_trade_snapshots(mint);
CREATE INDEX IF NOT EXISTS idx_sts_position ON st_trade_snapshots(position_id);
CREATE INDEX IF NOT EXISTS idx_sts_created ON st_trade_snapshots(created_at DESC);


-- ── precheck_log (no prior migration) ────────────────────────────
-- Matches the payload built in src/filters/precheck.rs.
CREATE TABLE IF NOT EXISTS precheck_log (
    id                       BIGSERIAL PRIMARY KEY,
    mint                     TEXT NOT NULL,
    token_safety_passed      BOOLEAN,
    token_safety_reason      TEXT,
    goplus_passed            BOOLEAN,
    goplus_reason            TEXT,
    rugcheck_passed          BOOLEAN,
    rugcheck_reason          TEXT,
    rugcheck_score           DOUBLE PRECISION,
    all_passed               BOOLEAN,
    has_critical_danger      BOOLEAN,
    elapsed_ms               BIGINT,
    unique_buyers_at_trigger INTEGER,
    buy_pressure_at_trigger  DOUBLE PRECISION,
    volume_sol_at_trigger    DOUBLE PRECISION,
    checked_at               TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pcl_mint ON precheck_log(mint);
CREATE INDEX IF NOT EXISTS idx_pcl_created ON precheck_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pcl_all_passed ON precheck_log(all_passed);
