-- ═══════════════════════════════════════════════════════════════════
--  Sniper enrichment pipeline — Supabase migration
--  Run once in the Supabase SQL editor for your project.
-- ═══════════════════════════════════════════════════════════════════

-- ── 1. sniper_candidates ─────────────────────────────────────────
-- Every token that reaches the sniper pipeline (bought or rejected).
CREATE TABLE IF NOT EXISTS sniper_candidates (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mint          TEXT NOT NULL,
    symbol        TEXT,
    name          TEXT,
    pool_address  TEXT,
    creator_wallet TEXT,

    -- action: 'bought' | 'rejected' | 'skipped'
    action        TEXT NOT NULL DEFAULT 'rejected',
    rejection_reason TEXT,

    -- Liquidity snapshot at detection
    initial_liquidity_sol DOUBLE PRECISION,

    -- Full enrichment payload (60+ fields)
    sniper_features JSONB,

    -- Counterfactual tracking (filled by rejected-token tracker)
    price_1m      DOUBLE PRECISION,
    price_5m      DOUBLE PRECISION,
    price_15m     DOUBLE PRECISION,
    price_1h      DOUBLE PRECISION,
    peak_multiplier DOUBLE PRECISION,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sniper_candidates_mint
    ON sniper_candidates (mint);
CREATE INDEX IF NOT EXISTS idx_sniper_candidates_action
    ON sniper_candidates (action);
CREATE INDEX IF NOT EXISTS idx_sniper_candidates_created_at
    ON sniper_candidates (created_at);


-- ── 2. creator_reputation ────────────────────────────────────────
-- Tracks creator wallets across launches for serial-rugger detection.
CREATE TABLE IF NOT EXISTS creator_reputation (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wallet          TEXT NOT NULL UNIQUE,
    total_launches  INTEGER NOT NULL DEFAULT 0,
    rug_count       INTEGER NOT NULL DEFAULT 0,
    avg_token_lifespan_min DOUBLE PRECISION,
    last_launch_at  TIMESTAMPTZ,
    notes           TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_creator_reputation_wallet
    ON creator_reputation (wallet);


-- ── 3. Add sniper columns to existing positions table ────────────
-- These columns store enrichment data alongside the trade for analysis.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'sniper_features'
    ) THEN
        ALTER TABLE positions ADD COLUMN sniper_features JSONB;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'post_trade_features'
    ) THEN
        ALTER TABLE positions ADD COLUMN post_trade_features JSONB;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'detection_latency_ms'
    ) THEN
        ALTER TABLE positions ADD COLUMN detection_latency_ms INTEGER;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'positions' AND column_name = 'monitoring_snapshot'
    ) THEN
        ALTER TABLE positions ADD COLUMN monitoring_snapshot JSONB;
    END IF;

    -- Gap 3: rugged + rug_time_secs on sniper_candidates
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sniper_candidates' AND column_name = 'rugged'
    ) THEN
        ALTER TABLE sniper_candidates ADD COLUMN rugged BOOLEAN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sniper_candidates' AND column_name = 'rug_time_secs'
    ) THEN
        ALTER TABLE sniper_candidates ADD COLUMN rug_time_secs INTEGER;
    END IF;
END $$;


-- ── 4. Enable Row Level Security (optional — depends on your setup) ──
-- Uncomment if you use RLS with service_role key:
-- ALTER TABLE sniper_candidates ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE creator_reputation ENABLE ROW LEVEL SECURITY;


-- ── 5. Helpful views ─────────────────────────────────────────────

-- View: missed opportunities (rejected tokens that pumped)
CREATE OR REPLACE VIEW sniper_missed_opportunities AS
SELECT
    id,
    mint,
    symbol,
    name,
    rejection_reason,
    initial_liquidity_sol,
    peak_multiplier,
    price_1m,
    price_5m,
    price_15m,
    price_1h,
    created_at
FROM sniper_candidates
WHERE action = 'rejected'
  AND peak_multiplier IS NOT NULL
  AND peak_multiplier > 2.0
ORDER BY peak_multiplier DESC;

-- View: creator reputation summary
CREATE OR REPLACE VIEW creator_reputation_summary AS
SELECT
    wallet,
    total_launches,
    rug_count,
    CASE WHEN total_launches > 0
         THEN ROUND(rug_count::numeric / total_launches * 100, 1)
         ELSE 0
    END AS rug_pct,
    avg_token_lifespan_min,
    last_launch_at
FROM creator_reputation
ORDER BY total_launches DESC;

-- ── 6. Unified training data view (Gap 4) ────────────────────
-- Joins sniper_candidates with positions for ML training.
-- Each row is a candidate that was bought AND has outcome data.
CREATE OR REPLACE VIEW sniper_training_data AS
SELECT
    sc.id AS candidate_id,
    sc.mint,
    sc.symbol,
    sc.name,
    sc.creator_wallet,
    sc.initial_liquidity_sol,
    sc.sniper_features,
    p.id AS position_id,
    p.entry_price_usd,
    p.exit_price_usd,
    p.sol_spent,
    p.pnl_pct,
    p.pnl_sol,
    p.peak_price_usd,
    p.peak_multiplier,
    p.hold_duration_secs,
    p.exit_reason,
    p.post_trade_features,
    p.monitoring_snapshot,
    p.detection_latency_ms,
    p.is_paper_trade,
    p.created_at AS trade_opened_at,
    p.closed_at AS trade_closed_at,
    sc.created_at AS detected_at
FROM sniper_candidates sc
INNER JOIN positions p ON p.mint = sc.mint
WHERE sc.action = 'bought'
ORDER BY sc.created_at DESC;
