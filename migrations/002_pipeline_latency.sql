-- ═══════════════════════════════════════════════════════════════════
--  Pipeline latency tracking — one row per token through the pipeline
--  Run in Supabase SQL editor.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pipeline_latency (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mint                    TEXT NOT NULL,

    -- Detection
    detected_at_ms          BIGINT,                -- unix ms when token was first seen
    detection_to_sniper_ms  BIGINT,                -- wall-clock from detected_at to sniper start

    -- Sniper Enrichment (9 parallel API calls)
    enrichment_total_ms     BIGINT,
    enrichment_per_source   JSONB,                 -- {"solana_tracker": 234, "on_chain_mint": 56, ...}

    -- Sniper Hard Filters (5 sequential checks)
    hard_filter_total_ms    BIGINT,

    -- Filter Engine (fast gates)
    filter_engine_total_ms  BIGINT,
    filter_per_check        JSONB,                 -- {"sanity": 0, "age": 0, "liquidity": 234, ...}

    -- Pre-Execution Checks
    precheck_total_ms       BIGINT,

    -- Execution Buy
    execution_total_ms      BIGINT,

    -- Post-buy Verification (4 parallel checks)
    post_buy_total_ms       BIGINT,
    post_buy_per_check      JSONB,                 -- {"rugcheck": 456, "goplus": 234, ...}

    -- End-to-end
    pipeline_total_ms       BIGINT,                -- detected_at → position opened

    -- Outcome
    outcome                 TEXT NOT NULL,          -- bought | rejected_hard_filter | rejected_filter | rejected_precheck | execution_failed
    rejection_stage         TEXT,                   -- hard_filter | filter_engine | precheck | execution
    rejection_reason        TEXT,
    position_id             BIGINT,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_latency_mint
    ON pipeline_latency (mint);
CREATE INDEX IF NOT EXISTS idx_pipeline_latency_outcome
    ON pipeline_latency (outcome);
CREATE INDEX IF NOT EXISTS idx_pipeline_latency_created_at
    ON pipeline_latency (created_at);
