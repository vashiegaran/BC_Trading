-- ═══════════════════════════════════════════════════════════════════
--  Migration 024 — path_eval_log
--  Records every TP1/TP2 intercept where the v14 B/C/D paper-paths
--  were evaluated as a fallback for moonbag promotion.
--
--  Lets us answer the question: "are B/C/D rules actually firing? if
--  not, is it because no positions reach TP1/TP2 below the OpenAI gate,
--  or because the thresholds never match?"
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS path_eval_log (
    id                  BIGSERIAL PRIMARY KEY,
    position_id         BIGINT,
    mint                TEXT NOT NULL,
    tp_intercept        TEXT NOT NULL,            -- 'tp1' | 'tp2'
    openai_score        DOUBLE PRECISION,
    min_score_gate      DOUBLE PRECISION,
    is_fast_runner      BOOLEAN DEFAULT FALSE,

    -- Path eligibility inputs (from sniper_features JSONB)
    is_us_hours         BOOLEAN,
    be_volume_24h_usd   DOUBLE PRECISION,
    be_liquidity_usd    DOUBLE PRECISION,
    bc_score            DOUBLE PRECISION,

    -- Path match results
    matched_path_c_off_hours_low_vol BOOLEAN DEFAULT FALSE,
    matched_path_b_liquidity_floor   BOOLEAN DEFAULT FALSE,
    matched_path_d_bc_score_80       BOOLEAN DEFAULT FALSE,
    matched_path                      TEXT,        -- final selected path or NULL
    eligible_count                    INTEGER DEFAULT 0,

    -- Outcome
    decision            TEXT NOT NULL,             -- 'narrative_promoted' | 'fast_runner_promoted' | 'paper_path_promoted' | 'no_promote'
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_path_eval_log_mint
    ON path_eval_log(mint);
CREATE INDEX IF NOT EXISTS idx_path_eval_log_decision
    ON path_eval_log(decision);
CREATE INDEX IF NOT EXISTS idx_path_eval_log_created
    ON path_eval_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_path_eval_log_intercept
    ON path_eval_log(tp_intercept);
