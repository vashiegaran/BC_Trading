-- ═══════════════════════════════════════════════════════════════════
--  Migration 037: Profit-lock shadow signal annotations
--  Adds observe-only columns to post_grad_flow_shadow. These fields mark a
--  hypothetical sell-all/profit-lock condition from first-minute post-grad
--  data plus bonding-curve distribution flow. No live execution path reads
--  these columns.
-- ═══════════════════════════════════════════════════════════════════

ALTER TABLE post_grad_flow_shadow
    ADD COLUMN IF NOT EXISTS profit_lock_shadow_triggered BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS profit_lock_shadow_rule TEXT,
    ADD COLUMN IF NOT EXISTS profit_lock_shadow_rules JSONB,
    ADD COLUMN IF NOT EXISTS profit_lock_shadow_reasons JSONB,
    ADD COLUMN IF NOT EXISTS profit_lock_shadow_thresholds JSONB,
    ADD COLUMN IF NOT EXISTS profit_lock_would_sell_pct DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS profit_lock_eval_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS profit_lock_eval_multiplier DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS profit_lock_eval_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS profit_lock_shadow_version TEXT;

CREATE INDEX IF NOT EXISTS idx_pgfs_profit_lock_shadow_triggered
    ON post_grad_flow_shadow(profit_lock_eval_at DESC)
    WHERE profit_lock_shadow_triggered IS TRUE;

CREATE INDEX IF NOT EXISTS idx_pgfs_profit_lock_shadow_rule
    ON post_grad_flow_shadow(profit_lock_shadow_rule)
    WHERE profit_lock_shadow_triggered IS TRUE;

COMMENT ON COLUMN post_grad_flow_shadow.profit_lock_shadow_triggered IS
    'Shadow-only marker: true when the first-minute profit-lock toxic-flow rule would have suggested a full take-profit exit. Never read by live execution.';

COMMENT ON COLUMN post_grad_flow_shadow.profit_lock_shadow_rule IS
    'Primary matched shadow rule, e.g. strict_toxic_profit_lock or agu_like_toxic.';

COMMENT ON COLUMN post_grad_flow_shadow.profit_lock_shadow_reasons IS
    'Human-readable reasons/metrics that caused or blocked the shadow profit-lock tag.';

COMMENT ON COLUMN post_grad_flow_shadow.profit_lock_shadow_thresholds IS
    'Configured thresholds used by the shadow profit-lock evaluator at evaluation time.';

COMMENT ON COLUMN post_grad_flow_shadow.profit_lock_would_sell_pct IS
    'Hypothetical sell percentage for analysis only. The bot does not submit an order from this signal.';
