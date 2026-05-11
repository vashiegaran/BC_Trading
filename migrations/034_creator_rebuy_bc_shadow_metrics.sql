-- ═══════════════════════════════════════════════════════════════════
--  Migration 034: Creator-rebuy bonding-curve quality metrics
--  Shadow/data-only columns. These do not change live entry behavior.
-- ═══════════════════════════════════════════════════════════════════

ALTER TABLE bonding_curve_signals
    ADD COLUMN IF NOT EXISTS creator_buy_count_bc INTEGER,
    ADD COLUMN IF NOT EXISTS creator_buy_sol_total_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_buy_max_sol_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_first_buy_after_secs DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_first_buy_progress_pct DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_last_buy_after_secs DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_last_buy_progress_pct DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_sell_count_bc INTEGER,
    ADD COLUMN IF NOT EXISTS creator_sell_sol_total_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_net_sol_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_buy_share_pct DOUBLE PRECISION;

ALTER TABLE narrative_cluster_shadow
    ADD COLUMN IF NOT EXISTS creator_buy_count_bc INTEGER,
    ADD COLUMN IF NOT EXISTS creator_buy_sol_total_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_buy_max_sol_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_first_buy_after_secs DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_first_buy_progress_pct DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_last_buy_after_secs DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_last_buy_progress_pct DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_sell_count_bc INTEGER,
    ADD COLUMN IF NOT EXISTS creator_sell_sol_total_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_net_sol_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_buy_share_pct DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_bcs_creator_rebuy_quality
    ON bonding_curve_signals(creator_buy_count_bc, creator_buy_sol_total_bc, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ncs_creator_rebuy_quality
    ON narrative_cluster_shadow(creator_buy_count_bc, creator_buy_sol_total_bc, created_at DESC);

COMMENT ON COLUMN bonding_curve_signals.creator_buy_count_bc IS
    'Number of creator buy trades observed on the bonding curve before this signal row.';
COMMENT ON COLUMN bonding_curve_signals.creator_buy_sol_total_bc IS
    'Total SOL bought by the creator on the bonding curve before this signal row.';
COMMENT ON COLUMN bonding_curve_signals.creator_buy_share_pct IS
    'Creator buy SOL divided by all observed buy SOL at signal time, as a percentage.';
COMMENT ON COLUMN narrative_cluster_shadow.creator_buy_count_bc IS
    'Number of creator buy trades observed before the narrative-cluster shadow row armed.';
COMMENT ON COLUMN narrative_cluster_shadow.creator_buy_sol_total_bc IS
    'Total SOL bought by the creator before the narrative-cluster shadow row armed.';
COMMENT ON COLUMN narrative_cluster_shadow.creator_buy_share_pct IS
    'Creator buy SOL divided by all observed buy SOL at narrative arm time, as a percentage.';
