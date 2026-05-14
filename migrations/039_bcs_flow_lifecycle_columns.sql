-- ═══════════════════════════════════════════════════════════════════
--  Migration 039: bonding_curve_signals flow/lifecycle columns
--
--  ROOT CAUSE:
--    build_signal_payload() writes post-v18.9 flow and creator-lifecycle
--    metrics as top-level bonding_curve_signals columns. These existed in
--    JSON and in post_grad_flow_shadow, but not in bonding_curve_signals,
--    causing PGRST204 schema-cache insert failures such as:
--      "Could not find the 'creator_prior_mints_6h' column"
--
--  ADD COLUMN IF NOT EXISTS keeps this safe to rerun.
-- ═══════════════════════════════════════════════════════════════════

ALTER TABLE bonding_curve_signals
    ADD COLUMN IF NOT EXISTS proven_wallet_buy_count_bc INTEGER,
    ADD COLUMN IF NOT EXISTS proven_wallet_buy_sol_total_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS proven_wallet_sell_count_bc INTEGER,
    ADD COLUMN IF NOT EXISTS proven_wallet_sell_sol_total_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS proven_wallet_net_sol_bc DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS creator_prior_mints_6h INTEGER,
    ADD COLUMN IF NOT EXISTS creator_same_label_prior_mints_6h INTEGER,
    ADD COLUMN IF NOT EXISTS creator_seconds_since_last_mint BIGINT;

CREATE INDEX IF NOT EXISTS idx_bcs_proven_wallet_flow
    ON bonding_curve_signals(proven_wallet_buy_count_bc, proven_wallet_buy_sol_total_bc, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_bcs_creator_lifecycle
    ON bonding_curve_signals(creator_prior_mints_6h, creator_same_label_prior_mints_6h, created_at DESC);

COMMENT ON COLUMN bonding_curve_signals.proven_wallet_buy_count_bc IS
    'Number of proven-wallet buy trades observed on the bonding curve before this signal row.';
COMMENT ON COLUMN bonding_curve_signals.proven_wallet_buy_sol_total_bc IS
    'Total SOL bought by proven wallets on the bonding curve before this signal row.';
COMMENT ON COLUMN bonding_curve_signals.creator_prior_mints_6h IS
    'Number of prior mints by this creator observed in the rolling 6h creator lifecycle cache.';
COMMENT ON COLUMN bonding_curve_signals.creator_same_label_prior_mints_6h IS
    'Number of prior same-label mints by this creator observed in the rolling 6h creator lifecycle cache.';

NOTIFY pgrst, 'reload schema';
