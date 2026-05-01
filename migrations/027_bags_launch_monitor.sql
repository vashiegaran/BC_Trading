-- ═══════════════════════════════════════════════════════════════════
--  Migration 027: Bags launch monitor tables
--  Research-only tracking for fresh Bags launches, creator-side wallets,
--  and early post-launch demand.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bags_launches (
    id                         BIGSERIAL PRIMARY KEY,
    mint                       TEXT NOT NULL UNIQUE,
    symbol                     TEXT,
    name                       TEXT,
    launch_signature           TEXT NOT NULL UNIQUE,
    launch_slot                BIGINT NOT NULL,
    launch_at                  TIMESTAMPTZ NOT NULL,
    creator_wallet             TEXT NOT NULL,
    bags_fee_payer             TEXT NOT NULL,
    creator_funding_lamports   BIGINT,
    pool_owner_wallet          TEXT,
    pool_token_account         TEXT,
    signers                    JSONB,

    demand_checked_at          TIMESTAMPTZ,
    demand_window_seconds      INTEGER,
    demand_trade_count         INTEGER,
    demand_buy_tx_count        INTEGER,
    demand_unique_buyers       INTEGER,
    demand_buy_volume_sol      DOUBLE PRECISION,
    demand_peak_single_buy_sol DOUBLE PRECISION,
    has_real_demand            BOOLEAN,

    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bags_launches_creator ON bags_launches(creator_wallet);
CREATE INDEX IF NOT EXISTS idx_bags_launches_launch_at ON bags_launches(launch_at DESC);
CREATE INDEX IF NOT EXISTS idx_bags_launches_demand ON bags_launches(has_real_demand);

CREATE TABLE IF NOT EXISTS bags_creator_stats (
    creator_wallet         TEXT PRIMARY KEY,
    launch_count           INTEGER NOT NULL DEFAULT 0,
    demand_launch_count    INTEGER NOT NULL DEFAULT 0,
    demand_rate            DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_unique_buyers      DOUBLE PRECISION,
    avg_buy_volume_sol     DOUBLE PRECISION,
    best_mint              TEXT,
    best_buy_volume_sol    DOUBLE PRECISION,
    last_launch_at         TIMESTAMPTZ,
    last_demand_launch_at  TIMESTAMPTZ,
    watchworthy            BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bags_creator_stats_watchworthy
    ON bags_creator_stats(watchworthy);