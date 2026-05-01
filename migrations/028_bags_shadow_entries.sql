-- ═══════════════════════════════════════════════════════════════════
--  Migration 028: Bags watchworthy shadow lane
--  Observe-only rows for fresh Bags launches whose creators already have a
--  strong prior demand record in bags_creator_stats.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bags_shadow_entries (
    id                               BIGSERIAL PRIMARY KEY,
    mint                             TEXT NOT NULL UNIQUE,
    symbol                           TEXT,
    name                             TEXT,
    entry_trigger                    TEXT NOT NULL DEFAULT 'bags_watchworthy_shadow',
    launch_signature                 TEXT NOT NULL UNIQUE,
    launch_at                        TIMESTAMPTZ NOT NULL,
    creator_wallet                   TEXT NOT NULL,

    creator_launch_count_at_entry    INTEGER,
    creator_demand_launch_count_at_entry INTEGER,
    creator_demand_rate_at_entry     DOUBLE PRECISION,

    entry_price_usd                  DOUBLE PRECISION,
    price_15m_usd                    DOUBLE PRECISION,
    price_1h_usd                     DOUBLE PRECISION,
    peak_price_usd                   DOUBLE PRECISION,
    peak_multiplier                  DOUBLE PRECISION,
    tracked_secs                     INTEGER,

    demand_trade_count               INTEGER,
    demand_buy_tx_count              INTEGER,
    demand_unique_buyers             INTEGER,
    demand_buy_volume_sol            DOUBLE PRECISION,
    demand_peak_single_buy_sol       DOUBLE PRECISION,
    has_real_demand                  BOOLEAN,

    status                           TEXT NOT NULL DEFAULT 'pending',
    status_message                   TEXT,
    completed_at                     TIMESTAMPTZ,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bags_shadow_creator
    ON bags_shadow_entries(creator_wallet);

CREATE INDEX IF NOT EXISTS idx_bags_shadow_launch_at
    ON bags_shadow_entries(launch_at DESC);

CREATE INDEX IF NOT EXISTS idx_bags_shadow_completed
    ON bags_shadow_entries(completed_at);

CREATE INDEX IF NOT EXISTS idx_bags_shadow_status
    ON bags_shadow_entries(status);