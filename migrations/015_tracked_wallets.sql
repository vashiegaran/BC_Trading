-- ─────────────────────────────────────────────────────────────
--  Migration 015: tracked_wallets — dynamic copy-trader roster
-- ─────────────────────────────────────────────────────────────
--  Moves the wallet list out of the Rust const array so it can
--  be rotated without recompile.
--
--  Sources:
--    - track_a: Birdeye top_traders on our own winning positions
--               (forward-looking, tied to our exit strategy)
--    - track_b: Helius first-buyers on PumpFun graduates  (future)
--    - manual : hand-added wallet
--    - legacy : seeded from src/shadow/copy_trader.rs const array
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tracked_wallets (
    wallet              TEXT PRIMARY KEY,
    source              TEXT NOT NULL,             -- 'track_a' | 'track_b' | 'manual' | 'legacy'
    status              TEXT NOT NULL DEFAULT 'active',  -- 'active' | 'paused' | 'removed'
    discovered_from     TEXT,                      -- mint or description of why this wallet was added
    win_count           INTEGER,                   -- appearances on winning mints at discovery time
    added_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_reviewed_at    TIMESTAMPTZ,

    -- Rolling performance snapshot (updated by rotation script)
    n_signals_48h       INTEGER DEFAULT 0,
    n_signals_7d        INTEGER DEFAULT 0,
    mean_ret_1h_48h     DOUBLE PRECISION,
    win_rate_1h_48h     DOUBLE PRECISION,
    mean_ret_1h_7d      DOUBLE PRECISION,

    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_tw_status ON tracked_wallets(status);
CREATE INDEX IF NOT EXISTS idx_tw_source ON tracked_wallets(source);

-- Audit log of wallet lifecycle events (added, paused, removed).
CREATE TABLE IF NOT EXISTS tracked_wallets_events (
    id             BIGSERIAL PRIMARY KEY,
    wallet         TEXT NOT NULL,
    event          TEXT NOT NULL,            -- 'added' | 'paused' | 'removed' | 'reactivated' | 'reviewed'
    reason         TEXT,
    stats_snapshot JSONB,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_twe_wallet ON tracked_wallets_events(wallet);
CREATE INDEX IF NOT EXISTS idx_twe_occurred ON tracked_wallets_events(occurred_at);
