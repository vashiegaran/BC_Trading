-- ST Trade Snapshots: per-poll trade data for pattern analysis
-- Written by st_trades.rs every 15 seconds per active position

CREATE TABLE IF NOT EXISTS st_trade_snapshots (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id     BIGINT NOT NULL,
    mint            TEXT NOT NULL,
    poll_number     INT NOT NULL,
    buy_count       INT NOT NULL DEFAULT 0,
    sell_count      INT NOT NULL DEFAULT 0,
    total_buy_sol   DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_sell_sol  DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_buy_sol     DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_buy_sol     DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_sell_sol    DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_sell_sol    DOUBLE PRECISION NOT NULL DEFAULT 0,
    unique_buyers   INT NOT NULL DEFAULT 0,
    unique_sellers  INT NOT NULL DEFAULT 0,
    patterns_detected JSONB DEFAULT '[]'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for querying by position
CREATE INDEX IF NOT EXISTS idx_st_snapshots_position
    ON st_trade_snapshots (position_id);

-- Index for querying by mint
CREATE INDEX IF NOT EXISTS idx_st_snapshots_mint
    ON st_trade_snapshots (mint);

-- Enable RLS (Supabase best practice)
ALTER TABLE st_trade_snapshots ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "service_role_all" ON st_trade_snapshots
    FOR ALL USING (true) WITH CHECK (true);
