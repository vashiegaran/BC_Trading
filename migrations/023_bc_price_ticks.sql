-- ═══════════════════════════════════════════════════════════════════
--  Migration 023 — bc_price_ticks
--  Per-minute price snapshots for BC paper trades.
--  Lets us simulate any exit policy (30s, 2m, 10m, etc.) instead of
--  only the 4 fixed snapshots (1m / 5m / 15m / 1h) that
--  bc_paper_trades currently stores.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bc_price_ticks (
    id              BIGSERIAL PRIMARY KEY,
    paper_trade_id  BIGINT NOT NULL,        -- FK to bc_paper_trades.id
    mint            TEXT NOT NULL,
    seq             INTEGER NOT NULL,        -- 0,1,2,... index of tick since fire
    elapsed_secs    INTEGER NOT NULL,        -- seconds since paper-trade entry
    price_usd       DOUBLE PRECISION,        -- spot USD/token at tick
    multiplier      DOUBLE PRECISION,        -- price_usd / bc_price_usd at entry
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bcpt_ticks_pid
    ON bc_price_ticks(paper_trade_id, seq);
CREATE INDEX IF NOT EXISTS idx_bcpt_ticks_mint
    ON bc_price_ticks(mint);
CREATE INDEX IF NOT EXISTS idx_bcpt_ticks_captured
    ON bc_price_ticks(captured_at DESC);

COMMENT ON TABLE bc_price_ticks IS
    'Per-minute price snapshots for bc_paper_trades. Source of truth for'
    ' exit-policy simulation beyond the fixed 1m/5m/15m/1h snapshots on'
    ' bc_paper_trades. Writer is added in a follow-up commit.';
