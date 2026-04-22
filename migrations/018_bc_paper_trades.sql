-- ═══════════════════════════════════════════════════════════════════
--  Migration 018: Pre-graduation paper trades + bonding_curve_signals fixes
--  Run in Supabase SQL Editor.
-- ═══════════════════════════════════════════════════════════════════

-- Add missing graduated columns to bonding_curve_signals
-- (the code already PATCHes these but they weren't in any migration)
ALTER TABLE bonding_curve_signals ADD COLUMN IF NOT EXISTS graduated BOOLEAN DEFAULT FALSE;
ALTER TABLE bonding_curve_signals ADD COLUMN IF NOT EXISTS graduated_at TIMESTAMPTZ;

-- ── bc_paper_trades ──────────────────────────────────────────────
-- One row per simulated pre-graduation buy.
-- Entry recorded at BC signal time; outcome tracked through graduation + price monitoring.
CREATE TABLE IF NOT EXISTS bc_paper_trades (
    id                      BIGSERIAL PRIMARY KEY,
    mint                    TEXT NOT NULL,
    symbol                  TEXT,
    name                    TEXT,
    creator_wallet          TEXT,

    -- Entry snapshot (at BC signal time, before graduation)
    entry_volume_sol        DOUBLE PRECISION,       -- total BC volume when signal fired
    entry_buy_count         INTEGER,
    entry_sell_count        INTEGER,
    entry_unique_buyers     INTEGER,
    entry_buy_sell_ratio    DOUBLE PRECISION,
    entry_creator_rebuy     BOOLEAN,
    entry_token_age_secs    DOUBLE PRECISION,       -- age of token when signal fired
    entry_signals           JSONB,                  -- whale/velocity/flip flags

    -- Bonding curve state at signal time (from pump.fun API)
    bc_price_usd            DOUBLE PRECISION,       -- token price on bonding curve
    bc_market_cap_usd       DOUBLE PRECISION,       -- market cap on bonding curve
    bc_progress_pct         DOUBLE PRECISION,       -- bonding curve completion %
    bc_virtual_sol_reserves DOUBLE PRECISION,       -- virtual SOL reserves
    bc_virtual_token_reserves DOUBLE PRECISION,     -- virtual token reserves
    bc_reply_count          INTEGER,                -- pump.fun comment count
    bc_last_reply_at        TIMESTAMPTZ,            -- last comment timestamp
    bc_website              TEXT,                    -- project website (if any)
    bc_twitter              TEXT,                    -- twitter handle (if any)
    bc_telegram             TEXT,                    -- telegram link (if any)
    bc_king_of_hill_at      TIMESTAMPTZ,            -- when it hit king of the hill
    bc_raw_response         JSONB,                  -- full pump.fun API response

    -- Simulated buy
    sim_buy_sol             DOUBLE PRECISION DEFAULT 0.05,  -- how much SOL we "spent"

    -- Graduation outcome
    graduated               BOOLEAN DEFAULT FALSE,
    graduated_at            TIMESTAMPTZ,
    time_to_graduate_secs   DOUBLE PRECISION,       -- from signal to graduation (null if didn't grad)
    initial_liquidity_sol   DOUBLE PRECISION,       -- pool liquidity at graduation

    -- Post-graduation price tracking (filled by price tracker)
    price_at_graduation     DOUBLE PRECISION,       -- baseline price right after graduation
    price_1m                DOUBLE PRECISION,
    price_5m                DOUBLE PRECISION,
    price_15m               DOUBLE PRECISION,
    price_1h                DOUBLE PRECISION,
    peak_price              DOUBLE PRECISION,
    peak_multiplier         DOUBLE PRECISION,

    -- Timestamps
    signal_recorded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bcpt_mint ON bc_paper_trades(mint);
CREATE INDEX IF NOT EXISTS idx_bcpt_graduated ON bc_paper_trades(graduated);
CREATE INDEX IF NOT EXISTS idx_bcpt_created ON bc_paper_trades(created_at DESC);
