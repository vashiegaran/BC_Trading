-- ═══════════════════════════════════════════════════════════════════
--  Detailed cost-per-trade tracking.
--
--  One row per SIDE (buy / sell).  For a full round-trip trade you get
--  two rows: one with side='buy', one with side='sell'.
--  Paper trades still get rows so PnL analysis is consistent; all fee
--  columns are 0 for paper.
--
--  Run in Supabase SQL editor.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS trade_costs (
    id                          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id                 BIGINT NOT NULL,
    mint                        TEXT NOT NULL,
    side                        TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    is_paper_trade              BOOLEAN NOT NULL DEFAULT false,
    created_at                  TIMESTAMPTZ DEFAULT now(),

    -- ── Amounts ──────────────────────────────────────────────────
    sol_amount                  DOUBLE PRECISION,      -- SOL spent (buy) or SOL received (sell)
    token_amount                DOUBLE PRECISION,      -- tokens received (buy) or tokens sold (sell)
    token_price_usd             DOUBLE PRECISION,      -- token price at execution
    sol_usd_price               DOUBLE PRECISION,      -- SOL/USD at execution time

    -- ── Fees (all in SOL) ────────────────────────────────────────
    network_fee_sol             DOUBLE PRECISION DEFAULT 0,  -- base tx fee (5000 lamports = 0.000005 SOL)
    priority_fee_sol            DOUBLE PRECISION DEFAULT 0,  -- compute-unit priority fee
    jito_tip_sol                DOUBLE PRECISION DEFAULT 0,  -- Jito bundle tip (buy only, or exit)
    helius_tip_sol              DOUBLE PRECISION DEFAULT 0,  -- Helius sender tip
    total_fees_sol              DOUBLE PRECISION DEFAULT 0,  -- network + priority + jito + helius

    -- ── Slippage ─────────────────────────────────────────────────
    slippage_bps                INT DEFAULT 0,               -- slippage tolerance used
    expected_sol                DOUBLE PRECISION,             -- theoretical SOL at zero slippage
    actual_sol                  DOUBLE PRECISION,             -- actual SOL after slippage
    slippage_cost_sol           DOUBLE PRECISION DEFAULT 0,  -- expected - actual (positive = cost)

    -- ── Wallet balance tracking ──────────────────────────────────
    wallet_sol_before           DOUBLE PRECISION,      -- SOL balance right before this tx
    wallet_sol_after            DOUBLE PRECISION,      -- SOL balance right after this tx
    wallet_sol_change           DOUBLE PRECISION,      -- after - before (negative on buy, positive on sell)

    -- ── Round-trip summary (populated on SELL side only) ─────────
    entry_sol_spent             DOUBLE PRECISION,      -- original buy sol_spent (for reference)
    exit_sol_received           DOUBLE PRECISION,      -- this exit's sol received
    total_round_trip_fees_sol   DOUBLE PRECISION,      -- buy fees + sell fees combined
    gross_pnl_sol               DOUBLE PRECISION,      -- exit_sol - entry_sol (before fees)
    net_pnl_sol                 DOUBLE PRECISION,      -- gross_pnl - total_round_trip_fees
    net_pnl_pct                 DOUBLE PRECISION,      -- net_pnl / entry_sol * 100

    -- ── Execution metadata ───────────────────────────────────────
    tx_sig                      TEXT,                  -- on-chain transaction signature
    exit_reason                 TEXT,                  -- only for sell side
    attempt_number              INT DEFAULT 1,         -- retry attempt that succeeded
    execution_ms                BIGINT                 -- wall-clock ms for this side
);

-- Fast lookups by position, mint, and time
CREATE INDEX IF NOT EXISTS idx_trade_costs_position ON trade_costs (position_id);
CREATE INDEX IF NOT EXISTS idx_trade_costs_mint ON trade_costs (mint);
CREATE INDEX IF NOT EXISTS idx_trade_costs_created ON trade_costs (created_at DESC);

-- ═══════════════════════════════════════════════════════════════════
--  VIEW: round-trip cost breakdown per position
-- ═══════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW trade_cost_summary AS
SELECT
    b.position_id,
    b.mint,
    b.is_paper_trade,

    -- Entry
    b.sol_amount              AS buy_sol_spent,
    b.token_price_usd         AS entry_price_usd,
    b.total_fees_sol          AS buy_fees_sol,
    b.slippage_cost_sol       AS buy_slippage_cost_sol,
    b.wallet_sol_before       AS wallet_before_buy,
    b.wallet_sol_after        AS wallet_after_buy,

    -- Exit
    s.sol_amount              AS sell_sol_received,
    s.token_price_usd         AS exit_price_usd,
    s.total_fees_sol          AS sell_fees_sol,
    s.slippage_cost_sol       AS sell_slippage_cost_sol,
    s.wallet_sol_before       AS wallet_before_sell,
    s.wallet_sol_after        AS wallet_after_sell,
    s.exit_reason,

    -- Combined costs
    COALESCE(b.total_fees_sol, 0) + COALESCE(s.total_fees_sol, 0)
        AS total_fees_sol,
    COALESCE(b.slippage_cost_sol, 0) + COALESCE(s.slippage_cost_sol, 0)
        AS total_slippage_cost_sol,
    COALESCE(b.total_fees_sol, 0) + COALESCE(s.total_fees_sol, 0)
      + COALESCE(b.slippage_cost_sol, 0) + COALESCE(s.slippage_cost_sol, 0)
        AS total_cost_sol,

    -- PnL
    s.net_pnl_sol,
    s.net_pnl_pct,

    -- Wallet net change: from before buy to after sell
    COALESCE(s.wallet_sol_after, b.wallet_sol_after) - b.wallet_sol_before
        AS wallet_net_change_sol

FROM trade_costs b
LEFT JOIN trade_costs s
    ON  s.position_id = b.position_id
    AND s.side = 'sell'
WHERE b.side = 'buy';
