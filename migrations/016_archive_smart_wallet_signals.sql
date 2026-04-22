-- ─────────────────────────────────────────────────────────────
--  Migration 016: Archive the v1 copy-trader dataset
-- ─────────────────────────────────────────────────────────────
--  The original 100-wallet list (discovered 2026-04 via Birdeye
--  top_traders on DexScreener trending tokens) produced n=1,552
--  buys with 1h mean return of −8.5% and win rate 23%. Analysis
--  confirmed the list is broken (48/100 wallets silent, high-n
--  wallets with 0% win). We keep the data as evidence/archive
--  and give the bot a fresh table to accumulate post-rotation,
--  post-Track-A signals on.
--
--  This migration is idempotent: re-running does nothing.
-- ─────────────────────────────────────────────────────────────

BEGIN;

-- 1. Rename the existing table if it still holds the v1 data.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'smart_wallet_signals'
          AND table_schema = 'public'
    )
    AND NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'smart_wallet_signals_v1_archive'
          AND table_schema = 'public'
    ) THEN
        ALTER TABLE smart_wallet_signals
            RENAME TO smart_wallet_signals_v1_archive;

        -- Rename the indexes so new ones can be created on the fresh table.
        ALTER INDEX IF EXISTS idx_sws_mint     RENAME TO idx_sws_v1_mint;
        ALTER INDEX IF EXISTS idx_sws_wallet   RENAME TO idx_sws_v1_wallet;
        ALTER INDEX IF EXISTS idx_sws_detected RENAME TO idx_sws_v1_detected;
    END IF;
END $$;

-- 2. Create the fresh smart_wallet_signals table with the same schema.
CREATE TABLE IF NOT EXISTS smart_wallet_signals (
    id BIGSERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    action TEXT NOT NULL DEFAULT 'buy',
    sol_amount DOUBLE PRECISION,
    token_amount DOUBLE PRECISION,
    price_at_signal DOUBLE PRECISION,
    price_5m DOUBLE PRECISION,
    price_30m DOUBLE PRECISION,
    price_1h DOUBLE PRECISION,
    price_24h DOUBLE PRECISION,
    peak_price DOUBLE PRECISION,
    peak_multiplier DOUBLE PRECISION,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sws_mint     ON smart_wallet_signals(mint);
CREATE INDEX IF NOT EXISTS idx_sws_wallet   ON smart_wallet_signals(wallet);
CREATE INDEX IF NOT EXISTS idx_sws_detected ON smart_wallet_signals(detected_at);

COMMIT;

-- ─────────────────────────────────────────────────────────────
--  Verification queries (run these after the migration):
-- ─────────────────────────────────────────────────────────────
-- SELECT 'archive' AS table_, COUNT(*) FROM smart_wallet_signals_v1_archive
-- UNION ALL
-- SELECT 'fresh',   COUNT(*) FROM smart_wallet_signals;
--
-- Expected: archive ≈ 1,552 rows, fresh = 0.
