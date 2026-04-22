-- Shadow strategy data collection tables.
-- These are observe-only — no trading decisions are made from this data.
-- After 1-2 weeks of collection, analyze to decide which strategies to activate.

-- ── 1. Smart Wallet Signals (Copy Trading) ───────────────────
CREATE TABLE IF NOT EXISTS smart_wallet_signals (
    id BIGSERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    action TEXT NOT NULL DEFAULT 'buy',  -- buy / sell
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
CREATE INDEX IF NOT EXISTS idx_sws_mint ON smart_wallet_signals(mint);
CREATE INDEX IF NOT EXISTS idx_sws_wallet ON smart_wallet_signals(wallet);
CREATE INDEX IF NOT EXISTS idx_sws_detected ON smart_wallet_signals(detected_at);

-- ── 2. CTO Watchlist ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cto_watchlist (
    id BIGSERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    rejection_reason TEXT,
    volume_at_reject DOUBLE PRECISION,
    holders_at_reject INTEGER,
    price_at_reject DOUBLE PRECISION,
    -- Recovery check columns (filled by background poller)
    volume_1h DOUBLE PRECISION,
    volume_6h DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    holders_1h INTEGER,
    holders_6h INTEGER,
    holders_24h INTEGER,
    price_1h DOUBLE PRECISION,
    price_6h DOUBLE PRECISION,
    price_24h DOUBLE PRECISION,
    recovery_detected BOOLEAN DEFAULT FALSE,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_cto_mint ON cto_watchlist(mint);
CREATE INDEX IF NOT EXISTS idx_cto_recovery ON cto_watchlist(recovery_detected);
CREATE INDEX IF NOT EXISTS idx_cto_detected ON cto_watchlist(detected_at);

-- ── 3. Narrative Signals (Meta Tracker) ──────────────────────
CREATE TABLE IF NOT EXISTS narrative_signals (
    id BIGSERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    category TEXT,               -- AI, cat, political, dog, frog, etc.
    narrative_score DOUBLE PRECISION,
    meta_hot BOOLEAN DEFAULT FALSE,  -- true if 3+ tokens in same category pumped
    price_at_signal DOUBLE PRECISION,
    price_1h DOUBLE PRECISION,
    price_6h DOUBLE PRECISION,
    price_24h DOUBLE PRECISION,
    peak_multiplier DOUBLE PRECISION,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_ns_category ON narrative_signals(category);
CREATE INDEX IF NOT EXISTS idx_ns_meta ON narrative_signals(meta_hot);
CREATE INDEX IF NOT EXISTS idx_ns_detected ON narrative_signals(detected_at);

-- ── 4. Dip Watchlist ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dip_watchlist (
    id BIGSERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    entry_price DOUBLE PRECISION,
    ath_price DOUBLE PRECISION,
    ath_at TIMESTAMPTZ,
    current_price DOUBLE PRECISION,
    drawdown_pct DOUBLE PRECISION,    -- % drop from ATH
    holder_count INTEGER,
    holder_retention_pct DOUBLE PRECISION,  -- % of holders that stayed
    dip_signal BOOLEAN DEFAULT FALSE, -- true when 50-70% dip + stable holders
    price_after_signal_1h DOUBLE PRECISION,
    price_after_signal_6h DOUBLE PRECISION,
    price_after_signal_24h DOUBLE PRECISION,
    source_position_id BIGINT,        -- which position this came from
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dw_mint ON dip_watchlist(mint);
CREATE INDEX IF NOT EXISTS idx_dw_signal ON dip_watchlist(dip_signal);
CREATE INDEX IF NOT EXISTS idx_dw_detected ON dip_watchlist(detected_at);

-- ── 5. Raydium Direct Launches ───────────────────────────────
CREATE TABLE IF NOT EXISTS raydium_direct_launches (
    id BIGSERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    pool_address TEXT,
    initial_liquidity_sol DOUBLE PRECISION,
    price_at_detection DOUBLE PRECISION,
    price_5m DOUBLE PRECISION,
    price_30m DOUBLE PRECISION,
    price_1h DOUBLE PRECISION,
    price_24h DOUBLE PRECISION,
    peak_price DOUBLE PRECISION,
    peak_multiplier DOUBLE PRECISION,
    passed_filters BOOLEAN,         -- would it have passed our filters?
    filter_rejection TEXT,          -- why it failed (if applicable)
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_rdl_mint ON raydium_direct_launches(mint);
CREATE INDEX IF NOT EXISTS idx_rdl_detected ON raydium_direct_launches(detected_at);

-- ── 6. Volume Spike Signals ──────────────────────────────────
CREATE TABLE IF NOT EXISTS volume_spike_signals (
    id BIGSERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    token_age_hours DOUBLE PRECISION,
    avg_volume_sol DOUBLE PRECISION,   -- average volume over past 24h
    spike_volume_sol DOUBLE PRECISION, -- volume in spike window
    spike_multiplier DOUBLE PRECISION, -- spike_volume / avg_volume
    price_at_signal DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    price_1h DOUBLE PRECISION,
    price_6h DOUBLE PRECISION,
    price_24h DOUBLE PRECISION,
    peak_price DOUBLE PRECISION,
    peak_multiplier DOUBLE PRECISION,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_vss_mint ON volume_spike_signals(mint);
CREATE INDEX IF NOT EXISTS idx_vss_spike ON volume_spike_signals(spike_multiplier);
CREATE INDEX IF NOT EXISTS idx_vss_detected ON volume_spike_signals(detected_at);
