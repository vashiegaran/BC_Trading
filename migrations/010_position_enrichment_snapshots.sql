-- Migration 010: position_enrichment_snapshots
-- Time-series data collection during position hold phase — feeds v6 decisions.
-- Populated by src/monitoring/enrichment_sampler.rs.

CREATE TABLE IF NOT EXISTS position_enrichment_snapshots (
    id BIGSERIAL PRIMARY KEY,
    position_id BIGINT NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
    mint TEXT NOT NULL,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    elapsed_secs INTEGER NOT NULL,
    trigger TEXT NOT NULL,  -- 'scheduled' | 'pre_dip_death' | 'pre_tp1' | 'pre_tp2' | 'post_exit_1h'

    -- Price state
    price_usd DOUBLE PRECISION,
    pnl_pct DOUBLE PRECISION,
    peak_multiplier DOUBLE PRECISION,

    -- Holders (Helius DAS)
    holder_count INTEGER,
    holder_delta_from_prev INTEGER,
    top10_concentration_pct DOUBLE PRECISION,

    -- Volume / trading activity (Birdeye)
    vol_5m_usd DOUBLE PRECISION,
    vol_1h_usd DOUBLE PRECISION,
    vol_acceleration DOUBLE PRECISION,  -- current 5m / previous 5m
    buy_count_5m INTEGER,
    sell_count_5m INTEGER,
    buy_sell_ratio DOUBLE PRECISION,
    unique_traders_5m INTEGER,

    -- Social (DexScreener)
    social_count INTEGER,
    has_twitter BOOLEAN,
    has_telegram BOOLEAN,
    has_website BOOLEAN,
    new_social_links JSONB,

    -- Smart wallets (SolanaTracker)
    smart_wallet_buy_count INTEGER,
    smart_wallet_sell_count INTEGER,
    smart_wallet_net_sol DOUBLE PRECISION,
    smart_wallets JSONB,

    -- Whales (Helius RPC large trade detection)
    whale_buy_count INTEGER,
    whale_sell_count INTEGER,
    largest_trade_sol DOUBLE PRECISION,

    -- Dev wallet (Helius RPC)
    dev_wallet_sol_delta DOUBLE PRECISION,
    dev_wallet_token_balance DOUBLE PRECISION,

    -- Pool (DexScreener + Jupiter)
    liquidity_usd DOUBLE PRECISION,
    liquidity_delta_pct DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    price_impact_1sol_bps DOUBLE PRECISION,

    -- Tier 2 logging — pre-dip-death hypothetical suppression
    would_have_suppressed_dip_death BOOLEAN,
    suppression_reason TEXT,  -- e.g. 'smart_wallet_accumulating', 'holder_growth', 'none'

    -- Raw API payloads (for re-analysis without new API calls)
    raw_birdeye JSONB,
    raw_dexscreener JSONB,
    raw_solana_tracker JSONB,
    raw_das JSONB,

    -- API health per snapshot
    apis_called JSONB,   -- {"birdeye": 230, "das": 180, ...}  ms latencies
    apis_failed JSONB    -- {"birdeye": "timeout", "das": "circuit_open", ...}
);

CREATE INDEX IF NOT EXISTS idx_pes_position_id ON position_enrichment_snapshots(position_id);
CREATE INDEX IF NOT EXISTS idx_pes_mint ON position_enrichment_snapshots(mint);
CREATE INDEX IF NOT EXISTS idx_pes_trigger ON position_enrichment_snapshots(trigger);
CREATE INDEX IF NOT EXISTS idx_pes_snapshot_at ON position_enrichment_snapshots(snapshot_at DESC);
