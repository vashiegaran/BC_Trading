-- ═══════════════════════════════════════════════════════════════════
--  BC Research Bot — Combined Schema
--  Run this ONCE in your NEW Supabase SQL Editor.
--  Creates all tables, indexes, and views needed for the BC research bot.
-- ═══════════════════════════════════════════════════════════════════


-- ── positions ────────────────────────────────────────────────────
-- Core table: one row per trade (entry + exit patched onto same row).
CREATE TABLE IF NOT EXISTS positions (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mint                    TEXT NOT NULL,
    name                    TEXT,
    symbol                  TEXT,
    status                  TEXT NOT NULL DEFAULT 'open',
    is_paper_trade          BOOLEAN NOT NULL DEFAULT TRUE,
    entry_tx_sig            TEXT,
    entry_price_usd         DOUBLE PRECISION,
    sol_spent               DOUBLE PRECISION,
    token_amount            DOUBLE PRECISION,
    jito_tip_sol            DOUBLE PRECISION DEFAULT 0,
    tp1_triggered           BOOLEAN DEFAULT FALSE,
    tp2_triggered           BOOLEAN DEFAULT FALSE,
    peak_price_usd          DOUBLE PRECISION,
    peak_multiplier         DOUBLE PRECISION,
    pool_address            TEXT,
    dev_wallet              TEXT,
    detection_latency_ms    BIGINT,
    detection_source        TEXT,
    entry_hour_utc          INTEGER,
    concurrent_positions    INTEGER,
    strategy_version        TEXT,
    sniper_features         JSONB,
    post_trade_features     JSONB,
    monitoring_snapshot      JSONB,
    -- Narrative/moonbag columns
    narrative_state         TEXT,
    narrative_score         INTEGER,
    moonbag_promoted        BOOLEAN DEFAULT FALSE,
    moonbag_exit_reason     TEXT,
    moonbag_exit_multiplier DOUBLE PRECISION,
    moonbag_hold_duration_secs BIGINT,
    -- Exit columns (patched on close)
    exit_tx_sig             TEXT,
    exit_price_usd          DOUBLE PRECISION,
    exit_reason             TEXT,
    exit_slippage_bps       INTEGER,
    exit_attempts           INTEGER,
    pnl_pct                 DOUBLE PRECISION,
    pnl_sol                 DOUBLE PRECISION,
    sol_received            DOUBLE PRECISION,
    exit_time               TIMESTAMPTZ,
    closed_at               TIMESTAMPTZ,
    hold_duration_secs      BIGINT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_positions_mint ON positions(mint);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_created_at ON positions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy_version);


-- ── sniper_candidates ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sniper_candidates (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mint                    TEXT NOT NULL,
    symbol                  TEXT,
    name                    TEXT,
    pool_address            TEXT,
    creator_wallet          TEXT,
    action                  TEXT NOT NULL DEFAULT 'rejected',
    rejection_reason        TEXT,
    initial_liquidity_sol   DOUBLE PRECISION,
    sniper_features         JSONB,
    -- Counterfactual tracking
    price_1m                DOUBLE PRECISION,
    price_5m                DOUBLE PRECISION,
    price_15m               DOUBLE PRECISION,
    price_1h                DOUBLE PRECISION,
    peak_multiplier         DOUBLE PRECISION,
    -- Rug tracking
    rugged                  BOOLEAN,
    rug_time_secs           INTEGER,
    -- Score/filter
    sniper_score            DOUBLE PRECISION,
    filter_name             TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sniper_candidates_mint ON sniper_candidates(mint);
CREATE INDEX IF NOT EXISTS idx_sniper_candidates_action ON sniper_candidates(action);
CREATE INDEX IF NOT EXISTS idx_sniper_candidates_created_at ON sniper_candidates(created_at);


-- ── creator_reputation ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS creator_reputation (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wallet                  TEXT NOT NULL UNIQUE,
    total_launches          INTEGER NOT NULL DEFAULT 0,
    rug_count               INTEGER NOT NULL DEFAULT 0,
    avg_token_lifespan_min  DOUBLE PRECISION,
    last_launch_at          TIMESTAMPTZ,
    notes                   TEXT,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_creator_reputation_wallet ON creator_reputation(wallet);


-- ── pipeline_latency ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_latency (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mint                    TEXT NOT NULL,
    detected_at_ms          BIGINT,
    detection_to_sniper_ms  BIGINT,
    enrichment_total_ms     BIGINT,
    enrichment_per_source   JSONB,
    hard_filter_total_ms    BIGINT,
    filter_engine_total_ms  BIGINT,
    filter_per_check        JSONB,
    precheck_total_ms       BIGINT,
    execution_total_ms      BIGINT,
    post_buy_total_ms       BIGINT,
    post_buy_per_check      JSONB,
    pipeline_total_ms       BIGINT,
    outcome                 TEXT NOT NULL,
    rejection_stage         TEXT,
    rejection_reason        TEXT,
    position_id             BIGINT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_latency_mint ON pipeline_latency(mint);
CREATE INDEX IF NOT EXISTS idx_pipeline_latency_outcome ON pipeline_latency(outcome);
CREATE INDEX IF NOT EXISTS idx_pipeline_latency_created_at ON pipeline_latency(created_at);


-- ── trade_costs ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_costs (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id             BIGINT NOT NULL,
    mint                    TEXT NOT NULL,
    side                    TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    is_paper_trade          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    sol_amount              DOUBLE PRECISION,
    token_amount            DOUBLE PRECISION,
    token_price_usd         DOUBLE PRECISION,
    sol_usd_price           DOUBLE PRECISION,
    network_fee_sol         DOUBLE PRECISION DEFAULT 0,
    priority_fee_sol        DOUBLE PRECISION DEFAULT 0,
    jito_tip_sol            DOUBLE PRECISION DEFAULT 0,
    helius_tip_sol          DOUBLE PRECISION DEFAULT 0,
    total_fees_sol          DOUBLE PRECISION DEFAULT 0,
    slippage_bps            INT DEFAULT 0,
    expected_sol            DOUBLE PRECISION,
    actual_sol              DOUBLE PRECISION,
    slippage_cost_sol       DOUBLE PRECISION DEFAULT 0,
    wallet_sol_before       DOUBLE PRECISION,
    wallet_sol_after        DOUBLE PRECISION,
    wallet_sol_change       DOUBLE PRECISION,
    entry_sol_spent         DOUBLE PRECISION,
    exit_sol_received       DOUBLE PRECISION,
    total_round_trip_fees_sol DOUBLE PRECISION,
    gross_pnl_sol           DOUBLE PRECISION,
    net_pnl_sol             DOUBLE PRECISION,
    net_pnl_pct             DOUBLE PRECISION,
    tx_sig                  TEXT,
    exit_reason             TEXT,
    attempt_number          INT DEFAULT 1,
    execution_ms            BIGINT
);

CREATE INDEX IF NOT EXISTS idx_trade_costs_position ON trade_costs(position_id);
CREATE INDEX IF NOT EXISTS idx_trade_costs_mint ON trade_costs(mint);
CREATE INDEX IF NOT EXISTS idx_trade_costs_created ON trade_costs(created_at DESC);


-- ── trade_latency ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_latency (
    id                      BIGSERIAL PRIMARY KEY,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    position_id             BIGINT,
    mint                    TEXT NOT NULL,
    side                    TEXT NOT NULL,
    quote_ms                INTEGER,
    swap_tx_ms              INTEGER,
    sign_ms                 INTEGER,
    submit_confirm_ms       INTEGER,
    price_derive_ms         INTEGER,
    total_ms                INTEGER NOT NULL,
    used_jito               BOOLEAN,
    used_helius_sender      BOOLEAN,
    tx_sig                  TEXT,
    exit_reason             TEXT,
    attempts                INTEGER,
    slippage_bps            INTEGER
);

CREATE INDEX IF NOT EXISTS idx_trade_latency_position_id ON trade_latency(position_id);
CREATE INDEX IF NOT EXISTS idx_trade_latency_mint ON trade_latency(mint);
CREATE INDEX IF NOT EXISTS idx_trade_latency_side ON trade_latency(side);
CREATE INDEX IF NOT EXISTS idx_trade_latency_created_at ON trade_latency(created_at DESC);


-- ── moonbag_positions ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS moonbag_positions (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id             BIGINT NOT NULL,
    mint                    TEXT NOT NULL,
    token_name              TEXT,
    token_symbol            TEXT,
    narrative_state         TEXT NOT NULL,
    entry_price_usd         DOUBLE PRECISION NOT NULL DEFAULT 0,
    token_amount            DOUBLE PRECISION NOT NULL DEFAULT 0,
    sol_value               DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_price_usd          DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_multiplier         DOUBLE PRECISION NOT NULL DEFAULT 0,
    initial_trailing_pct    DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_hold_secs           BIGINT NOT NULL DEFAULT 0,
    profit_gate_multiplier  DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    profit_gate_reached     BOOLEAN NOT NULL DEFAULT FALSE,
    extension_checked       BOOLEAN NOT NULL DEFAULT FALSE,
    narrative_recheck_count INTEGER NOT NULL DEFAULT 0,
    exit_reason             TEXT,
    exit_price_usd          DOUBLE PRECISION,
    exit_multiplier         DOUBLE PRECISION,
    final_trailing_pct      DOUBLE PRECISION,
    hold_duration_secs      BIGINT,
    promoted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exited_at               TIMESTAMPTZ,
    is_paper_trade          BOOLEAN NOT NULL DEFAULT TRUE,
    strategy_version        TEXT
);

CREATE INDEX IF NOT EXISTS idx_moonbag_positions_mint ON moonbag_positions(mint);
CREATE INDEX IF NOT EXISTS idx_moonbag_positions_position_id ON moonbag_positions(position_id);
CREATE INDEX IF NOT EXISTS idx_moonbag_positions_promoted_at ON moonbag_positions(promoted_at);


-- ── narrative_checks ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS narrative_checks (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id             BIGINT,
    mint                    TEXT NOT NULL,
    token_name              TEXT,
    token_symbol            TEXT,
    check_phase             TEXT NOT NULL,
    check_index             INTEGER,
    narrative_state         TEXT NOT NULL,
    score                   INTEGER NOT NULL DEFAULT 0,
    narrative_strength      DOUBLE PRECISION NOT NULL DEFAULT 0,
    market_strength         DOUBLE PRECISION NOT NULL DEFAULT 0,
    web_sources_found       INTEGER NOT NULL DEFAULT 0,
    reasons                 JSONB DEFAULT '[]'::jsonb,
    risk_flags              JSONB DEFAULT '[]'::jsonb,
    current_price_usd       DOUBLE PRECISION,
    entry_price_usd         DOUBLE PRECISION,
    peak_multiplier         DOUBLE PRECISION,
    hold_seconds            BIGINT,
    momentum_ratio          DOUBLE PRECISION,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_narrative_checks_mint ON narrative_checks(mint);
CREATE INDEX IF NOT EXISTS idx_narrative_checks_position_id ON narrative_checks(position_id);
CREATE INDEX IF NOT EXISTS idx_narrative_checks_created_at ON narrative_checks(created_at);


-- ── tokens_seen ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tokens_seen (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mint                    TEXT NOT NULL,
    source                  TEXT,
    detected_at             TIMESTAMPTZ,
    initial_liquidity_sol   DOUBLE PRECISION,
    name                    TEXT,
    symbol                  TEXT,
    pool_address            TEXT,
    creator_wallet          TEXT
);

CREATE INDEX IF NOT EXISTS idx_tokens_seen_mint ON tokens_seen(mint);
CREATE INDEX IF NOT EXISTS idx_tokens_seen_detected ON tokens_seen(detected_at);


-- ── filter_results ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS filter_results (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mint                    TEXT NOT NULL,
    passed                  BOOLEAN,
    fail_reason             TEXT,
    rugcheck_score          DOUBLE PRECISION,
    mint_authority          BOOLEAN,
    freeze_authority        BOOLEAN,
    bundled                 BOOLEAN,
    top_10_holder_pct       DOUBLE PRECISION,
    liquidity_usd           DOUBLE PRECISION,
    market_cap_usd          DOUBLE PRECISION,
    price_impact_pct        DOUBLE PRECISION,
    token_age_seconds       BIGINT,
    checked_at              TIMESTAMPTZ,
    check_details           JSONB
);

CREATE INDEX IF NOT EXISTS idx_filter_results_mint ON filter_results(mint);
CREATE INDEX IF NOT EXISTS idx_filter_results_checked ON filter_results(checked_at);


-- ── system_events ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS system_events (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type              TEXT,
    message                 TEXT,
    occurred_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_system_events_type ON system_events(event_type);
CREATE INDEX IF NOT EXISTS idx_system_events_at ON system_events(occurred_at);


-- ── shadow_log ───────────────────────────────────────────────────
-- Post-exit price shadow logging for missed-gain analysis.
CREATE TABLE IF NOT EXISTS shadow_log (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id             BIGINT,
    mint                    TEXT NOT NULL,
    entry_price_usd         DOUBLE PRECISION,
    snapshots               JSONB,
    shadow_peak_usd         DOUBLE PRECISION,
    shadow_peak_multiplier  DOUBLE PRECISION,
    shadow_low_usd          DOUBLE PRECISION,
    total_ticks             BIGINT,
    exit_at_secs            BIGINT,
    exit_reason             TEXT,
    duration_secs           BIGINT,
    completed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shadow_log_position ON shadow_log(position_id);
CREATE INDEX IF NOT EXISTS idx_shadow_log_mint ON shadow_log(mint);


-- ── daily_stats ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_stats (
    date                    TEXT PRIMARY KEY,
    trades_total            INTEGER DEFAULT 0,
    trades_won              INTEGER DEFAULT 0,
    trades_lost             INTEGER DEFAULT 0,
    pnl_sol                 DOUBLE PRECISION DEFAULT 0
);


-- ── position_enrichment_snapshots ────────────────────────────────
CREATE TABLE IF NOT EXISTS position_enrichment_snapshots (
    id                      BIGSERIAL PRIMARY KEY,
    position_id             BIGINT NOT NULL,
    mint                    TEXT NOT NULL,
    snapshot_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    elapsed_secs            INTEGER NOT NULL,
    trigger                 TEXT NOT NULL,
    price_usd               DOUBLE PRECISION,
    pnl_pct                 DOUBLE PRECISION,
    peak_multiplier         DOUBLE PRECISION,
    holder_count            INTEGER,
    holder_delta_from_prev  INTEGER,
    top10_concentration_pct DOUBLE PRECISION,
    vol_5m_usd              DOUBLE PRECISION,
    vol_1h_usd              DOUBLE PRECISION,
    vol_acceleration        DOUBLE PRECISION,
    buy_count_5m            INTEGER,
    sell_count_5m           INTEGER,
    buy_sell_ratio          DOUBLE PRECISION,
    unique_traders_5m       INTEGER,
    social_count            INTEGER,
    has_twitter             BOOLEAN,
    has_telegram            BOOLEAN,
    has_website             BOOLEAN,
    new_social_links        JSONB,
    smart_wallet_buy_count  INTEGER,
    smart_wallet_sell_count INTEGER,
    smart_wallet_net_sol    DOUBLE PRECISION,
    smart_wallets           JSONB,
    whale_buy_count         INTEGER,
    whale_sell_count        INTEGER,
    largest_trade_sol       DOUBLE PRECISION,
    dev_wallet_sol_delta    DOUBLE PRECISION,
    dev_wallet_token_balance DOUBLE PRECISION,
    liquidity_usd           DOUBLE PRECISION,
    liquidity_delta_pct     DOUBLE PRECISION,
    market_cap_usd          DOUBLE PRECISION,
    price_impact_1sol_bps   DOUBLE PRECISION,
    would_have_suppressed_dip_death BOOLEAN,
    suppression_reason      TEXT,
    raw_birdeye             JSONB,
    raw_dexscreener         JSONB,
    raw_solana_tracker      JSONB,
    raw_das                 JSONB,
    apis_called             JSONB,
    apis_failed             JSONB
);

CREATE INDEX IF NOT EXISTS idx_pes_position_id ON position_enrichment_snapshots(position_id);
CREATE INDEX IF NOT EXISTS idx_pes_mint ON position_enrichment_snapshots(mint);
CREATE INDEX IF NOT EXISTS idx_pes_trigger ON position_enrichment_snapshots(trigger);
CREATE INDEX IF NOT EXISTS idx_pes_snapshot_at ON position_enrichment_snapshots(snapshot_at DESC);


-- ══════════════════════════════════════════════════════════════════
--  BC RESEARCH TABLES
-- ══════════════════════════════════════════════════════════════════

-- ── bonding_curve_signals ────────────────────────────────────────
-- Records BC trade patterns for tokens approaching graduation.
CREATE TABLE IF NOT EXISTS bonding_curve_signals (
    id                      BIGSERIAL PRIMARY KEY,
    mint                    TEXT NOT NULL,
    name                    TEXT,
    symbol                  TEXT,
    creator_wallet          TEXT,
    token_created_at        BIGINT,
    signal_recorded_at      BIGINT,
    token_age_secs          DOUBLE PRECISION,
    total_volume_sol        DOUBLE PRECISION,
    buy_count               INTEGER,
    sell_count              INTEGER,
    unique_buyers           INTEGER,
    initial_buy_sol         DOUBLE PRECISION,
    trades                  JSONB,
    signals                 JSONB,
    price_1m_multiplier     DOUBLE PRECISION,
    price_5m_multiplier     DOUBLE PRECISION,
    price_15m_multiplier    DOUBLE PRECISION,
    price_1h_multiplier     DOUBLE PRECISION,
    peak_multiplier         DOUBLE PRECISION,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bcs_mint ON bonding_curve_signals(mint);
CREATE INDEX IF NOT EXISTS idx_bcs_created ON bonding_curve_signals(created_at);


-- ── bc_gate_backtest ─────────────────────────────────────────────
-- PnL backtest results from scripts/backfill_bc_gate_pnl.py.
CREATE TABLE IF NOT EXISTS bc_gate_backtest (
    mint                    TEXT PRIMARY KEY,
    symbol                  TEXT,
    signal_recorded_at      TIMESTAMPTZ NOT NULL,
    matches_gate_v1         BOOLEAN,
    graduated               BOOLEAN,
    bsr                     DOUBLE PRECISION,
    unique_buyers           INTEGER,
    creator_rebuy           BOOLEAN,
    total_volume_sol        DOUBLE PRECISION,
    token_age_secs          DOUBLE PRECISION,
    price_at_signal         DOUBLE PRECISION,
    price_peak              DOUBLE PRECISION,
    peak_multiplier         DOUBLE PRECISION,
    time_to_peak_mins       DOUBLE PRECISION,
    max_drawdown_pct        DOUBLE PRECISION,
    price_24h               DOUBLE PRECISION,
    sim_tp30_sl20_pnl_pct   DOUBLE PRECISION,
    sim_tp30_sl20_reason    TEXT,
    sim_tp50_sl30_pnl_pct   DOUBLE PRECISION,
    sim_tp50_sl30_reason    TEXT,
    sim_tp100_sl30_pnl_pct  DOUBLE PRECISION,
    sim_tp100_sl30_reason   TEXT,
    sim_tp100_sl50_pnl_pct  DOUBLE PRECISION,
    sim_tp100_sl50_reason   TEXT,
    sim_tp200_sl50_pnl_pct  DOUBLE PRECISION,
    sim_tp200_sl50_reason   TEXT,
    candles_used            INTEGER,
    backfill_source         TEXT,
    backfill_error          TEXT,
    backfilled_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bcgb_signal_at ON bc_gate_backtest(signal_recorded_at);
CREATE INDEX IF NOT EXISTS idx_bcgb_gate ON bc_gate_backtest(matches_gate_v1);
CREATE INDEX IF NOT EXISTS idx_bcgb_grad ON bc_gate_backtest(graduated);


-- ══════════════════════════════════════════════════════════════════
--  VIEWS
-- ══════════════════════════════════════════════════════════════════

-- Missed opportunities (rejected tokens that pumped)
CREATE OR REPLACE VIEW sniper_missed_opportunities AS
SELECT id, mint, symbol, name, rejection_reason, initial_liquidity_sol,
       peak_multiplier, price_1m, price_5m, price_15m, price_1h, created_at
FROM sniper_candidates
WHERE action = 'rejected' AND peak_multiplier IS NOT NULL AND peak_multiplier > 2.0
ORDER BY peak_multiplier DESC;

-- Creator reputation summary
CREATE OR REPLACE VIEW creator_reputation_summary AS
SELECT wallet, total_launches, rug_count,
       CASE WHEN total_launches > 0
            THEN ROUND(rug_count::numeric / total_launches * 100, 1)
            ELSE 0 END AS rug_pct,
       avg_token_lifespan_min, last_launch_at
FROM creator_reputation
ORDER BY total_launches DESC;

-- Training data (candidates joined with positions)
CREATE OR REPLACE VIEW sniper_training_data AS
SELECT sc.id AS candidate_id, sc.mint, sc.symbol, sc.name, sc.creator_wallet,
       sc.initial_liquidity_sol, sc.sniper_features,
       p.id AS position_id, p.entry_price_usd, p.exit_price_usd, p.sol_spent,
       p.pnl_pct, p.pnl_sol, p.peak_price_usd, p.peak_multiplier,
       p.hold_duration_secs, p.exit_reason, p.post_trade_features,
       p.monitoring_snapshot, p.detection_latency_ms, p.is_paper_trade,
       p.created_at AS trade_opened_at, p.closed_at AS trade_closed_at,
       sc.created_at AS detected_at
FROM sniper_candidates sc
INNER JOIN positions p ON p.mint = sc.mint
WHERE sc.action = 'bought'
ORDER BY sc.created_at DESC;

-- Trade cost summary
CREATE OR REPLACE VIEW trade_cost_summary AS
SELECT b.position_id, b.mint, b.is_paper_trade,
       b.sol_amount AS buy_sol_spent, b.token_price_usd AS entry_price_usd,
       b.total_fees_sol AS buy_fees_sol, b.slippage_cost_sol AS buy_slippage_cost_sol,
       s.sol_amount AS sell_sol_received, s.token_price_usd AS exit_price_usd,
       s.total_fees_sol AS sell_fees_sol, s.slippage_cost_sol AS sell_slippage_cost_sol,
       s.exit_reason,
       COALESCE(b.total_fees_sol,0) + COALESCE(s.total_fees_sol,0) AS total_fees_sol,
       COALESCE(b.slippage_cost_sol,0) + COALESCE(s.slippage_cost_sol,0) AS total_slippage_cost_sol,
       s.net_pnl_sol, s.net_pnl_pct
FROM trade_costs b
LEFT JOIN trade_costs s ON s.position_id = b.position_id AND s.side = 'sell'
WHERE b.side = 'buy';
