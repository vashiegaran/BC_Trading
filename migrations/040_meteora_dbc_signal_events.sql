-- ═══════════════════════════════════════════════════════════════════
--  Migration 040: Meteora DBC immutable shadow signal events
--
--  Stores the exact snapshot when a Meteora DBC row first passes the
--  narrowed shadow canary. This is observe-only research data; no live
--  execution path reads this table.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS meteora_dbc_signal_events (
    id                              BIGSERIAL PRIMARY KEY,
    mint                            TEXT NOT NULL,
    symbol                          TEXT,
    name                            TEXT,
    rule_version                    TEXT NOT NULL,
    fired_at                        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    meteora_dbc_score               DOUBLE PRECISION,
    market_cap_usd                  DOUBLE PRECISION,
    price_usd                       DOUBLE PRECISION,
    liquidity_usd                   DOUBLE PRECISION,
    volume_h1_usd                   DOUBLE PRECISION,
    txns_h1_buys                    INTEGER,
    txns_h1_sells                   INTEGER,
    buy_pressure_h1_pct             DOUBLE PRECISION,
    buy_sell_ratio_h1               DOUBLE PRECISION,
    price_change_h1_pct             DOUBLE PRECISION,

    dbc_pair_address                TEXT,
    dbc_pair_url                    TEXT,
    meteora_pair_address            TEXT,
    meteora_pair_url                TEXT,

    score_reasons                   JSONB,
    score_penalties                 JSONB,
    entry_snapshot                  JSONB NOT NULL,

    outcome_peak_multiplier         DOUBLE PRECISION,
    outcome_peak_market_cap_usd     DOUBLE PRECISION,
    outcome_last_seen_at            TIMESTAMPTZ,
    outcome_backfilled_at           TIMESTAMPTZ,

    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (mint, rule_version)
);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_signal_events_fired_at
    ON meteora_dbc_signal_events(fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_signal_events_rule_version
    ON meteora_dbc_signal_events(rule_version, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_meteora_dbc_signal_events_peak
    ON meteora_dbc_signal_events(outcome_peak_multiplier DESC);

COMMENT ON TABLE meteora_dbc_signal_events IS
    'Immutable observe-only Meteora DBC shadow canary events. Not read by live execution.';

COMMENT ON COLUMN meteora_dbc_signal_events.rule_version IS
    'Narrowed shadow rule that fired, e.g. numeric Tier A score/buy-pressure/market-cap profile.';

COMMENT ON COLUMN meteora_dbc_signal_events.entry_snapshot IS
    'Full DexScreener/Supabase payload at the moment the shadow canary first passed.';