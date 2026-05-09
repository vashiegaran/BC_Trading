use serde::Serialize;

/// All event types the logger can receive and write to Supabase.
///
/// Other engines send these through the logger's MPSC channel.
/// The logger batches them and flushes to the appropriate Supabase table.
#[derive(Debug, Serialize)]
pub enum LogEvent {
    /// A new token was detected by the detection engine.
    TokenDetected {
        mint: String,
        pool_address: Option<String>,
        source: String,
    },

    /// Result of running safety filters on a token.
    FilterResult {
        mint: String,
        passed: bool,
        fail_reason: Option<String>,
        rugcheck_score: Option<f64>,
        mint_authority: Option<bool>,
        freeze_authority: Option<bool>,
        bundled: Option<bool>,
        top_10_holder_pct: Option<f64>,
        liquidity_usd: Option<f64>,
        market_cap_usd: Option<f64>,
        price_impact_pct: Option<f64>,
        token_age_seconds: Option<i32>,
    },

    /// A new position was opened.
    PositionOpened {
        mint: String,
        entry_tx_sig: Option<String>,
        entry_price_usd: Option<f64>,
        sol_spent: Option<f64>,
        token_amount: Option<f64>,
        jito_tip_sol: Option<f64>,
        is_paper_trade: bool,
    },

    /// An existing position was partially closed.
    PositionUpdated {
        id: i64,
        tp1_triggered: Option<bool>,
        tp2_triggered: Option<bool>,
        peak_price_usd: Option<f64>,
        peak_multiplier: Option<f64>,
    },

    /// A position was fully closed.
    PositionClosed {
        id: i64,
        exit_tx_sig: Option<String>,
        exit_price_usd: Option<f64>,
        sol_received: Option<f64>,
        exit_reason: Option<String>,
        pnl_sol: Option<f64>,
        pnl_pct: Option<f64>,
    },

    /// End-of-day stats update.
    DailyStatsUpdate {
        trades_total: i32,
        trades_won: i32,
        trades_lost: i32,
        pnl_sol: f64,
    },

    /// Generic system event (startup, shutdown, error, alert, rpc_failover).
    SystemEvent { event_type: String, message: String },
}
