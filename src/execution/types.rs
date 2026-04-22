use serde::{Deserialize, Serialize};

/// Represents an open (or closed) position stored in Supabase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// Supabase row ID (assigned by the database).
    #[serde(default)]
    pub id: i64,
    /// Token mint address.
    pub mint: String,
    /// Position status: "open" | "partial" | "closed" | "paper".
    pub status: String,
    /// Whether this is a simulated (paper) trade.
    pub is_paper_trade: bool,

    // ── Entry ────────────────────────────────────────────
    pub entry_tx_sig: Option<String>,
    pub entry_price_usd: Option<f64>,
    #[serde(default)]
    pub sol_spent: Option<f64>,
    pub token_amount: Option<f64>,
    pub jito_tip_sol: Option<f64>,

    // ── Exit ─────────────────────────────────────────────
    pub exit_tx_sig: Option<String>,
    pub exit_price_usd: Option<f64>,
    pub sol_received: Option<f64>,
    pub exit_reason: Option<String>,

    // ── PnL ──────────────────────────────────────────────
    pub pnl_sol: Option<f64>,
    pub pnl_pct: Option<f64>,

    // ── Tracking ─────────────────────────────────────────
    #[serde(default)]
    pub tp1_triggered: bool,
    #[serde(default)]
    pub tp2_triggered: bool,
    pub peak_price_usd: Option<f64>,
    pub peak_multiplier: Option<f64>,
}

/// Event sent from the execution engine to the monitoring engine.
#[derive(Debug, Clone)]
pub struct PositionOpened {
    /// Supabase row ID for this position.
    pub position_id: i64,
    /// Token mint address.
    pub mint: String,
    /// Entry price in USD.
    pub entry_price_usd: f64,
    /// SOL spent on this trade.
    pub sol_spent: f64,
    /// Number of tokens received.
    pub token_amount: f64,
    /// Whether this is a paper trade.
    pub is_paper_trade: bool,
    /// Dev (creator) wallet address — used for dev-wallet monitoring.
    pub dev_wallet: Option<String>,
    /// Dev wallet's initial token balance at time of position open.
    pub dev_initial_balance: Option<u64>,
    /// Raydium pool address — used for LP vault monitoring.
    pub pool_address: Option<String>,
    /// Sniper enrichment features JSONB — persisted with the position for post-trade analysis.
    pub sniper_features: Option<serde_json::Value>,
    /// Initial pool liquidity in SOL at detection time — used for low-liq exit param overrides.
    pub initial_liquidity_sol: f64,
    /// Which detection source found this token (for win-rate-by-source analysis).
    pub detection_source: String,
    /// Token name (for narrative detection).
    pub token_name: String,
    /// Token ticker symbol (for narrative detection).
    pub token_symbol: String,
}
