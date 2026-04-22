use serde::{Deserialize, Serialize};

/// Result of an exit operation (used for logging).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitResult {
    pub position_id: i64,
    pub mint: String,
    pub exit_reason: String,
    pub pct_sold: u8,
    pub pnl_sol: f64,
    pub pnl_pct: f64,
    pub is_paper_trade: bool,
}
