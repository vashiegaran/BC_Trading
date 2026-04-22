use serde::{Deserialize, Serialize};

/// Reason why an exit was triggered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    TakeProfit1,
    TakeProfit2,
    TakeProfit3,
    StopLoss,
    TrailingStop,
    TimeStop,
    VolumeDrop,
    DevWalletDumping,
    LiquidityRemoved,
    DipDeath,
    PostFillSanity,
    PostBuyVerificationFailed,
    WhaleDump,
    SellPressure,
    BuyersFading,
    StealthDump,
    DeadInterest,
    VolumeCliff,
    RoundRejection,
    LowerHigh,
    MomentumKill,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TakeProfit1 => write!(f, "tp1"),
            Self::TakeProfit2 => write!(f, "tp2"),
            Self::TakeProfit3 => write!(f, "tp3"),
            Self::StopLoss => write!(f, "stop_loss"),
            Self::TrailingStop => write!(f, "trailing_stop"),
            Self::TimeStop => write!(f, "time_stop"),
            Self::VolumeDrop => write!(f, "volume_drop"),
            Self::DevWalletDumping => write!(f, "dev_wallet_dumping"),
            Self::LiquidityRemoved => write!(f, "liquidity_removed"),
            Self::DipDeath => write!(f, "dip_death"),
            Self::PostFillSanity => write!(f, "post_fill_sanity"),
            Self::PostBuyVerificationFailed => write!(f, "post_buy_verification_failed"),
            Self::WhaleDump => write!(f, "whale_dump"),
            Self::SellPressure => write!(f, "sell_pressure"),
            Self::BuyersFading => write!(f, "buyers_fading"),
            Self::StealthDump => write!(f, "stealth_dump"),
            Self::DeadInterest => write!(f, "dead_interest"),
            Self::VolumeCliff => write!(f, "volume_cliff"),
            Self::RoundRejection => write!(f, "round_rejection"),
            Self::LowerHigh => write!(f, "lower_high"),
            Self::MomentumKill => write!(f, "momentum_kill"),
        }
    }
}

/// Result sent from the exit engine back to the monitoring engine
/// to confirm whether a sell succeeded or failed.
#[derive(Debug, Clone)]
pub struct ExitResult {
    /// Token mint address.
    pub mint: String,
    /// The reason that was attempted.
    pub reason: ExitReason,
    /// Whether the sell was confirmed on-chain (or paper-confirmed).
    pub success: bool,
    /// If true, the failure is permanent (e.g. TOKEN_NOT_TRADABLE) — do not retry.
    pub permanent: bool,
}

/// Signal sent from the monitoring engine to the exit engine when
/// a position should be (partially or fully) closed.
#[derive(Debug, Clone)]
pub struct ExitSignal {
    /// Supabase row ID for the position.
    pub position_id: i64,
    /// Token mint address.
    pub mint: String,
    /// Percentage of the remaining position to sell (1–100).
    pub pct_to_sell: u8,
    /// Why we're exiting.
    pub reason: ExitReason,
    /// Current token price in USD at the moment the trigger fired.
    pub current_price: f64,
    /// Entry price in USD (needed for PnL calculation in exit engine).
    pub entry_price_usd: f64,
    /// SOL originally spent on this position.
    pub sol_spent: f64,
    /// Token amount in the position (remaining).
    pub token_amount: f64,
    /// Whether this is a paper trade.
    pub is_paper_trade: bool,
    /// Optional sub-reason for the exit (e.g. specific dip_death trigger:
    /// "whale_sell_during_dip", "sell_acceleration", "dip_grace_expired", etc.)
    /// Appended to `exit_reason` in DB for post-hoc tuning analysis.
    pub sub_reason: Option<String>,
}
