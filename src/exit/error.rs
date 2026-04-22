//! Typed exit error classification.
//!
//! Replaces the old string-matching scattered between the outer spawn handler
//! in [`crate::exit::start`] and the `is_slippage_error` helper in the confirm
//! loop. All classification now goes through [`ExitError::classify`] so the
//! retry-vs-mark-failed decision is made in exactly one place.
//!
//! Variants map directly to the post-exit outcome we want:
//!
//! - `OnChainSlippage`, `RateLimit`, `RouteNotFound`, `QuoteSanityFailed`,
//!   `Transient` — keep the position open; monitoring will re-fire the exit
//!   from a later trigger.
//! - `SwapNoOp`, `SlippageExhausted`, `Permanent` — mark the position
//!   `exit_failed`; the next trigger will not help.

use std::fmt;

#[derive(Debug, Clone)]
pub enum ExitError {
    /// On-chain swap failed with a slippage-family program error.
    OnChainSlippage(String),
    /// Jupiter / Helius / RPC rate limited us.
    RateLimit(String),
    /// Jupiter could not find a route right now. Usually transient.
    RouteNotFound(String),
    /// Pre-swap quote sanity check said the sell would lose money.
    /// Not retryable *this attempt*, but keep position open so monitoring
    /// can re-quote on the next trigger against fresh market state.
    QuoteSanityFailed(String),
    /// Tx confirmed but no tokens actually moved (Jupiter route touched a
    /// stale pool). Terminal — retrying with the same route won't help.
    SwapNoOp(String),
    /// We ran out of slippage escalation tiers.
    SlippageExhausted,
    /// Unclassified retryable error (generic HTTP / timeout / tokio).
    Transient(String),
    /// Terminal error: insufficient funds, signer refusal, non-slippage
    /// program error, etc. Mark position exit_failed.
    Permanent(String),
}

impl ExitError {
    /// Whether the exit engine should keep the position open for the next
    /// trigger (true) or mark it `exit_failed` (false).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::OnChainSlippage(_)
                | Self::RateLimit(_)
                | Self::RouteNotFound(_)
                | Self::QuoteSanityFailed(_)
                | Self::Transient(_)
        )
    }

    /// Stable short tag for DB logging / analytics. Keep these names stable
    /// across refactors — downstream queries group by this.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::OnChainSlippage(_) => "on_chain_slippage",
            Self::RateLimit(_) => "rate_limit",
            Self::RouteNotFound(_) => "route_not_found",
            Self::QuoteSanityFailed(_) => "quote_sanity_failed",
            Self::SwapNoOp(_) => "swap_no_op",
            Self::SlippageExhausted => "slippage_exhausted",
            Self::Transient(_) => "transient",
            Self::Permanent(_) => "permanent",
        }
    }

    /// Classify an error string (typically from `anyhow::Error::to_string()`).
    /// Callers should pass the string exactly as it will be persisted to the
    /// DB so that the classification and the logged reason agree.
    pub fn classify(err: &str) -> Self {
        let lower = err.to_ascii_lowercase();

        // Order matters: most-specific first.
        if is_slippage_marker(err) {
            return Self::OnChainSlippage(err.to_string());
        }

        if err.contains("Quote sanity check failed")
            || err.contains("TP sanity check failed")
            || lower.contains("would lose money")
        {
            return Self::QuoteSanityFailed(err.to_string());
        }

        if err.contains("NO_ROUTES_FOUND") || lower.contains("no routes found") {
            return Self::RouteNotFound(err.to_string());
        }

        if err.contains("429")
            || lower.contains("too many requests")
            || lower.contains("rate limit")
        {
            return Self::RateLimit(err.to_string());
        }

        if lower.contains("swap_no_op")
            || lower.contains("swap no-op")
            || lower.contains("swap did not execute")
        {
            return Self::SwapNoOp(err.to_string());
        }

        if err.contains("Exit failed after") && err.contains("retries") {
            return Self::SlippageExhausted;
        }

        // Network / transport blips — keep open, monitoring will re-fire.
        if lower.contains("timed out")
            || lower.contains("timeout")
            || lower.contains("connection")
            || lower.contains("dns")
            || lower.contains("tls")
            || lower.contains("eof")
            || lower.contains("hyper")
        {
            return Self::Transient(err.to_string());
        }

        // Anything else we treat as permanent so the position does not loop
        // forever. If we learn a new transient family, add it above.
        Self::Permanent(err.to_string())
    }
}

impl fmt::Display for ExitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OnChainSlippage(s)
            | Self::RateLimit(s)
            | Self::RouteNotFound(s)
            | Self::QuoteSanityFailed(s)
            | Self::SwapNoOp(s)
            | Self::Transient(s)
            | Self::Permanent(s) => write!(f, "{}: {}", self.tag(), s),
            Self::SlippageExhausted => write!(f, "{}", self.tag()),
        }
    }
}

/// Matches the slippage-family program-error codes already observed in the
/// logs. This is the same set the previous `is_slippage_error` helper used;
/// kept as a separate function so the classification table above stays
/// readable.
fn is_slippage_marker(err: &str) -> bool {
    err.contains("0x1788")                      // Raydium CLMM / Orca
        || err.contains("6024")                  // AMM slippage (decimal)
        || err.contains("0x1789")                // InvalidTickArraySequence
        || err.contains("6025")                  // slippage (decimal)
        || err.contains("0x1786")                // Raydium AmountTooSmall
        || err.contains("6001")                  // AMM InsufficientInputAmount
        || err.contains("0x1771")                // Hex variant
        || err.contains("SlippageToleranceExceeded")
        || err.contains("ExceededSlippage")
        || err.contains("on-chain slippage")
        || err.contains("slippage guard")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_slippage() {
        let e = ExitError::classify("Exit tx on-chain slippage: Custom(6024)");
        assert!(matches!(e, ExitError::OnChainSlippage(_)));
        assert!(e.is_retryable());
    }

    #[test]
    fn classifies_rate_limit() {
        let e = ExitError::classify("jupiter 429 Too Many Requests");
        assert!(matches!(e, ExitError::RateLimit(_)));
        assert!(e.is_retryable());
    }

    #[test]
    fn classifies_route_not_found() {
        let e = ExitError::classify("jupiter: NO_ROUTES_FOUND");
        assert!(matches!(e, ExitError::RouteNotFound(_)));
    }

    #[test]
    fn classifies_sanity_fail() {
        let e = ExitError::classify(
            "Quote sanity check failed: ratio=0.800, would lose money",
        );
        assert!(matches!(e, ExitError::QuoteSanityFailed(_)));
        assert!(e.is_retryable());
    }

    #[test]
    fn classifies_slippage_exhausted() {
        let e = ExitError::classify("Exit failed after 4 retries");
        assert!(matches!(e, ExitError::SlippageExhausted));
        assert!(!e.is_retryable());
    }

    #[test]
    fn classifies_permanent_default() {
        let e = ExitError::classify("TOKEN_NOT_TRADABLE");
        assert!(matches!(e, ExitError::Permanent(_)));
        assert!(!e.is_retryable());
    }

    #[test]
    fn classifies_transient_network() {
        let e = ExitError::classify("hyper client connection reset");
        assert!(matches!(e, ExitError::Transient(_)));
    }
}
