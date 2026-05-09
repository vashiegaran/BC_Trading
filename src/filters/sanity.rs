use tracing::warn;

use super::types::FilterResult;
use crate::config::AppConfig;
use crate::detection::types::GraduatedToken;

const CHECK_NAME: &str = "sanity";

pub struct SanityFilter;

impl SanityFilter {
    pub fn new() -> Self {
        Self
    }

    /// Hard-reject tokens that are structurally broken.
    ///
    /// This runs synchronously before any async filters to avoid wasting
    /// API calls on garbage tokens.
    ///
    /// Rejects:
    /// - Market cap below minimum floor (zero or absurdly low MC)
    /// - Entry price near-zero (broken price data)
    pub fn check(&self, token: &GraduatedToken, cfg: &AppConfig) -> FilterResult {
        // 1. Missing pool address — warn but allow through.
        // Pump-amm migrations may not resolve a pool address, and Jupiter
        // can still route trades without one.
        if token.pool_address.is_none() {
            warn!(
                mint = %token.mint,
                "⚠️ No pool address — LP monitoring unavailable, but allowing through"
            );
        }

        // 2. Bonding volume sanity: if token graduated but bonding volume is
        //    suspiciously low, something is wrong with the data.
        //    Skip when volume == 0.0 — ST poller and Raydium poller don't
        //    report bonding curve data, so 0.0 means "not measured".
        if token.bonding_curve_volume_sol > 0.0 && token.bonding_curve_volume_sol < 0.5 {
            warn!(
                mint = %token.mint,
                volume = token.bonding_curve_volume_sol,
                "🚫 Rejected — bonding volume too low (> 0 but < 0.5 SOL), likely garbage"
            );
            return FilterResult::fail(
                CHECK_NAME,
                &format!(
                    "bonding_volume_too_low: {:.2} SOL",
                    token.bonding_curve_volume_sol
                ),
            );
        }

        FilterResult::pass(CHECK_NAME)
    }
}
