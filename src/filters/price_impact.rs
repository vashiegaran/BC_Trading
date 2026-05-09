use tracing::{debug, warn};

use super::types::FilterResult;
use crate::config::AppConfig;

const CHECK_NAME: &str = "price_impact";

pub struct PriceImpactFilter;

impl PriceImpactFilter {
    pub fn new() -> Self {
        Self
    }

    /// Estimate price impact mathematically from pool liquidity.
    ///
    /// For a constant-product AMM: impact ≈ buy_sol / pool_sol_reserve.
    /// This replaces the Jupiter quote call — zero latency, zero API calls.
    /// The execution engine fetches a fresh Jupiter quote for the actual trade.
    pub fn check_from_liquidity(
        &self,
        initial_liquidity_sol: f64,
        cfg: &AppConfig,
    ) -> FilterResult {
        let buy_sol = cfg.strategy.execution.buy_amount_sol;

        if initial_liquidity_sol <= 0.0 {
            warn!("price_impact: no liquidity data — soft passing");
            return FilterResult::pass(CHECK_NAME);
        }

        // Constant-product AMM price impact: buy_amount / pool_reserve
        // Pool reserve ≈ initial_liquidity_sol (the SOL side of the pool)
        let estimated_impact_pct = (buy_sol / initial_liquidity_sol) * 100.0;

        debug!(
            buy_sol,
            pool_sol = initial_liquidity_sol,
            estimated_impact_pct,
            max = cfg.strategy.filters.max_price_impact_pct,
            "price_impact estimated from pool math"
        );

        if estimated_impact_pct > cfg.strategy.filters.max_price_impact_pct {
            return FilterResult::fail(
                CHECK_NAME,
                &format!(
                    "estimated_impact_{:.2}pct_exceeds_max_{:.2}pct",
                    estimated_impact_pct, cfg.strategy.filters.max_price_impact_pct
                ),
            );
        }

        FilterResult::pass(CHECK_NAME)
    }
}
