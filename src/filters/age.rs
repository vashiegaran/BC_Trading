use tracing::debug;

use super::types::FilterResult;
use crate::config::AppConfig;
use crate::detection::types::{DetectionSource, GraduatedToken};

const CHECK_NAME: &str = "age";

pub struct AgeFilter;

impl AgeFilter {
    pub fn new() -> Self {
        Self
    }

    /// Check token freshness and graduation speed.
    ///
    /// This is a **synchronous** check — no network calls needed.
    pub fn check(&self, token: &GraduatedToken, cfg: &AppConfig) -> FilterResult {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let age_secs = ((now_ms - token.detected_at) as f64 / 1_000.0).max(0.0);
        let max_age = cfg.strategy.filters.max_token_age_seconds as f64;

        debug!(
            mint = %token.mint,
            age_secs,
            max_age,
            graduation_secs = token.time_to_graduate_seconds,
            "age check"
        );

        if age_secs > max_age {
            let source_str = match token.source {
                DetectionSource::PumpFun => "pump.fun",
                DetectionSource::Poll => "raydium_logs",
                _ => "unknown",
            };
            tracing::warn!(
                mint = %token.mint,
                token_age_seconds = age_secs as u64,
                max_allowed = max_age as u64,
                source = source_str,
                "❌ Age filter failed — token too old"
            );
            return FilterResult::fail(
                CHECK_NAME,
                &format!("age_{:.0}s_exceeds_max_{:.0}s", age_secs, max_age),
            );
        }

        // Graduation time check — slow graduation = weak demand
        let max_graduation_time_secs = cfg.strategy.filters.max_graduation_time_seconds;
        if token.time_to_graduate_seconds > max_graduation_time_secs {
            return FilterResult::fail(
                CHECK_NAME,
                &format!(
                    "graduation_took_{:.0}s_exceeds_max_{:.0}s",
                    token.time_to_graduate_seconds, max_graduation_time_secs
                ),
            );
        }

        tracing::info!(
            mint = %token.mint,
            token_age_seconds = age_secs as u64,
            "✅ Age filter passed"
        );

        FilterResult::pass(CHECK_NAME)
    }
}
