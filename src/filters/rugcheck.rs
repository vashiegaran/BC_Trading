use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::config::AppConfig;
use super::types::{FilterResult, RugCheckReport};

const CHECK_NAME: &str = "rugcheck";
const RUGCHECK_BASE_URL: &str = "https://api.rugcheck.xyz/v1/tokens";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CACHE_TTL: Duration = Duration::from_secs(300); // 5 minutes
const RATE_LIMIT_WAIT: Duration = Duration::from_secs(2);

// ─── Cache entry ─────────────────────────────────────────────

struct CacheEntry {
    report: RugCheckReport,
    inserted_at: Instant,
}

// ─── Filter ──────────────────────────────────────────────────

pub struct RugCheckFilter {
    client: Client,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl RugCheckFilter {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("Failed to build rugcheck HTTP client");
        Self {
            client,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Run the rug-check filter.
    ///
    /// Returns `(FilterResult, Option<score>)`.
    pub async fn check(&self, mint: &str, cfg: &AppConfig) -> (FilterResult, Option<f64>) {
        // ── Cache lookup ─────────────────────────────────
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(mint) {
                if entry.inserted_at.elapsed() < CACHE_TTL {
                    debug!(mint, "rugcheck cache hit");
                    return self.evaluate(&entry.report, cfg);
                }
            }
        }

        // ── Fetch from API ───────────────────────────────
        match self.fetch(mint).await {
            Ok(report) => {
                let result = self.evaluate(&report, cfg);
                // Store in cache
                let mut cache = self.cache.lock().await;
                cache.insert(
                    mint.to_string(),
                    CacheEntry {
                        report,
                        inserted_at: Instant::now(),
                    },
                );
                // Prune if too large
                if cache.len() > 2_000 {
                    cache.retain(|_, v| v.inserted_at.elapsed() < CACHE_TTL);
                }
                result
            }
            Err(reason) => {
                if reason.starts_with("http_4") || reason.starts_with("http_5") || reason.contains("request_failed") || reason.contains("rate_limited") {
                    warn!(mint, reason = %reason, "rugcheck API error — passing through");
                    (FilterResult::pass(CHECK_NAME), None)
                } else {
                    warn!(mint, reason = %reason, "rugcheck fetch failed");
                    (FilterResult::fail(CHECK_NAME, &reason), None)
                }
            }
        }
    }

    // ── HTTP fetch with 429 retry ────────────────────────

    async fn fetch(&self, mint: &str) -> Result<RugCheckReport, String> {
        let url = format!("{}/{}/report", RUGCHECK_BASE_URL, mint);

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("request_failed: {}", e))?;

        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            warn!(mint, "rugcheck 429 — retrying in {}s", RATE_LIMIT_WAIT.as_secs());
            tokio::time::sleep(RATE_LIMIT_WAIT).await;

            let retry = self
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("retry_request_failed: {}", e))?;

            if retry.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err("rugcheck_rate_limited".to_string());
            }
            if !retry.status().is_success() {
                return Err(format!("http_{}", retry.status().as_u16()));
            }
            return retry
                .json::<RugCheckReport>()
                .await
                .map_err(|e| format!("retry_parse_failed: {}", e));
        }

        if !resp.status().is_success() {
            return Err(format!("http_{}", resp.status().as_u16()));
        }

        resp.json::<RugCheckReport>()
            .await
            .map_err(|e| format!("parse_failed: {}", e))
    }

    // ── Evaluate report against config ───────────────────

    fn evaluate(&self, report: &RugCheckReport, cfg: &AppConfig) -> (FilterResult, Option<f64>) {
        let filters = &cfg.strategy.filters;

        // mint authority
        if let Some(ref token_info) = report.token {
            if token_info.mint_authority.is_some() {
                return (
                    FilterResult::fail(CHECK_NAME, "mint_authority_not_revoked"),
                    report.score,
                );
            }
            if token_info.freeze_authority.is_some() {
                return (
                    FilterResult::fail(CHECK_NAME, "freeze_authority_not_revoked"),
                    report.score,
                );
            }
        }

        // liquidity locked
        if filters.require_liquidity_locked {
            if let Some(market) = report.markets.first() {
                if !market.liquidity_locked.unwrap_or(false) {
                    return (
                        FilterResult::fail(CHECK_NAME, "liquidity_not_locked"),
                        report.score,
                    );
                }
            }
        }

        // top-holder concentration
        let top_10_pct: f64 = report
            .top_holders
            .iter()
            .take(10)
            .filter_map(|h| h.pct)
            .sum();

        if top_10_pct > filters.max_top_holder_pct {
            return (
                FilterResult::fail(
                    CHECK_NAME,
                    &format!(
                        "top_holders_{:.1}pct_exceeds_max_{:.1}pct",
                        top_10_pct, filters.max_top_holder_pct
                    ),
                ),
                report.score,
            );
        }

        // overall score
        if let Some(score) = report.score {
            if score > filters.max_rugcheck_score as f64 {
                return (
                    FilterResult::fail(
                        CHECK_NAME,
                        &format!("score_{}_exceeds_max_{}", score, filters.max_rugcheck_score),
                    ),
                    Some(score),
                );
            }
        }

        // bundled launch
        if filters.reject_bundled && report.bundled.unwrap_or(false) {
            return (
                FilterResult::fail(CHECK_NAME, "token_is_bundled"),
                report.score,
            );
        }

        (FilterResult::pass(CHECK_NAME), report.score)
    }
}
