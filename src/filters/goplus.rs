use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use super::types::FilterResult;
use crate::config::AppConfig;

const CHECK_NAME: &str = "goplus";
const GOPLUS_BASE_URL: &str = "https://api.gopluslabs.io/api/v1/solana/token_security";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const CACHE_TTL: Duration = Duration::from_secs(300);

// ─── GoPlus API response types ───────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct GoPlusResponse {
    code: Option<i32>,
    result: Option<HashMap<String, GoPlusTokenResult>>,
}

#[derive(Debug, serde::Deserialize)]
struct GoPlusTokenResult {
    /// "1" = yes (bad), "0" = no (good), null = unknown
    is_open_source: Option<String>,
    is_mintable: Option<String>,
    #[serde(rename = "is_honeypot")]
    is_honeypot: Option<String>,
    is_proxy: Option<String>,
    #[serde(rename = "can_take_back_ownership")]
    can_take_back_ownership: Option<String>,
    #[serde(rename = "transfer_pausable")]
    transfer_pausable: Option<String>,
    #[serde(rename = "is_blacklisted")]
    is_blacklisted: Option<String>,
    creator_address: Option<String>,
    #[serde(rename = "holder_count")]
    holder_count: Option<String>,
}

// ─── Cache entry ─────────────────────────────────────────────

struct CacheEntry {
    result: GoPlusTokenResult,
    inserted_at: Instant,
}

// ─── Filter ──────────────────────────────────────────────────

pub struct GoPlusFilter {
    client: Client,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl GoPlusFilter {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("Failed to build GoPlus HTTP client");
        Self {
            client,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Run the GoPlus safety check.
    ///
    /// Returns `FilterResult`. On any API failure, passes through (graceful degradation).
    pub async fn check(&self, mint: &str, _cfg: &AppConfig) -> FilterResult {
        // ── Cache lookup ─────────────────────────────────
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(mint) {
                if entry.inserted_at.elapsed() < CACHE_TTL {
                    debug!(mint, "goplus cache hit");
                    return self.evaluate(mint, &entry.result);
                }
            }
        }

        // ── Fetch from API ───────────────────────────────
        match self.fetch(mint).await {
            Ok(result) => {
                let filter_result = self.evaluate(mint, &result);
                let mut cache = self.cache.lock().await;
                cache.insert(
                    mint.to_string(),
                    CacheEntry {
                        result,
                        inserted_at: Instant::now(),
                    },
                );
                if cache.len() > 2_000 {
                    cache.retain(|_, v| v.inserted_at.elapsed() < CACHE_TTL);
                }
                filter_result
            }
            Err(reason) => {
                warn!(mint, reason = %reason, "goplus API error — passing through");
                FilterResult::pass(CHECK_NAME)
            }
        }
    }

    async fn fetch(&self, mint: &str) -> Result<GoPlusTokenResult, String> {
        // GoPlus expects the mint address as a path parameter
        let url = format!("{}/{}", GOPLUS_BASE_URL, mint);

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("request_failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("http_{}", resp.status().as_u16()));
        }

        let body: GoPlusResponse = resp
            .json()
            .await
            .map_err(|e| format!("parse_failed: {}", e))?;

        if body.code != Some(1) {
            return Err(format!("api_error_code_{}", body.code.unwrap_or(-1)));
        }

        let result_map = body.result.ok_or("no_result_field")?;

        // GoPlus returns results keyed by the lowercase mint address
        let key = mint.to_lowercase();
        let token_result = result_map
            .into_iter()
            .find(|(k, _)| k.to_lowercase() == key || k == mint)
            .map(|(_, v)| v)
            .ok_or_else(|| format!("mint_not_in_response"))?;

        Ok(token_result)
    }

    fn evaluate(&self, mint: &str, result: &GoPlusTokenResult) -> FilterResult {
        // "1" = dangerous flag is SET

        if result.is_honeypot.as_deref() == Some("1") {
            warn!(mint, "🚫 goplus: token is a HONEYPOT");
            return FilterResult::fail(CHECK_NAME, "honeypot_detected");
        }

        if result.is_mintable.as_deref() == Some("1") {
            warn!(mint, "🚫 goplus: token is mintable (supply not fixed)");
            return FilterResult::fail(CHECK_NAME, "is_mintable");
        }

        if result.transfer_pausable.as_deref() == Some("1") {
            warn!(mint, "🚫 goplus: transfers can be paused");
            return FilterResult::fail(CHECK_NAME, "transfer_pausable");
        }

        if result.is_blacklisted.as_deref() == Some("1") {
            warn!(mint, "🚫 goplus: token has blacklist functionality");
            return FilterResult::fail(CHECK_NAME, "has_blacklist");
        }

        if result.can_take_back_ownership.as_deref() == Some("1") {
            warn!(mint, "🚫 goplus: ownership can be reclaimed");
            return FilterResult::fail(CHECK_NAME, "can_reclaim_ownership");
        }

        if result.is_proxy.as_deref() == Some("1") {
            warn!(mint, "🚫 goplus: token is a proxy contract (upgradeable)");
            return FilterResult::fail(CHECK_NAME, "proxy_contract");
        }

        debug!(mint, "✅ goplus: all safety checks passed");
        FilterResult::pass(CHECK_NAME)
    }
}
