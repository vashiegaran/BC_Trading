use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, warn};

use super::types::FilterResult;
use crate::config::AppConfig;

const CHECK_NAME: &str = "helius_das";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

// ─── DAS response types ─────────────────────────────────────

#[derive(Debug, Deserialize)]
struct DasResponse {
    result: Option<DasAsset>,
}

#[derive(Debug, Deserialize)]
struct DasAsset {
    /// Token-level info (mint/freeze authority, supply, decimals).
    #[serde(default)]
    token_info: Option<DasTokenInfo>,
    /// Whether the metadata is mutable (can be changed post-launch).
    #[serde(default)]
    mutable: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DasTokenInfo {
    mint_authority: Option<String>,
    freeze_authority: Option<String>,
}

// ─── Filter ──────────────────────────────────────────────────

pub struct HeliusDasFilter {
    client: Client,
}

impl HeliusDasFilter {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("Failed to build Helius DAS HTTP client");
        Self { client }
    }

    /// Run Helius DAS getAsset check.
    ///
    /// Checks mint authority and freeze authority via Helius's DAS API.
    /// Returns pass if Helius is not configured (graceful degradation).
    pub async fn check(&self, mint: &str, cfg: &AppConfig) -> FilterResult {
        let rpc_url = match &cfg.env.helius_rpc_url {
            Some(url) => url,
            None => {
                debug!("helius_das: no Helius RPC URL configured — skipping");
                return FilterResult::pass(CHECK_NAME);
            }
        };

        match self.fetch_asset(rpc_url, mint).await {
            Ok(asset) => self.evaluate(asset),
            Err(reason) => {
                // Helius DAS failure should NOT block the token — RugCheck
                // provides the same checks as a primary.  Log and pass.
                warn!(mint, reason = %reason, "helius_das fetch failed — passing through");
                FilterResult::pass(CHECK_NAME)
            }
        }
    }

    async fn fetch_asset(&self, rpc_url: &str, mint: &str) -> Result<DasAsset, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAsset",
            "params": {
                "id": mint
            }
        });

        let resp = self
            .client
            .post(rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("request_failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("http_{}", resp.status().as_u16()));
        }

        let das: DasResponse = resp
            .json()
            .await
            .map_err(|e| format!("parse_failed: {}", e))?;

        das.result.ok_or_else(|| "empty_result".to_string())
    }

    fn evaluate(&self, asset: DasAsset) -> FilterResult {
        if let Some(token_info) = &asset.token_info {
            // Mint authority: if present and non-empty, someone can mint new tokens
            if let Some(ref authority) = token_info.mint_authority {
                if !authority.is_empty() {
                    return FilterResult::fail(CHECK_NAME, "mint_authority_not_revoked");
                }
            }

            // Freeze authority: if present and non-empty, someone can freeze your account
            if let Some(ref authority) = token_info.freeze_authority {
                if !authority.is_empty() {
                    return FilterResult::fail(CHECK_NAME, "freeze_authority_not_revoked");
                }
            }
        }

        debug!("helius_das: all checks passed");
        FilterResult::pass(CHECK_NAME)
    }
}
