//! Birdeye API client — token overview, security, creation info, meme detail.
//!
//! Starter plan ($49/mo): 10 req/sec, ~3M credits/mo.
//! All endpoints share the same rate limit.

use reqwest::Client;
use std::time::Duration;
use tracing::{debug, warn};

use super::types::{BirdeyeCreation, BirdeyeMemeDetail, BirdeyeOverview, BirdeyeSecurity};

const BASE_URL: &str = "https://public-api.birdeye.so";
const REQUEST_TIMEOUT_SECS: u64 = 3;

pub struct BirdeyeClient {
    client: Client,
    api_key: String,
}

impl BirdeyeClient {
    pub fn new(api_key: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .expect("Failed to build Birdeye HTTP client");
        Self {
            client,
            api_key: api_key.to_string(),
        }
    }

    fn headers(&self) -> Vec<(&str, &str)> {
        vec![("X-API-KEY", &self.api_key), ("x-chain", "solana")]
    }

    async fn get_json(&self, url: &str) -> Option<serde_json::Value> {
        let mut req = self.client.get(url);
        for (k, v) in self.headers() {
            req = req.header(k, v);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                warn!("Birdeye request failed for {}: {}", url, e);
                return None;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!("Birdeye HTTP {}: {}", status, &body[..body.len().min(200)]);
            return None;
        }

        match resp.json::<serde_json::Value>().await {
            Ok(v) => v.get("data").cloned().or(Some(v)),
            Err(e) => {
                warn!("Birdeye parse error: {}", e);
                None
            }
        }
    }

    // ── Enrichment endpoints (Phase 1) ──────────────────

    /// GET /defi/token_overview — market cap, volume, wallets, social links
    pub async fn token_overview(&self, mint: &str) -> Option<BirdeyeOverview> {
        let url = format!("{}/defi/token_overview?address={}", BASE_URL, mint);
        let json = self.get_json(&url).await?;
        match serde_json::from_value::<BirdeyeOverview>(json.clone()) {
            Ok(v) => {
                debug!(mint = %mint, "Birdeye overview fetched");
                Some(v)
            }
            Err(e) => {
                warn!(mint = %mint, "Birdeye overview parse error: {} — raw: {}", e, &json.to_string()[..json.to_string().len().min(200)]);
                None
            }
        }
    }

    /// GET /defi/token_security — owner/creator balance, top10%, token standard
    pub async fn token_security(&self, mint: &str) -> Option<BirdeyeSecurity> {
        let url = format!("{}/defi/token_security?address={}", BASE_URL, mint);
        let json = self.get_json(&url).await?;
        match serde_json::from_value::<BirdeyeSecurity>(json.clone()) {
            Ok(v) => {
                debug!(mint = %mint, "Birdeye security fetched");
                Some(v)
            }
            Err(e) => {
                warn!(mint = %mint, "Birdeye security parse error: {}", e);
                None
            }
        }
    }

    /// GET /defi/token_creation_info — creation tx, slot, block time
    pub async fn token_creation_info(&self, mint: &str) -> Option<BirdeyeCreation> {
        let url = format!("{}/defi/token_creation_info?address={}", BASE_URL, mint);
        let json = self.get_json(&url).await?;
        match serde_json::from_value::<BirdeyeCreation>(json.clone()) {
            Ok(v) => {
                debug!(mint = %mint, "Birdeye creation info fetched");
                Some(v)
            }
            Err(e) => {
                warn!(mint = %mint, "Birdeye creation parse error: {}", e);
                None
            }
        }
    }

    /// GET /defi/v3/token/meme/detail/single — meme-specific aggregated stats
    pub async fn meme_detail(&self, mint: &str) -> Option<BirdeyeMemeDetail> {
        let url = format!(
            "{}/defi/v3/token/meme/detail/single?address={}",
            BASE_URL, mint
        );
        let json = self.get_json(&url).await?;
        match serde_json::from_value::<BirdeyeMemeDetail>(json.clone()) {
            Ok(v) => {
                debug!(mint = %mint, "Birdeye meme detail fetched");
                Some(v)
            }
            Err(e) => {
                warn!(mint = %mint, "Birdeye meme detail parse error: {}", e);
                None
            }
        }
    }

    // ── Post-trade endpoints (Phase 2.5) ────────────────

    /// GET /defi/ohlcv — candle data for post-exit trajectory analysis
    pub async fn ohlcv(
        &self,
        mint: &str,
        candle_type: &str,
        time_from: i64,
        time_to: i64,
    ) -> Option<serde_json::Value> {
        let url = format!(
            "{}/defi/ohlcv?address={}&type={}&time_from={}&time_to={}",
            BASE_URL, mint, candle_type, time_from, time_to
        );
        self.get_json(&url).await
    }

    /// GET /defi/v3/token/trade-data/single — buy/sell volume, unique wallets
    pub async fn trade_data(&self, mint: &str) -> Option<serde_json::Value> {
        let url = format!(
            "{}/defi/v3/token/trade-data/single?address={}",
            BASE_URL, mint
        );
        self.get_json(&url).await
    }

    /// GET /defi/v3/token/top-traders — who profited most
    pub async fn top_traders(&self, mint: &str) -> Option<serde_json::Value> {
        let url = format!("{}/defi/v3/token/top-traders?address={}", BASE_URL, mint);
        self.get_json(&url).await
    }

    /// GET /defi/v3/token/holder — holder distribution
    pub async fn holder_distribution(&self, mint: &str) -> Option<serde_json::Value> {
        let url = format!("{}/defi/v3/token/holder?address={}", BASE_URL, mint);
        self.get_json(&url).await
    }

    /// GET /v1/wallet/token_pnl — wallet PnL for smart wallets
    pub async fn wallet_pnl(&self, wallet: &str) -> Option<serde_json::Value> {
        let url = format!("{}/v1/wallet/token_pnl?wallet={}", BASE_URL, wallet);
        self.get_json(&url).await
    }
}
