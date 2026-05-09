use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, warn};

use super::rpc_fallback::is_rate_limited;
use super::types::FilterResult;
use crate::config::AppConfig;

const CHECK_NAME: &str = "market_cap";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEXSCREENER_TOKEN_URL: &str = "https://api.dexscreener.com/latest/dex/tokens";

#[derive(Debug, Deserialize)]
struct DexScreenerResponse {
    pairs: Option<Vec<DexScreenerPair>>,
}

#[derive(Debug, Deserialize)]
struct DexScreenerPair {
    #[serde(rename = "chainId")]
    chain_id: String,
    #[serde(rename = "priceUsd")]
    price_usd: Option<String>,
}

pub struct MarketCapFilter {
    http: Client,
}

impl MarketCapFilter {
    pub fn new() -> Self {
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("Failed to build market_cap HTTP client");
        Self { http }
    }

    /// Calculate market cap = price × total supply.
    ///
    /// Returns `(FilterResult, Option<market_cap_usd>, Option<token_price_usd>)`.
    pub async fn check(
        &self,
        mint: &Pubkey,
        cfg: &AppConfig,
        prefetched_supply: Option<f64>,
    ) -> (FilterResult, Option<f64>, Option<f64>) {
        // 1. Fetch token price from Jupiter
        let mint_str = mint.to_string();
        let token_price = match self.fetch_price(&mint_str).await {
            Ok(p) => p,
            Err(e) => {
                if e.contains("no_pairs_in_response") {
                    warn!(
                        mint = %mint,
                        "price fetch failed: no pairs on DexScreener yet, skipping price filter"
                    );
                    return (FilterResult::pass(CHECK_NAME), None, None);
                }
                warn!(mint = %mint, "market_cap: price fetch failed: {}", e);
                return (
                    FilterResult::fail(CHECK_NAME, &format!("price_fetch_failed: {}", e)),
                    None,
                    None,
                );
            }
        };

        // 2. Resolve total supply (use pre-fetched value when available)
        let total_supply_ui = if let Some(supply) = prefetched_supply {
            supply
        } else {
            let rpc = RpcClient::new_with_timeout(cfg.env.solana_rpc_url.clone(), REQUEST_TIMEOUT);
            let supply_result = rpc.get_token_supply(mint).await;
            let supply_result = if matches!(&supply_result, Err(e) if is_rate_limited(e)) {
                warn!(mint = %mint, "QuickNode rate limited, using fallback RPC");
                let fallback = RpcClient::new_with_timeout(
                    cfg.env.solana_rpc_backup_url.clone(),
                    REQUEST_TIMEOUT,
                );
                fallback.get_token_supply(mint).await
            } else {
                supply_result
            };

            let supply = match supply_result {
                Ok(s) => s,
                Err(e) => {
                    warn!(mint = %mint, "market_cap: supply fetch failed: {}", e);
                    return (
                        FilterResult::fail(CHECK_NAME, &format!("supply_fetch_failed: {}", e)),
                        None,
                        Some(token_price),
                    );
                }
            };

            supply.ui_amount.unwrap_or_else(|| {
                let raw: f64 = supply.amount.parse().unwrap_or(0.0);
                raw / 10_f64.powi(supply.decimals as i32)
            })
        };

        // 3. Calculate
        let market_cap_usd = token_price * total_supply_ui;

        debug!(
            mint = %mint,
            token_price,
            total_supply_ui,
            market_cap_usd,
            "market_cap calculated"
        );

        let max_mcap = cfg.strategy.filters.max_market_cap_usd as f64;
        if market_cap_usd > max_mcap {
            return (
                FilterResult::fail(
                    CHECK_NAME,
                    &format!("mcap_${:.0}_exceeds_max_${:.0}", market_cap_usd, max_mcap),
                ),
                Some(market_cap_usd),
                Some(token_price),
            );
        }

        // Floor: reject tokens with absurdly low market cap (broken price data)
        let min_mcap = cfg.strategy.filters.min_market_cap_usd as f64;
        if min_mcap > 0.0 && market_cap_usd < min_mcap {
            return (
                FilterResult::fail(
                    CHECK_NAME,
                    &format!("mcap_${:.0}_below_min_${:.0}", market_cap_usd, min_mcap),
                ),
                Some(market_cap_usd),
                Some(token_price),
            );
        }

        (
            FilterResult::pass(CHECK_NAME),
            Some(market_cap_usd),
            Some(token_price),
        )
    }

    async fn fetch_price(&self, mint: &str) -> Result<f64, String> {
        let url = format!("{}/{}", DEXSCREENER_TOKEN_URL, mint);

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("request_failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("http_{}", resp.status().as_u16()));
        }

        let body: DexScreenerResponse = resp
            .json()
            .await
            .map_err(|e| format!("parse_failed: {}", e))?;

        let pairs = body
            .pairs
            .ok_or_else(|| "no_pairs_in_response".to_string())?;
        let pair = pairs
            .iter()
            .find(|p| p.chain_id == "solana")
            .or_else(|| pairs.first())
            .ok_or_else(|| "token_not_found".to_string())?;

        pair.price_usd
            .as_ref()
            .ok_or_else(|| "price_usd_missing".to_string())?
            .parse::<f64>()
            .map_err(|e| format!("price_parse_failed: {}", e))
    }
}
