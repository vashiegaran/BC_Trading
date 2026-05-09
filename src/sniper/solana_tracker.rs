//! Solana Tracker API client — Advanced tier (EU region).
//!
//! Endpoints used:
//!   - GET /tokens/{mint}           — full token data + risk + volume
//!   - GET /tokens/deployer/{addr}  — all tokens by a deployer (serial creator detection)
//!   - GET /search                  — server-side filtered token discovery
//!   - GET /trades/{mint}           — recent trades for whale/sell-pressure detection
//!
//! Advanced plan: 200k req/month, no rate limit.

use reqwest::Client;
use std::time::Duration;
use tracing::{debug, warn};

use super::types::{SolanaTrackerData, SolanaTrackerTrade};

/// EU region endpoint (shared Advanced plan).
const BASE_URL: &str = "https://eu.data.solanatracker.io";
const REQUEST_TIMEOUT_SECS: u64 = 3;

pub struct SolanaTrackerClient {
    client: Client,
    api_key: Option<String>,
}

impl SolanaTrackerClient {
    pub fn new(api_key: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .expect("Failed to build SolanaTracker HTTP client");
        Self { client, api_key }
    }

    /// Build a GET request with API key header.
    fn get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.get(url);
        if let Some(ref key) = self.api_key {
            req = req.header("x-api-key", key);
        }
        req
    }

    /// Fetch comprehensive token data from Solana Tracker.
    /// Parses risk, volume, momentum, fees, and holder data.
    pub async fn fetch_token(&self, mint: &str) -> Option<SolanaTrackerData> {
        let url = format!("{}/tokens/{}", BASE_URL, mint);

        let resp = match self.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(mint = %mint, "SolanaTracker request failed: {}", e);
                return None;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(mint = %mint, "SolanaTracker HTTP {}: {}", status, &body[..body.len().min(200)]);
            return None;
        }

        let json: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(mint = %mint, "SolanaTracker parse failed: {}", e);
                return None;
            }
        };

        let mut data = SolanaTrackerData::default();

        // ── Risk data ──
        let risk = json.get("risk").or_else(|| json.get("risks"));
        if let Some(r) = risk {
            data.risk_score = r.get("score").and_then(|v| v.as_f64());
            data.bundlers_pct = r.get("bundlers").and_then(|b| {
                b.get("totalPercentage")
                    .or_else(|| b.get("percentage"))
                    .and_then(|v| v.as_f64())
            });
            data.bundler_count = r
                .get("bundlers")
                .and_then(|b| b.get("count").and_then(|v| v.as_u64()));
            data.snipers_pct = r.get("snipers").and_then(|s| {
                s.get("totalPercentage")
                    .or_else(|| s.get("percentage"))
                    .and_then(|v| v.as_f64())
            });
            data.sniper_count = r
                .get("snipers")
                .and_then(|s| s.get("count").and_then(|v| v.as_u64()));
            data.insiders_pct = r.get("insiders").and_then(|i| {
                i.get("totalPercentage")
                    .or_else(|| i.get("percentage"))
                    .and_then(|v| v.as_f64())
            });
            data.insider_count = r
                .get("insiders")
                .and_then(|i| i.get("count").and_then(|v| v.as_u64()));
            data.dev_pct = r
                .get("dev")
                .and_then(|d| d.get("percentage").and_then(|v| v.as_f64()));
            data.top10_pct = r.get("top10").and_then(|v| v.as_f64());
            data.rugged = r.get("rugged").and_then(|v| v.as_bool());
            data.jupiter_verified = r.get("jupiterVerified").and_then(|v| v.as_bool());

            // Fees (Jito tips = smart money signal)
            if let Some(fees) = r.get("fees") {
                data.fees_total_sol = fees.get("total").and_then(|v| v.as_f64());
                data.fees_total_tips = fees.get("totalTips").and_then(|v| v.as_f64());
                data.fees_total_trading = fees.get("totalTrading").and_then(|v| v.as_f64());
            }
        }

        // ── Top-level fields ──
        data.holders = json.get("holders").and_then(|v| v.as_u64());
        data.total_buys = json
            .get("buys")
            .or_else(|| json.get("totalBuys"))
            .and_then(|v| v.as_u64());
        data.total_sells = json
            .get("sells")
            .or_else(|| json.get("totalSells"))
            .and_then(|v| v.as_u64());
        data.total_txns = json
            .get("txns")
            .or_else(|| json.get("totalTxns"))
            .and_then(|v| v.as_u64());

        // ── Pool data (first pool) ──
        if let Some(pools) = json.get("pools").and_then(|v| v.as_array()) {
            if let Some(pool) = pools.first() {
                data.lp_burn_pct = pool.get("lpBurn").and_then(|v| v.as_f64());
                // Security from pool
                if let Some(sec) = pool.get("security") {
                    data.has_freeze_authority = sec.get("freezeAuthority").map(|v| !v.is_null());
                    data.has_mint_authority = sec.get("mintAuthority").map(|v| !v.is_null());
                }
                // Pool-level volume
                if let Some(txns) = pool.get("txns") {
                    // Prefer pool-level buys/sells if top-level missing
                    if data.total_buys.is_none() {
                        data.total_buys = txns.get("buys").and_then(|v| v.as_u64());
                    }
                    if data.total_sells.is_none() {
                        data.total_sells = txns.get("sells").and_then(|v| v.as_u64());
                    }
                }
                // Deployer
                data.deployer = pool
                    .get("deployer")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                // Market (pumpfun, raydium, etc.)
                data.market = pool
                    .get("market")
                    .and_then(|v| v.as_str())
                    .map(String::from);
            }
        }

        // ── Volume by timeframe (top-level on search results, nested in full response) ──
        data.volume_5m = json.get("volume_5m").and_then(|v| v.as_f64());
        data.volume_15m = json.get("volume_15m").and_then(|v| v.as_f64());
        data.volume_1h = json.get("volume_1h").and_then(|v| v.as_f64());
        data.volume_24h = json.get("volume_24h").and_then(|v| v.as_f64());

        // ── Price change events ──
        if let Some(events) = json.get("events") {
            data.price_change_5m = events
                .get("5m")
                .and_then(|e| e.get("priceChangePercentage").and_then(|v| v.as_f64()));
            data.price_change_1h = events
                .get("1h")
                .and_then(|e| e.get("priceChangePercentage").and_then(|v| v.as_f64()));
        }

        debug!(
            mint = %mint,
            risk_score = ?data.risk_score,
            bundlers_pct = ?data.bundlers_pct,
            holders = ?data.holders,
            volume_5m = ?data.volume_5m,
            "SolanaTracker data fetched"
        );

        Some(data)
    }

    /// Fetch all tokens created by a deployer wallet.
    /// Returns the count of tokens and the deployer's track record.
    pub async fn fetch_deployer_tokens(&self, deployer: &str) -> Option<Vec<DeployerToken>> {
        let url = format!("{}/tokens/deployer/{}", BASE_URL, deployer);

        let resp = match self.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(deployer = %deployer, "SolanaTracker deployer request failed: {}", e);
                return None;
            }
        };

        if !resp.status().is_success() {
            return None;
        }

        let json: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(deployer = %deployer, "SolanaTracker deployer parse failed: {}", e);
                return None;
            }
        };

        let tokens = json.as_array()?;
        let result: Vec<DeployerToken> = tokens
            .iter()
            .filter_map(|t| {
                let token = t.get("token")?;
                Some(DeployerToken {
                    mint: token.get("mint")?.as_str()?.to_string(),
                    name: token
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    created_time: token
                        .get("creation")
                        .and_then(|c| c.get("created_time").and_then(|v| v.as_i64())),
                    rugged: t
                        .get("risk")
                        .and_then(|r| r.get("rugged").and_then(|v| v.as_bool()))
                        .unwrap_or(false),
                })
            })
            .collect();

        debug!(deployer = %deployer, token_count = result.len(), "Deployer tokens fetched");
        Some(result)
    }

    /// Search for recently graduated tokens with server-side filtering.
    /// Returns tokens that match our criteria — useful as a backup detection source.
    pub async fn search_graduated(
        &self,
        min_liquidity_usd: f64,
        max_top10: f64,
        max_bundler_pct: f64,
    ) -> Option<Vec<SearchResult>> {
        let url = format!(
            "{}/search?market=pumpfun&status=graduated&minLiquidity={}&maxTop10={}&maxBundlerPercentage={}&mintAuthority=null&freezeAuthority=null&sortBy=createdAt&sortOrder=desc&limit=50",
            BASE_URL, min_liquidity_usd, max_top10, max_bundler_pct
        );

        let resp = match self.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!("SolanaTracker search failed: {}", e);
                return None;
            }
        };

        if !resp.status().is_success() {
            return None;
        }

        let json: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!("SolanaTracker search parse failed: {}", e);
                return None;
            }
        };

        let data = json.get("data").and_then(|v| v.as_array())?;
        let results: Vec<SearchResult> = data
            .iter()
            .filter_map(|item| {
                Some(SearchResult {
                    mint: item.get("mint")?.as_str()?.to_string(),
                    name: item
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    symbol: item
                        .get("symbol")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    pool_address: item
                        .get("poolAddress")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    liquidity_usd: item
                        .get("liquidityUsd")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    market_cap_usd: item
                        .get("marketCapUsd")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    holders: item.get("holders").and_then(|v| v.as_u64()).unwrap_or(0),
                    top10_pct: item.get("top10").and_then(|v| v.as_f64()).unwrap_or(0.0),
                    volume_5m: item
                        .get("volume_5m")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    buys: item.get("buys").and_then(|v| v.as_u64()).unwrap_or(0),
                    sells: item.get("sells").and_then(|v| v.as_u64()).unwrap_or(0),
                    deployer: item
                        .get("deployer")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    created_at_ms: item.get("createdAt").and_then(|v| v.as_i64()).unwrap_or(0),
                    risk_score: item
                        .get("riskScore")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    has_socials: item
                        .get("hasSocials")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                })
            })
            .collect();

        debug!(
            count = results.len(),
            "SolanaTracker search returned tokens"
        );
        Some(results)
    }

    /// Fetch recent trades for a token — used for whale detection and sell pressure monitoring.
    pub async fn fetch_trades(&self, mint: &str) -> Option<Vec<SolanaTrackerTrade>> {
        let url = format!("{}/trades/{}?hideArb=true", BASE_URL, mint);

        let resp = match self.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(mint = %mint, "SolanaTracker trades request failed: {}", e);
                return None;
            }
        };

        if !resp.status().is_success() {
            return None;
        }

        let json: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(mint = %mint, "SolanaTracker trades parse failed: {}", e);
                return None;
            }
        };

        let trades_arr = json.get("trades").and_then(|v| v.as_array())?;
        let trades: Vec<SolanaTrackerTrade> = trades_arr
            .iter()
            .filter_map(|t| {
                Some(SolanaTrackerTrade {
                    tx: t
                        .get("tx")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    trade_type: t
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    volume_usd: t.get("volume").and_then(|v| v.as_f64()).unwrap_or(0.0),
                    volume_sol: t.get("volumeSol").and_then(|v| v.as_f64()).unwrap_or(0.0),
                    wallet: t
                        .get("wallet")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    time_ms: t.get("time").and_then(|v| v.as_i64()).unwrap_or(0),
                })
            })
            .collect();

        debug!(mint = %mint, count = trades.len(), "SolanaTracker trades fetched");
        Some(trades)
    }
}

/// Token info from the deployer endpoint.
#[derive(Debug, Clone)]
pub struct DeployerToken {
    pub mint: String,
    pub name: String,
    pub created_time: Option<i64>,
    pub rugged: bool,
}

/// Token from the /search endpoint.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub pool_address: Option<String>,
    pub liquidity_usd: f64,
    pub market_cap_usd: f64,
    pub holders: u64,
    pub top10_pct: f64,
    pub volume_5m: f64,
    pub buys: u64,
    pub sells: u64,
    pub deployer: Option<String>,
    pub created_at_ms: i64,
    pub risk_score: f64,
    pub has_socials: bool,
}
