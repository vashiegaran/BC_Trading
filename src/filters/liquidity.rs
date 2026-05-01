use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::config::AppConfig;
use super::rpc_fallback::is_rate_limited;
use super::types::FilterResult;

const CHECK_NAME: &str = "liquidity";
const LOCK_CHECK_NAME: &str = "liquidity_lock_duration";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEXSCREENER_TOKEN_URL: &str = "https://api.dexscreener.com/latest/dex/tokens";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Seconds in one day.
const SECONDS_PER_DAY: u64 = 86_400;

// ─── DexScreener price response ─────────────────────────────

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

// ─── Filter ──────────────────────────────────────────────────

pub struct LiquidityFilter {
    http: Client,
    sol_price_cache: Arc<RwLock<f64>>,
}

impl LiquidityFilter {
    pub fn new() -> Self {
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("Failed to build liquidity HTTP client");

        let sol_price_cache = Arc::new(RwLock::new(0.0_f64));

        // Background task: refresh SOL price every 10 seconds
        let cache_clone = Arc::clone(&sol_price_cache);
        let http_clone = http.clone();
        tokio::spawn(async move {
            loop {
                let filter = LiquidityFilter {
                    http: http_clone.clone(),
                    sol_price_cache: Arc::new(RwLock::new(0.0)),
                };
                match filter.fetch_sol_price().await {
                    Ok(price) => {
                        if price < 50.0 || price > 10000.0 {
                            tracing::warn!(
                                price,
                                "SOL price looks wrong, skipping cache update"
                            );
                        } else {
                            let mut w = cache_clone.write().await;
                            *w = price;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("SOL price cache refresh failed: {}", e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });

        Self { http, sol_price_cache }
    }

    /// Check that the Raydium pool has enough liquidity in USD.
    ///
    /// Returns `(FilterResult, Option<liquidity_usd>)`.
    pub async fn check(
        &self,
        pool_address: Option<&Pubkey>,
        mint: &Pubkey,
        initial_liquidity_sol: f64,
        cfg: &AppConfig,
        rpc: &RpcClient,
        backup_rpc: &RpcClient,
    ) -> (FilterResult, Option<f64>) {
        // 1. Get SOL price in USD (from background cache)
        let sol_price_usd = {
            let cached = self.sol_price_cache.read().await;
            if *cached > 0.0 {
                *cached
            } else {
                // Cache not ready yet (first run), fetch directly
                drop(cached);
                match self.fetch_sol_price().await {
                    Ok(p) => {
                        let mut w = self.sol_price_cache.write().await;
                        *w = p;
                        p
                    }
                    Err(e) => {
                        warn!("liquidity: SOL price fetch failed: {}", e);
                        return (
                            FilterResult::fail(
                                CHECK_NAME,
                                &format!("sol_price_fetch_failed: {}", e),
                            ),
                            None,
                        );
                    }
                }
            }
        };

        // 2. If pool address is unknown, fall back to pool SOL estimate
        //    initial_liquidity_sol is the SOL side only — total pool liquidity
        //    is 2× (SOL side + token side of equal value)
        let pool_address = match pool_address {
            Some(p) => p,
            None => {
                let liquidity_usd = initial_liquidity_sol * sol_price_usd * 2.0;
                let min = cfg.strategy.filters.min_liquidity_usd as f64;
                if liquidity_usd < min {
                    warn!(
                        mint = %mint,
                        liquidity_usd,
                        min,
                        "liquidity: no pool address — estimated liquidity below min"
                    );
                    return (
                        FilterResult::fail(
                            CHECK_NAME,
                            &format!("no_pool_liquidity_${:.0}_below_min_${:.0}", liquidity_usd, min),
                        ),
                        Some(liquidity_usd),
                    );
                }
                let max = cfg.strategy.filters.max_liquidity_usd as f64;
                if max > 0.0 && liquidity_usd > max {
                    return (
                        FilterResult::fail(
                            CHECK_NAME,
                            &format!("no_pool_liquidity_${:.0}_above_max_${:.0}", liquidity_usd, max),
                        ),
                        Some(liquidity_usd),
                    );
                }
                return (FilterResult::pass(CHECK_NAME), Some(liquidity_usd));
            }
        };

        // Fetch pool account data from shared RPC client

        // ── Fallback for 429 rate-limit errors ───────────
        let pool_result = rpc.get_account_data(pool_address).await;
        let pool_result = if matches!(&pool_result, Err(e) if is_rate_limited(e)) {
            warn!(pool = %pool_address, "Primary RPC rate limited, using fallback RPC");
            backup_rpc.get_account_data(pool_address).await
        } else {
            pool_result
        };

        let account_data = match pool_result {
            Ok(data) => data,
            Err(e) => {
                warn!(pool = %pool_address, "liquidity: pool fetch failed: {}", e);
                return (
                    FilterResult::fail(CHECK_NAME, &format!("pool_fetch_failed: {}", e)),
                    None,
                );
            }
        };

        // 3. Parse SOL reserves from Raydium AMM v4 layout
        let sol_lamports = match parse_sol_reserve(&account_data) {
            Ok(l) => l,
            Err(e) => {
                // pump.fun tokens not yet on Raydium — pool data too short (still on bonding curve)
                if e.contains("pool_data_too_short") {
                    // Token on pump-AMM — use initial_liquidity_sol from detection
                    // Multiply by 2 for double-sided pool value (SOL + token side)
                    let liquidity_usd = initial_liquidity_sol * sol_price_usd * 2.0;
                    let min = cfg.strategy.filters.min_liquidity_usd as f64;
                    if liquidity_usd < min {
                        warn!(
                            mint = %mint,
                            liquidity_usd,
                            min,
                            "liquidity: pump-AMM pool below min liquidity"
                        );
                        return (
                            FilterResult::fail(
                                CHECK_NAME,
                                &format!("pump_amm_liquidity_${:.0}_below_min_${:.0}", liquidity_usd, min),
                            ),
                            Some(liquidity_usd),
                        );
                    }
                    let max = cfg.strategy.filters.max_liquidity_usd as f64;
                    if max > 0.0 && liquidity_usd > max {
                        return (
                            FilterResult::fail(
                                CHECK_NAME,
                                &format!("pump_amm_liquidity_${:.0}_above_max_${:.0}", liquidity_usd, max),
                            ),
                            Some(liquidity_usd),
                        );
                    }
                    return (FilterResult::pass(CHECK_NAME), Some(liquidity_usd));
                }
                warn!(pool = %pool_address, "liquidity: reserve parse failed: {}", e);
                return (
                    FilterResult::fail(CHECK_NAME, &format!("reserve_parse_failed: {}", e)),
                    None,
                );
            }
        };

        let sol_reserve = sol_lamports as f64 / 1_000_000_000.0;
        // Liquidity ≈ 2 × SOL-side value (AMM pools hold ~equal value each side)
        let liquidity_usd = sol_reserve * sol_price_usd * 2.0;

        debug!(
            pool = %pool_address,
            sol_reserve,
            liquidity_usd,
            "liquidity calculated"
        );

        let min = cfg.strategy.filters.min_liquidity_usd as f64;
        if liquidity_usd < min {
            return (
                FilterResult::fail(
                    CHECK_NAME,
                    &format!("liquidity_${:.0}_below_min_${:.0}", liquidity_usd, min),
                ),
                Some(liquidity_usd),
            );
        }

        let max = cfg.strategy.filters.max_liquidity_usd as f64;
        if max > 0.0 && liquidity_usd > max {
            return (
                FilterResult::fail(
                    CHECK_NAME,
                    &format!("liquidity_${:.0}_above_max_${:.0}", liquidity_usd, max),
                ),
                Some(liquidity_usd),
            );
        }

        // 4. Check liquidity lock duration if locking is required
        if cfg.strategy.filters.require_liquidity_locked {
            let min_days = cfg.strategy.filters.min_lock_duration_days;
            match self.check_lock_duration(pool_address, rpc, backup_rpc, min_days).await {
                Ok(()) => {
                    debug!(pool = %pool_address, "liquidity lock duration OK");
                }
                Err(reason) => {
                    return (
                        FilterResult::fail(LOCK_CHECK_NAME, &reason),
                        Some(liquidity_usd),
                    );
                }
            }
        }

        (FilterResult::pass(CHECK_NAME), Some(liquidity_usd))
    }

    // ── Check liquidity lock duration ────────────────────

    /// Verify the liquidity lock has an unlock timestamp at least
    /// `min_days` in the future.
    ///
    /// Reads the lock account data from the Raydium pool and extracts
    /// the unlock timestamp (a little-endian u64 at a known offset).
    async fn check_lock_duration(
        &self,
        pool_address: &Pubkey,
        rpc: &RpcClient,
        backup_rpc: &RpcClient,
        min_days: u64,
    ) -> Result<(), String> {
        // Fetch pool account data to extract the lock account reference.
        // In Raydium AMM v4 pools, the LP token lock timestamp is
        // stored in the pool state. Offset 432 holds the lock-until
        // timestamp as a little-endian i64 (unix seconds).
        // ── Fallback for 429 rate-limit errors ───────────
        let lock_result = rpc.get_account_data(pool_address).await;
        let lock_result = if matches!(&lock_result, Err(e) if is_rate_limited(e)) {
            warn!("Primary RPC rate limited, using fallback RPC");
            backup_rpc.get_account_data(pool_address).await
        } else {
            lock_result
        };

        let account_data = lock_result
            .map_err(|e| format!("lock_account_fetch_failed: {}", e))?;

        let unlock_timestamp = parse_lock_timestamp(&account_data)?;

        let now = chrono::Utc::now().timestamp();
        let lock_duration_seconds = unlock_timestamp.saturating_sub(now);

        if lock_duration_seconds <= 0 {
            return Err("liquidity_lock_already_expired".to_string());
        }

        let lock_duration_days = lock_duration_seconds as u64 / SECONDS_PER_DAY;

        debug!(
            unlock_timestamp,
            lock_duration_days,
            min_days,
            "liquidity lock duration check"
        );

        if lock_duration_days < min_days {
            return Err(format!(
                "liquidity_lock_too_short: {}d < min_{}d",
                lock_duration_days, min_days
            ));
        }

        Ok(())
    }

    // ── Fetch SOL price (multi-source) ────────────────────

    async fn fetch_sol_price(&self) -> Result<f64, String> {
        // Primary: Jupiter price API (most accurate for SOL)
        let jupiter_url = "https://price.jup.ag/v4/price?ids=SOL";

        if let Ok(resp) = self
            .http
            .get(jupiter_url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(price) = json
                    .get("data")
                    .and_then(|d| d.get("SOL"))
                    .and_then(|s| s.get("price"))
                    .and_then(|p| p.as_f64())
                {
                    if price > 50.0 && price < 10000.0 {
                        return Ok(price);
                    }
                }
            }
        }

        // Fallback: Coinbase public API
        let coinbase_url = "https://api.coinbase.com/v2/prices/SOL-USD/spot";

        if let Ok(resp) = self
            .http
            .get(coinbase_url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(price) = json
                    .get("data")
                    .and_then(|d| d.get("amount"))
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    if price > 50.0 && price < 10000.0 {
                        return Ok(price);
                    }
                }
            }
        }

        // Last fallback: DexScreener (original method)
        let url = format!("{}/{}", DEXSCREENER_TOKEN_URL, SOL_MINT);

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

        let pairs = body.pairs.ok_or_else(|| "no_pairs_in_response".to_string())?;
        let pair = pairs
            .iter()
            .find(|p| p.chain_id == "solana")
            .or_else(|| pairs.first())
            .ok_or_else(|| "sol_price_not_in_response".to_string())?;

        pair.price_usd
            .as_ref()
            .ok_or_else(|| "price_usd_missing".to_string())?
            .parse::<f64>()
            .map_err(|e| format!("price_parse_failed: {}", e))
    }
}

// ─── Raydium AMM v4 pool layout helper ──────────────────────

/// Extract the quote-side (SOL) reserve in lamports from raw Raydium
/// AMM v4 pool account data.
///
/// Raydium AMM v4 standard pool accounts are at least 752 bytes.
/// The quote vault balance (SOL) is a little-endian u64 at offset 136.
fn parse_sol_reserve(data: &[u8]) -> Result<u64, String> {
    if data.len() < 752 {
        return Err(format!(
            "pool_data_too_short_{}_bytes_expected_752",
            data.len()
        ));
    }

    let offset = 136;
    let bytes: [u8; 8] = data[offset..offset + 8]
        .try_into()
        .map_err(|_| "failed_to_read_sol_reserve_bytes".to_string())?;

    let lamports = u64::from_le_bytes(bytes);
    if lamports == 0 {
        return Err("zero_sol_reserve".to_string());
    }

    Ok(lamports)
}

/// Extract the liquidity lock-until timestamp from Raydium AMM v4
/// pool account data.
///
/// The lock-until timestamp is stored as a little-endian i64 (unix
/// seconds) at offset 432 in the pool state data.  Returns 0 if the
/// field is not present (no lock).
fn parse_lock_timestamp(data: &[u8]) -> Result<i64, String> {
    // If the pool data is shorter than the expected offset, the lock
    // field is not present — treat as no lock.
    let offset = 432;
    if data.len() < offset + 8 {
        return Err("pool_data_too_short_for_lock_timestamp".to_string());
    }

    let bytes: [u8; 8] = data[offset..offset + 8]
        .try_into()
        .map_err(|_| "failed_to_read_lock_timestamp_bytes".to_string())?;

    let timestamp = i64::from_le_bytes(bytes);
    Ok(timestamp)
}
