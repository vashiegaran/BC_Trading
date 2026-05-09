use anyhow::{Context, Result};
use reqwest::Client;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::execution::jupiter::{JupiterClient, SOL_MINT};
use crate::monitoring::helius_price_ws::HeliusPriceCache;

/// Maximum age of a Helius-WS-derived price before we fall back to Jupiter.
/// 3s tolerates one missed slot without exposing exits to wildly stale data.
const HELIUS_CACHE_MAX_AGE: Duration = Duration::from_secs(3);

const BIRDEYE_PRICE_URL: &str = "https://public-api.birdeye.so/defi/price";

/// Maximum consecutive price fetch failures before returning 0.0
/// instead of a stale cached price. At 500ms polling interval,
/// 6 failures = 3 seconds of dead price data → treat token as dead.
const MAX_CONSECUTIVE_FAILURES: u32 = 6;

/// Maximum age of a last_known price entry before it's considered stale
/// and evicted. 30 minutes — well beyond any active monitoring session.
const LAST_KNOWN_MAX_AGE: Duration = Duration::from_secs(1800);

/// Evict stale entries from last_known when the map exceeds this size.
const LAST_KNOWN_EVICT_THRESHOLD: usize = 200;

/// Price fetcher that uses Birdeye API (if configured) with Jupiter as
/// fallback. Caches the last known price per mint so that transient
/// errors don't cause monitoring gaps.
pub struct PriceFetcher {
    birdeye_client: Option<Client>,
    birdeye_api_key: Option<String>,
    jupiter: JupiterClient,
    /// Last known price per mint — used when both APIs fail.
    /// Stores (price, timestamp) so stale entries can be detected and evicted.
    last_known: Mutex<std::collections::HashMap<String, (f64, Instant)>>,
    /// Consecutive failure count per mint — when this exceeds
    /// MAX_CONSECUTIVE_FAILURES, we return 0.0 instead of stale cache.
    failure_count: Mutex<std::collections::HashMap<String, u32>>,
    max_sane_price: f64,
    max_price_change_ratio: f64,
    /// Optional Helius WS-fed price cache. When fresh, we serve from here
    /// and skip the Jupiter HTTP call entirely (massive 429 reduction).
    helius_cache: Option<Arc<HeliusPriceCache>>,
}

impl PriceFetcher {
    pub fn new(
        birdeye_api_key: Option<String>,
        price_timeout_secs: u64,
        api_request_timeout_secs: u64,
        max_retries: u32,
        max_sane_price: f64,
        max_price_change_ratio: f64,
    ) -> Self {
        let birdeye_client = birdeye_api_key.as_ref().map(|_| {
            Client::builder()
                .timeout(Duration::from_secs(price_timeout_secs))
                .build()
                .expect("Failed to build Birdeye HTTP client")
        });

        Self {
            birdeye_client,
            birdeye_api_key,
            jupiter: JupiterClient::new(api_request_timeout_secs, max_retries),
            last_known: Mutex::new(std::collections::HashMap::new()), // (price, timestamp)
            failure_count: Mutex::new(std::collections::HashMap::new()),
            max_sane_price,
            max_price_change_ratio,
            helius_cache: None,
        }
    }

    /// Attach a shared Helius-WS price cache. Should be called once at
    /// monitoring start; cloning an `Arc` keeps writers and the fetcher
    /// pointed at the same map.
    pub fn with_helius_cache(mut self, cache: Arc<HeliusPriceCache>) -> Self {
        self.helius_cache = Some(cache);
        self
    }

    /// Cache-only price for the monitoring loop. Returns Yellowstone/Helius WS
    /// cached price if available (any age), otherwise last_known price.
    /// **Never calls Jupiter HTTP** — eliminates 429s from price polling.
    ///
    /// Use `get_price()` only when an accurate fresh price is needed (exits,
    /// SOL/USD refresh).
    pub fn get_monitoring_price(&self, mint: &str) -> f64 {
        // Try the WS cache first (no freshness limit — any cached price is fine
        // for monitoring triggers; Yellowstone updates every ~400ms anyway).
        if let Some(cache) = &self.helius_cache {
            if let Some(p) = cache.get(mint) {
                self.cache_price(mint, p);
                self.reset_failure_count(mint);
                return p;
            }
        }
        // Fall back to last-known price (set by previous get_price() or cache hit).
        self.get_last_known(mint)
    }

    /// Return a clone of the attached cache (used by monitoring loop to
    /// seed SOL/USD and write into the same map from WS subscribers).
    pub fn helius_cache(&self) -> Option<Arc<HeliusPriceCache>> {
        self.helius_cache.clone()
    }

    /// Fetch the current USD price for a token mint.
    ///
    /// Strategy:
    /// 1. Derive price from Jupiter swap quote (fastest — uses Helius RPC, works
    ///    immediately for new tokens without waiting for indexing).
    /// 2. If quote fails → try DexScreener via Jupiter price API.
    /// 3. If Birdeye API key is set → try Birdeye as final fallback.
    /// 4. On total failure → return last known price and log a warning.
    ///
    /// All returned prices are validated through `validate_price` to
    /// guard against corrupt / absurd data.
    pub async fn get_price(&self, mint: &str) -> f64 {
        // Fast path: Helius WS cache. Skip Jupiter entirely when we have a
        // bonding-curve-derived price <3s old. Doesn't apply to SOL_MINT
        // because SOL/USD is itself sourced from Jupiter and feeds the cache.
        if mint != SOL_MINT {
            if let Some(cache) = &self.helius_cache {
                if let Some(p) = cache.get_fresh(mint, HELIUS_CACHE_MAX_AGE) {
                    self.cache_price(mint, p);
                    self.reset_failure_count(mint);
                    return p;
                }
            }
        }

        let last_known = self.get_last_known(mint);

        // Primary: derive price from Jupiter quote (Helius RPC — zero extra latency)
        // This works for brand-new tokens that DexScreener/Birdeye haven't indexed yet.
        if let Some(price) = self.derive_price_from_quote(mint).await {
            let validated = self.validate_price(price, last_known, mint);
            // API succeeded — reset failure count regardless of validation
            self.reset_failure_count(mint);
            // Only cache if validation accepted the NEW price (not stale fallback)
            if validated > 0.0 && (validated - price).abs() < f64::EPSILON {
                self.cache_price(mint, validated);
            }
            if validated > 0.0 {
                return validated;
            }
        }

        // Fallback: DexScreener via Jupiter price API
        match self.jupiter.get_price(mint).await {
            Ok(price) => {
                let validated = self.validate_price(price, last_known, mint);
                // API succeeded — reset failure count regardless of validation
                self.reset_failure_count(mint);
                // Only cache if validation accepted the NEW price (not stale fallback)
                if (validated - price).abs() < f64::EPSILON {
                    self.cache_price(mint, validated);
                }
                validated
            }
            Err(e) => {
                // Try Birdeye if configured
                if let (Some(client), Some(api_key)) = (&self.birdeye_client, &self.birdeye_api_key)
                {
                    if let Ok(price) = self.fetch_birdeye(client, api_key, mint).await {
                        let validated = self.validate_price(price, last_known, mint);
                        // API succeeded — reset failure count
                        self.reset_failure_count(mint);
                        // Only cache if validation accepted the NEW price
                        if (validated - price).abs() < f64::EPSILON {
                            self.cache_price(mint, validated);
                        }
                        return validated;
                    }
                }

                let failures = self.increment_failure_count(mint);
                if failures >= MAX_CONSECUTIVE_FAILURES {
                    warn!(
                        mint = mint,
                        consecutive_failures = failures,
                        "All price sources failed {} times — returning 0 (token likely dead)",
                        failures
                    );
                    0.0
                } else {
                    warn!(
                        mint = mint,
                        "Price fetch failed: {} — using last known price", e
                    );
                    last_known
                }
            }
        }
    }

    /// Validate a price against sanity bounds.
    ///
    /// If any check fails, the last known price is returned instead and
    /// a warning is logged. This prevents corrupt data from triggering
    /// false exit signals.
    ///
    /// Checks:
    /// - Price must be > 0.0
    /// - Price must be < 1,000,000.0
    /// - Price must not be NaN
    /// - Price must not be infinite
    /// - If a last known price exists, the change must be < 1000%
    fn validate_price(&self, price: f64, last_known_price: f64, mint: &str) -> f64 {
        if price.is_nan() {
            warn!(
                mint = mint,
                "price_sanity_check_failed: price is NaN, using last known price"
            );
            return last_known_price;
        }

        if price.is_infinite() {
            warn!(
                mint = mint,
                "price_sanity_check_failed: price is infinite, using last known price"
            );
            return last_known_price;
        }

        if price <= 0.0 {
            warn!(
                mint = mint,
                price,
                "price_sanity_check_failed: price is zero or negative, using last known price"
            );
            return last_known_price;
        }

        if price >= self.max_sane_price {
            warn!(mint = mint, price, "price_sanity_check_failed: price exceeds maximum threshold, using last known price");
            return last_known_price;
        }

        if last_known_price > 0.0 {
            let change_pct = (price - last_known_price).abs() / last_known_price;
            if change_pct >= self.max_price_change_ratio {
                warn!(
                    mint = mint,
                    price,
                    last_known_price,
                    change_pct = format!("{:.2}%", change_pct * 100.0),
                    "price_sanity_check_failed: change exceeds 1000%, using last known price"
                );
                return last_known_price;
            }
        }

        price
    }

    async fn fetch_birdeye(&self, client: &Client, api_key: &str, mint: &str) -> Result<f64> {
        let resp = client
            .get(BIRDEYE_PRICE_URL)
            .header("X-API-KEY", api_key)
            .query(&[("address", mint)])
            .send()
            .await
            .context("Birdeye request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Birdeye HTTP {} — {}", status, body);
        }

        let json: serde_json::Value = resp.json().await.context("Birdeye JSON parse failed")?;
        let price = json
            .get("data")
            .and_then(|d| d.get("value"))
            .and_then(|v| v.as_f64())
            .context("Missing price in Birdeye response")?;

        Ok(price)
    }

    fn cache_price(&self, mint: &str, price: f64) {
        if let Ok(mut map) = self.last_known.lock() {
            map.insert(mint.to_string(), (price, Instant::now()));
            // Evict stale entries to prevent unbounded memory growth
            if map.len() > LAST_KNOWN_EVICT_THRESHOLD {
                map.retain(|_, (_, ts)| ts.elapsed() < LAST_KNOWN_MAX_AGE);
            }
        }
    }

    fn get_last_known(&self, mint: &str) -> f64 {
        self.last_known
            .lock()
            .ok()
            .and_then(|map| {
                map.get(mint).and_then(|(price, ts)| {
                    // Reject entries older than LAST_KNOWN_MAX_AGE
                    if ts.elapsed() < LAST_KNOWN_MAX_AGE {
                        Some(*price)
                    } else {
                        None
                    }
                })
            })
            .unwrap_or(0.0)
    }

    /// Remove a mint from all caches (call when position closes).
    pub fn remove_mint(&self, mint: &str) {
        if let Ok(mut map) = self.last_known.lock() {
            map.remove(mint);
        }
        if let Ok(mut map) = self.failure_count.lock() {
            map.remove(mint);
        }
    }

    fn increment_failure_count(&self, mint: &str) -> u32 {
        if let Ok(mut map) = self.failure_count.lock() {
            let count = map.entry(mint.to_string()).or_insert(0);
            *count += 1;
            let result = *count;
            // Prune dead-token failure counters when map grows large
            if map.len() > LAST_KNOWN_EVICT_THRESHOLD {
                map.retain(|k, c| k == mint || *c < MAX_CONSECUTIVE_FAILURES);
            }
            result
        } else {
            1
        }
    }

    fn reset_failure_count(&self, mint: &str) {
        if let Ok(mut map) = self.failure_count.lock() {
            map.remove(mint);
        }
    }

    /// Derive token USD price from a small Jupiter swap quote.
    ///
    /// Gets a quote for swapping 0.001 SOL → token, then computes:
    ///   price_per_token = (SOL_amount × SOL_USD_price) / token_out_amount
    ///
    /// This works even for brand-new tokens that DexScreener/Birdeye
    /// haven't indexed yet, as long as Jupiter can route through the pool.
    async fn derive_price_from_quote(&self, mint: &str) -> Option<f64> {
        if !self.jupiter.is_available() {
            return None;
        }

        // Use a small probe amount: 0.001 SOL = 1_000_000 lamports
        let probe_lamports: u64 = 1_000_000;
        let probe_sol: f64 = probe_lamports as f64 / 1_000_000_000.0;

        let quote = self
            .jupiter
            .get_quote(SOL_MINT, mint, probe_lamports, 5000)
            .await
            .ok()?;
        let out_amount: f64 = quote.out_amount.parse().ok()?;
        if out_amount <= 0.0 {
            return None;
        }

        // Get SOL/USD price (DexScreener usually has this even when token is missing)
        let sol_usd = self.jupiter.get_price(SOL_MINT).await.ok()?;
        if sol_usd <= 0.0 {
            return None;
        }

        // out_amount from Jupiter is in raw smallest units.
        // For pump.fun tokens (6 decimals), out_amount of 1000000 = 1.0 token.
        // DexScreener reports price per *whole* token (not per raw unit).
        // Since we sell/buy using raw units everywhere, we need price per raw unit
        // to be consistent with DexScreener prices. However, DexScreener prices
        // are already per whole token. The issue is that our derived formula
        // gives price-per-raw-unit when it should give price-per-whole-token.
        //
        // Actually, Jupiter quote's out_amount IS in raw smallest units, and
        // DexScreener's priceUsd IS per whole token. So to match DexScreener:
        //   price_per_whole_token = (probe_sol * sol_usd) / (out_amount / 10^decimals)
        //
        // But we don't know the token's decimals here. However, pump.fun tokens
        // are always 6 decimals. For safety, we use the same formula as DexScreener
        // would: the probe effectively tells us the exchange rate.
        //
        // Since out_amount is raw, and our monitoring loop compares against
        // DexScreener prices (which are per whole token), we need to multiply
        // by 10^decimals. Pump.fun = 6 decimals.
        let decimals: f64 = 6.0; // pump.fun standard
        let token_amount_ui = out_amount / 10_f64.powf(decimals);
        if token_amount_ui <= 0.0 {
            return None;
        }

        let derived = (probe_sol * sol_usd) / token_amount_ui;
        if derived > 1e-15 && derived < self.max_sane_price {
            Some(derived)
        } else {
            None
        }
    }
}
