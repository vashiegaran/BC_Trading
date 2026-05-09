use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as TokioMutex, Semaphore};
use tracing::{debug, warn};

// ─── Process-wide Jupiter throttle ─────────────────────────
//
// v5.4: we historically ran with a per-JupiterClient semaphore of 2.
// But there are FOUR distinct JupiterClient instances in the bot
// (sniper, execution, exit, monitoring::price) — each with its own
// semaphore — so peak concurrency was ~8 and detection bursts flooded
// Jupiter and triggered 429s. The v5 logs show 67 `real_trade_failed`
// events in one session, every one a Jupiter 429 that cost us the trade.
//
// This module-local throttle is shared by every JupiterClient via a
// `OnceLock`, so a 429 from one client pauses every other client too.
// The min-interval keeps steady-state RPS below the account limit and
// `Retry-After` is honoured after 429s.

/// Minimum interval between outbound Jupiter calls, in milliseconds.
/// Read once from `JUPITER_MIN_INTERVAL_MS` (defaults to 100 ms → ~10 RPS,
/// which is below the paid tier of ~50 RPS but above the free tier of 10 RPS).
/// Set this to 80 for paid tier headroom or 150 for free tier safety.
const DEFAULT_JUPITER_MIN_INTERVAL_MS: u64 = 100;

/// Fallback cooldown when a 429 response has no `Retry-After` header.
const DEFAULT_JUPITER_COOLDOWN: Duration = Duration::from_secs(5);

/// Cap on how long we'll obey a `Retry-After` hint — prevents a malformed
/// header or hostile response from stalling exits for minutes.
const MAX_JUPITER_COOLDOWN: Duration = Duration::from_secs(15);

struct JupThrottleState {
    next_allowed_at: Instant,
    min_interval: Duration,
}

fn jup_throttle() -> &'static TokioMutex<JupThrottleState> {
    static THROTTLE: OnceLock<TokioMutex<JupThrottleState>> = OnceLock::new();
    THROTTLE.get_or_init(|| {
        let ms = std::env::var("JUPITER_MIN_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_JUPITER_MIN_INTERVAL_MS);
        TokioMutex::new(JupThrottleState {
            next_allowed_at: Instant::now(),
            min_interval: Duration::from_millis(ms),
        })
    })
}

/// Block the current task until the throttle allows the next Jupiter call.
/// Advances the shared "next allowed" timestamp by `min_interval`.
async fn jup_throttle_wait() {
    let sleep_for = {
        let mut g = jup_throttle().lock().await;
        let now = Instant::now();
        let wait = g.next_allowed_at.saturating_duration_since(now);
        let base = if wait.is_zero() {
            now
        } else {
            g.next_allowed_at
        };
        g.next_allowed_at = base + g.min_interval;
        wait
    };
    if !sleep_for.is_zero() {
        tokio::time::sleep(sleep_for).await;
    }
}

/// Extend the shared throttle gate by `cooldown`. Every other JupiterClient
/// in the process will wait behind this when it next calls `jup_throttle_wait`.
async fn jup_throttle_cooldown(cooldown: Duration) {
    let cooldown = cooldown.min(MAX_JUPITER_COOLDOWN);
    let mut g = jup_throttle().lock().await;
    let target = Instant::now() + cooldown;
    if target > g.next_allowed_at {
        g.next_allowed_at = target;
    }
}

/// Parse a Retry-After header value into a Duration. Jupiter returns seconds.
/// Falls back to `DEFAULT_JUPITER_COOLDOWN` on parse failure.
fn parse_retry_after(header: Option<&reqwest::header::HeaderValue>) -> Duration {
    header
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_JUPITER_COOLDOWN)
}

// ─── Public types ──────────────────────────────────────────

/// Parsed Jupiter `/swap/v1/quote` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterQuote {
    #[serde(rename = "inputMint")]
    pub input_mint: String,
    #[serde(rename = "outputMint")]
    pub output_mint: String,
    #[serde(rename = "inAmount")]
    pub in_amount: String,
    #[serde(rename = "outAmount")]
    pub out_amount: String,
    #[serde(rename = "priceImpactPct")]
    pub price_impact_pct: String,
    #[serde(rename = "routePlan", default)]
    pub route_plan: serde_json::Value,
    /// The complete raw JSON — forwarded as-is to `/swap/v1/swap`.
    #[serde(flatten)]
    pub raw: serde_json::Value,
}

/// Jupiter `/swap/v1/swap` response — contains a base64-encoded transaction.
#[derive(Debug, Deserialize)]
pub struct JupiterSwapResponse {
    #[serde(rename = "swapTransaction")]
    pub swap_transaction: String, // base64-encoded versioned transaction
}

/// DexScreener token price response.
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
    #[serde(rename = "pairAddress")]
    pair_address: Option<String>,
}

// ─── Constants ─────────────────────────────────────────────

const JUPITER_BASE_URL: &str = "https://api.jup.ag/swap/v1";
const DEXSCREENER_TOKEN_URL: &str = "https://api.dexscreener.com/latest/dex/tokens";
/// DexScreener price cache TTL — avoids duplicate calls within a short window.
const DEXSCREENER_CACHE_TTL: Duration = Duration::from_secs(5);

/// SOL native mint address used as input for buys.
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Maximum concurrent Jupiter API requests. Prevents burst 429s by serializing
/// access when many buy/exit tasks fire simultaneously. The free Jupiter tier
/// allows ~10 RPS — 2 concurrent permits with internal retries keeps us under.
const JUPITER_MAX_CONCURRENT: usize = 2;

// ─── Jupiter API client ────────────────────────────────────

/// Thin HTTP wrapper around the Jupiter Swap V1 API and DexScreener price API.
#[derive(Debug, Clone)]
pub struct JupiterClient {
    client: Client,
    api_key: Option<String>,
    max_retries: u32,
    /// DexScreener price cache: mint -> (price_usd, fetched_at)
    price_cache: Arc<Mutex<HashMap<String, (f64, Instant)>>>,
    /// Concurrency limiter for Jupiter API requests.
    rate_limiter: Arc<Semaphore>,
}

impl JupiterClient {
    pub fn new(api_request_timeout_secs: u64, max_retries: u32) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(api_request_timeout_secs))
            .build()
            .expect("Failed to build reqwest client for Jupiter");

        let api_key = std::env::var("JUPITER_API_KEY").ok();
        if api_key.is_none() {
            warn!("JUPITER_API_KEY not set — Jupiter swap calls will be skipped");
        }

        Self {
            client,
            api_key,
            max_retries,
            price_cache: Arc::new(Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(Semaphore::new(JUPITER_MAX_CONCURRENT)),
        }
    }

    /// Returns true if an API key is configured and Jupiter calls can proceed.
    pub fn is_available(&self) -> bool {
        self.api_key.is_some()
    }

    // ── get_quote ────────────────────────────────────────

    /// Fetch a swap quote from Jupiter.
    ///
    /// `amount_lamports` — amount of input token in its smallest unit.
    /// `slippage_bps`    — slippage tolerance in basis points.
    pub async fn get_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_lamports: u64,
        slippage_bps: u64,
    ) -> Result<JupiterQuote> {
        let api_key = match &self.api_key {
            Some(k) => k.clone(),
            None => bail!("Jupiter API key not set — cannot fetch quote"),
        };

        let quote_url = format!("{}/quote", JUPITER_BASE_URL);
        let mut last_err: Option<anyhow::Error> = None;
        let _permit = self
            .rate_limiter
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("Jupiter rate limiter closed"))?;

        for attempt in 0..self.max_retries {
            // Shared process-wide throttle. Every JupiterClient instance in
            // the bot waits behind the same gate, so a 429 triggered by one
            // task makes every other task back off too.
            jup_throttle_wait().await;

            if attempt > 0 {
                // Local exponential backoff between our own retries — the
                // shared cooldown is already applied via jup_throttle_wait
                // after a 429, so this is only for non-429 errors.
                let backoff = Duration::from_millis(250 * 2u64.pow(attempt - 1));
                debug!(attempt, "Retrying Jupiter quote after {:?}", backoff);
                tokio::time::sleep(backoff).await;
            }

            let result = self
                .client
                .get(&quote_url)
                .header("x-api-key", &api_key)
                .query(&[
                    ("inputMint", input_mint),
                    ("outputMint", output_mint),
                    ("amount", &amount_lamports.to_string()),
                    ("slippageBps", &slippage_bps.to_string()),
                ])
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        // Parse as raw JSON first to preserve the complete response
                        let raw_json: serde_json::Value = resp
                            .json()
                            .await
                            .context("Failed to parse Jupiter quote response as JSON")?;

                        // Build JupiterQuote manually so raw contains the FULL response
                        let quote = JupiterQuote {
                            input_mint: raw_json
                                .get("inputMint")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            output_mint: raw_json
                                .get("outputMint")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            in_amount: raw_json
                                .get("inAmount")
                                .and_then(|v| v.as_str())
                                .unwrap_or("0")
                                .to_string(),
                            out_amount: raw_json
                                .get("outAmount")
                                .and_then(|v| v.as_str())
                                .unwrap_or("0")
                                .to_string(),
                            price_impact_pct: raw_json
                                .get("priceImpactPct")
                                .and_then(|v| v.as_str())
                                .unwrap_or("0")
                                .to_string(),
                            route_plan: raw_json
                                .get("routePlan")
                                .cloned()
                                .unwrap_or(serde_json::Value::Null),
                            raw: raw_json, // Complete response for /swap endpoint
                        };
                        return Ok(quote);
                    }
                    // Read Retry-After BEFORE consuming the body.
                    let retry_after =
                        parse_retry_after(resp.headers().get(reqwest::header::RETRY_AFTER));
                    let is_429 = status.as_u16() == 429;
                    let body = resp.text().await.unwrap_or_default();
                    // Permanent failures — no point retrying
                    if body.contains("TOKEN_NOT_TRADABLE") {
                        bail!("Jupiter quote: TOKEN_NOT_TRADABLE — {}", body);
                    }
                    if is_429 {
                        warn!(
                            retry_after_secs = retry_after.as_secs(),
                            "Jupiter quote 429 — applying shared cooldown"
                        );
                        jup_throttle_cooldown(retry_after).await;
                    }
                    last_err = Some(anyhow::anyhow!("Jupiter quote HTTP {} — {}", status, body));
                }
                Err(e) => {
                    last_err = Some(e.into());
                }
            }
        }

        bail!(
            "Jupiter get_quote failed after {} retries: {}",
            self.max_retries,
            last_err.unwrap()
        );
    }

    // ── get_swap_transaction ─────────────────────────────

    /// Request a swap transaction from Jupiter using a previously fetched quote.
    ///
    /// Returns base64-encoded versioned transaction bytes ready for signing.
    /// `slippage_range_bps` — when set, configures Jupiter's `dynamicSlippage`
    /// with `(minBps, maxBps)` range. Giving Jupiter a range lets it optimise
    /// the on-chain tolerance instead of being locked to a single value.
    pub async fn get_swap_transaction(
        &self,
        quote_response: &serde_json::Value,
        user_public_key: &str,
        slippage_range_bps: Option<(u64, u64)>,
        priority_fee: Option<(u64, &str)>,
    ) -> Result<String> {
        let api_key = match &self.api_key {
            Some(k) => k.clone(),
            None => bail!("Jupiter API key not set — cannot fetch swap transaction"),
        };

        let swap_url = format!("{}/swap", JUPITER_BASE_URL);

        let mut body = serde_json::json!({
            "quoteResponse": quote_response,
            "userPublicKey": user_public_key,
            "wrapAndUnwrapSol": true,
            "dynamicComputeUnitLimit": true,
        });

        // Priority fee: let Jupiter bake ComputeBudget instructions into the tx
        if let Some((max_lamports, level)) = priority_fee {
            body.as_object_mut().unwrap().insert(
                "prioritizationFeeLamports".to_string(),
                serde_json::json!({
                    "priorityLevelWithMaxLamports": {
                        "maxLamports": max_lamports,
                        "priorityLevel": level
                    }
                }),
            );
        }

        // Give Jupiter a slippage range so it can pick the optimal on-chain
        // tolerance.  For exits the range is wide (e.g. 5000–10000) so the
        // tx can still land when the pool is volatile.
        if let Some((min_bps, max_bps)) = slippage_range_bps {
            body.as_object_mut().unwrap().insert(
                "dynamicSlippage".to_string(),
                serde_json::json!({ "minBps": min_bps, "maxBps": max_bps }),
            );
        }

        let mut last_err: Option<anyhow::Error> = None;
        let _permit = self
            .rate_limiter
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("Jupiter rate limiter closed"))?;

        for attempt in 0..self.max_retries {
            jup_throttle_wait().await;

            if attempt > 0 {
                let backoff = Duration::from_millis(250 * 2u64.pow(attempt - 1));
                debug!(attempt, "Retrying Jupiter swap tx after {:?}", backoff);
                tokio::time::sleep(backoff).await;
            }

            let result = self
                .client
                .post(&swap_url)
                .header("x-api-key", &api_key)
                .json(&body)
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let swap_resp: JupiterSwapResponse = resp
                            .json()
                            .await
                            .context("Failed to parse Jupiter swap response")?;
                        return Ok(swap_resp.swap_transaction);
                    }
                    let retry_after =
                        parse_retry_after(resp.headers().get(reqwest::header::RETRY_AFTER));
                    let is_429 = status.as_u16() == 429;
                    let resp_body = resp.text().await.unwrap_or_default();
                    if is_429 {
                        warn!(
                            retry_after_secs = retry_after.as_secs(),
                            "Jupiter swap 429 — applying shared cooldown"
                        );
                        jup_throttle_cooldown(retry_after).await;
                    }
                    last_err = Some(anyhow::anyhow!(
                        "Jupiter swap HTTP {} — {}",
                        status,
                        resp_body
                    ));
                }
                Err(e) => {
                    last_err = Some(e.into());
                }
            }
        }

        bail!(
            "Jupiter get_swap_transaction failed after {} retries: {}",
            self.max_retries,
            last_err.unwrap()
        );
    }

    // ── get_price (DexScreener, cached) ───────────────────

    /// Fetch the current USD price of a token via DexScreener.
    ///
    /// Results are cached for 5 seconds to avoid duplicate calls.
    /// Used by the monitoring engine for position tracking.
    pub async fn get_price(&self, mint: &str) -> Result<f64> {
        // Check cache first
        if let Ok(cache) = self.price_cache.lock() {
            if let Some((price, ts)) = cache.get(mint) {
                if ts.elapsed() < DEXSCREENER_CACHE_TTL {
                    return Ok(*price);
                }
            }
        }

        let url = format!("{}/{}", DEXSCREENER_TOKEN_URL, mint);

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("DexScreener price API request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("DexScreener price API HTTP {} — {}", status, body);
        }

        let body: DexScreenerResponse = resp
            .json()
            .await
            .context("Failed to parse DexScreener price response")?;

        let pairs = body.pairs.context("No pairs returned by DexScreener")?;

        let pair = pairs
            .iter()
            .find(|p| p.chain_id == "solana")
            .or_else(|| pairs.first())
            .with_context(|| format!("No price data returned for mint {}", mint))?;

        pair.price_usd
            .as_ref()
            .context("priceUsd missing in DexScreener response")?
            .parse::<f64>()
            .context("Failed to parse priceUsd as f64")
            .map(|price| {
                // Populate cache
                if let Ok(mut cache) = self.price_cache.lock() {
                    cache.insert(mint.to_string(), (price, Instant::now()));
                    // Evict stale entries periodically
                    if cache.len() > 500 {
                        cache.retain(|_, (_, ts)| ts.elapsed() < DEXSCREENER_CACHE_TTL * 2);
                    }
                }
                price
            })
    }

    /// Look up the Raydium pool address for a token via DexScreener.
    ///
    /// Returns `None` if DexScreener doesn't have the pair yet.
    pub async fn get_pool_address(&self, mint: &str) -> Option<String> {
        let url = format!("{}/{}", DEXSCREENER_TOKEN_URL, mint);
        let resp = self.client.get(&url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body: DexScreenerResponse = resp.json().await.ok()?;
        let pairs = body.pairs?;
        let pair = pairs
            .iter()
            .find(|p| p.chain_id == "solana")
            .or_else(|| pairs.first())?;
        pair.pair_address.clone()
    }
}

impl Default for JupiterClient {
    fn default() -> Self {
        Self::new(10, 3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_retry_after_seconds() {
        let h = reqwest::header::HeaderValue::from_static("3");
        assert_eq!(parse_retry_after(Some(&h)), Duration::from_secs(3));
    }

    #[test]
    fn parse_retry_after_missing_defaults() {
        assert_eq!(parse_retry_after(None), DEFAULT_JUPITER_COOLDOWN);
    }

    #[test]
    fn parse_retry_after_garbage_defaults() {
        let h = reqwest::header::HeaderValue::from_static("soon");
        assert_eq!(parse_retry_after(Some(&h)), DEFAULT_JUPITER_COOLDOWN);
    }

    #[tokio::test]
    async fn throttle_enforces_min_interval() {
        // Trigger init and then exercise the gate. We can't reset the
        // static, so just verify two consecutive waits advance the clock
        // by at least the min interval.
        let start = Instant::now();
        jup_throttle_wait().await;
        jup_throttle_wait().await;
        let elapsed = start.elapsed();
        // min_interval defaults to 100ms; allow slop for scheduling.
        assert!(
            elapsed >= Duration::from_millis(50),
            "two throttle waits should take ≥ ~min_interval, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn throttle_cooldown_extends_gate() {
        jup_throttle_wait().await;
        let start = Instant::now();
        jup_throttle_cooldown(Duration::from_millis(200)).await;
        jup_throttle_wait().await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(150),
            "cooldown should delay next wait, got {:?}",
            elapsed
        );
    }
}
