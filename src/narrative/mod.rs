//! Narrative detection module — 3-tier scoring system:
//!
//! **Tier 1 ($0.016):** DexScreener/Birdeye social links contain a tweet URL →
//! X API fetches real metrics (followers, likes, views, tweet text) → all data
//! passed to OpenAI Tier 2 (no web search) for holistic judgment.
//!
//! **Tier 1b ($0.011):** Social links contain a Twitter profile URL (no tweet) →
//! X API fetches user metrics (followers) → passed to OpenAI Tier 2.
//!
//! **Tier 2 (~$0.001):** Social links exist but no Twitter URL → call OpenAI
//! WITHOUT web_search_preview (just analysis of provided data).
//!
//! **Tier 3 ($0.025, last resort):** No social links at all → call OpenAI WITH
//! web_search_preview to search the internet for mentions.
//!
//! Pre-filters with DexScreener + Birdeye: if both show zero life, returns
//! NoSignal without any API call.
//!
//! Returns a `NarrativeResult` with a state, score, tier, and reasoning.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ── Types ────────────────────────────────────────────────────

/// Graduated narrative states — can only move UP (ratchet).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NarrativeState {
    NoSignal,
    EarlyAttention,
    ExpandingAttention,
    RunnerConfirmed,
}

impl std::fmt::Display for NarrativeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSignal => write!(f, "no_signal"),
            Self::EarlyAttention => write!(f, "early_attention"),
            Self::ExpandingAttention => write!(f, "expanding_attention"),
            Self::RunnerConfirmed => write!(f, "runner_confirmed"),
        }
    }
}

impl NarrativeState {
    /// Map a 0-100 score to a narrative state.
    pub fn from_score(score: u8) -> Self {
        match score {
            0..=25 => Self::NoSignal,
            26..=50 => Self::EarlyAttention,
            51..=75 => Self::ExpandingAttention,
            76..=100 => Self::RunnerConfirmed,
            _ => Self::RunnerConfirmed,
        }
    }
}

/// Result from a narrative check — structured response from OpenAI or X API + OpenAI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NarrativeResult {
    pub state: NarrativeState,
    pub score: u8,
    pub narrative_strength: String,
    pub market_strength: String,
    pub reasons: Vec<String>,
    pub risk_flags: Vec<String>,
    pub web_sources_found: u8,
    /// Which tier produced this result. Stored in Supabase for post-analysis.
    /// Values: "tier_0_dead_market", "tier_1_x_tweet", "tier_1b_x_profile",
    ///         "tier_2_openai_no_search", "tier_3_openai_with_search"
    #[serde(default)]
    pub tier: String,
    /// Latency: DexScreener + Birdeye prefilter fetch (ms).
    #[serde(default)]
    pub prefilter_ms: u64,
    /// Latency: X API calls — tweet or user lookup (ms). 0 if no X API call.
    #[serde(default)]
    pub x_api_ms: u64,
    /// Latency: OpenAI API call (ms). 0 if short-circuited.
    #[serde(default)]
    pub openai_ms: u64,
    /// Latency: total check_narrative wall time (ms).
    #[serde(default)]
    pub total_ms: u64,
    /// Latency: X API search/recent call (ms). 0 if no search or x_bearer empty.
    #[serde(default)]
    pub search_ms: u64,
    /// Number of tweets found via X API search for this token's CA/symbol.
    #[serde(default)]
    pub search_tweet_count: u16,
    /// Diamond ratio: approximate % of buyers still holding (1.0 - sells/buys).
    /// Computed from Birdeye 24h buy/sell counts. -1.0 if Birdeye data unavailable.
    #[serde(default = "default_diamond_ratio")]
    pub diamond_ratio: f64,
}

fn default_diamond_ratio() -> f64 {
    -1.0
}

/// Input context passed to the narrative checker.
#[derive(Debug, Clone)]
pub struct NarrativeContext {
    pub mint: String,
    pub name: String,
    pub symbol: String,
    pub current_price_usd: f64,
    pub entry_price_usd: f64,
    pub peak_multiplier: f64,
    pub hold_seconds: u64,
    pub buy_count: u32,
    pub sell_count: u32,
    pub momentum_ratio: f64,
    pub buy_volume_sol: f64,
    pub sell_volume_sol: f64,
}

// ── DexScreener data ─────────────────────────────────────────

#[derive(Debug, Default)]
struct DexScreenerData {
    volume_24h: f64,
    maker_count_24h: u64,
    fdv_usd: f64,
    liquidity_usd: f64,
    price_change_5m: f64,
    price_change_1h: f64,
    boosts: u64,
    trending_rank: Option<u32>,
    /// Social links from DexScreener info.socials (e.g. Twitter communities)
    social_urls: Vec<(String, String)>, // (type, url)
    /// Website links from DexScreener info.websites
    website_urls: Vec<(String, String)>, // (label, url)
}

async fn fetch_dexscreener(client: &reqwest::Client, mint: &str) -> DexScreenerData {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", mint);

    let resp = match client
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            debug!(mint = mint, status = %r.status(), "DexScreener returned non-200");
            return DexScreenerData::default();
        }
        Err(e) => {
            debug!(mint = mint, "DexScreener fetch failed: {}", e);
            return DexScreenerData::default();
        }
    };

    let json: serde_json::Value = match resp.json().await {
        Ok(j) => j,
        Err(_) => return DexScreenerData::default(),
    };

    let pair = json
        .get("pairs")
        .and_then(|p| p.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|p| p.get("chainId").and_then(|c| c.as_str()) == Some("solana"))
                .or_else(|| arr.first())
        });

    let pair = match pair {
        Some(p) => p,
        None => return DexScreenerData::default(),
    };

    DexScreenerData {
        volume_24h: pair
            .get("volume")
            .and_then(|v| v.get("h24"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        maker_count_24h: pair
            .get("txns")
            .and_then(|t| t.get("h24"))
            .map(|h24| {
                let buys = h24.get("buys").and_then(|b| b.as_u64()).unwrap_or(0);
                let sells = h24.get("sells").and_then(|s| s.as_u64()).unwrap_or(0);
                buys + sells
            })
            .unwrap_or(0),
        fdv_usd: pair.get("fdv").and_then(|v| v.as_f64()).unwrap_or(0.0),
        liquidity_usd: pair
            .get("liquidity")
            .and_then(|l| l.get("usd"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        price_change_5m: pair
            .get("priceChange")
            .and_then(|p| p.get("m5"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        price_change_1h: pair
            .get("priceChange")
            .and_then(|p| p.get("h1"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        boosts: pair
            .get("boosts")
            .and_then(|b| b.get("active"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        trending_rank: None,
        social_urls: pair
            .get("info")
            .and_then(|i| i.get("socials"))
            .and_then(|s| s.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| {
                        let stype = s
                            .get("type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let url = s
                            .get("url")
                            .and_then(|u| u.as_str())
                            .unwrap_or("")
                            .to_string();
                        if url.is_empty() {
                            None
                        } else {
                            Some((stype, url))
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
        website_urls: pair
            .get("info")
            .and_then(|i| i.get("websites"))
            .and_then(|w| w.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|w| {
                        let label = w
                            .get("label")
                            .and_then(|l| l.as_str())
                            .unwrap_or("Website")
                            .to_string();
                        let url = w
                            .get("url")
                            .and_then(|u| u.as_str())
                            .unwrap_or("")
                            .to_string();
                        if url.is_empty() {
                            None
                        } else {
                            Some((label, url))
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

// ── Birdeye overview (cheap pre-filter + social enrichment) ──

#[derive(Debug, Default)]
struct BirdeyePreFilter {
    has_twitter: bool,
    has_telegram: bool,
    has_website: bool,
    twitter_url: String,
    telegram_url: String,
    website_url: String,
    unique_wallets_24h: u64,
    holders: u64,
    buy_24h: u64,
    sell_24h: u64,
    fetched: bool,
}

async fn fetch_birdeye_overview(
    client: &reqwest::Client,
    api_key: &str,
    mint: &str,
) -> BirdeyePreFilter {
    if api_key.is_empty() {
        return BirdeyePreFilter::default();
    }

    let url = format!(
        "https://public-api.birdeye.so/defi/token_overview?address={}",
        mint
    );
    let resp = match client
        .get(&url)
        .header("X-API-KEY", api_key)
        .header("x-chain", "solana")
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            debug!(mint = mint, status = %r.status(), "Birdeye overview returned non-200");
            return BirdeyePreFilter::default();
        }
        Err(e) => {
            debug!(mint = mint, "Birdeye overview fetch failed: {}", e);
            return BirdeyePreFilter::default();
        }
    };

    let json: serde_json::Value = match resp.json().await {
        Ok(j) => j,
        Err(_) => return BirdeyePreFilter::default(),
    };

    let data = json.get("data").unwrap_or(&json);

    let ext = data.get("extensions");
    let twitter = ext
        .and_then(|e| e.get("twitter"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let telegram = ext
        .and_then(|e| e.get("telegram"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let website = ext
        .and_then(|e| e.get("website"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    BirdeyePreFilter {
        has_twitter: !twitter.is_empty(),
        has_telegram: !telegram.is_empty(),
        has_website: !website.is_empty(),
        twitter_url: twitter,
        telegram_url: telegram,
        website_url: website,
        unique_wallets_24h: data
            .get("uniqueWallet24h")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        holders: data.get("holder").and_then(|v| v.as_u64()).unwrap_or(0),
        buy_24h: data.get("buy24h").and_then(|v| v.as_u64()).unwrap_or(0),
        sell_24h: data.get("sell24h").and_then(|v| v.as_u64()).unwrap_or(0),
        fetched: true,
    }
}

// ── OpenAI call ──────────────────────────────────────────────

// ── Twitter username extraction ──────────────────────────────

/// Extract Twitter/X username from a URL.
/// Handles: https://x.com/username, https://twitter.com/username,
///          https://x.com/username/status/123, etc.
fn extract_twitter_username(url: &str) -> Option<String> {
    let url_lower = url.to_lowercase();
    // Match x.com or twitter.com URLs
    let path = if url_lower.contains("x.com/") {
        url.split("x.com/").nth(1)
    } else if url_lower.contains("twitter.com/") {
        url.split("twitter.com/").nth(1)
    } else {
        None
    };

    path.and_then(|p| {
        let username = p.split('/').next().unwrap_or("").trim();
        // Skip non-user paths
        if username.is_empty()
            || username == "i"
            || username == "search"
            || username == "home"
            || username == "explore"
            || username == "hashtag"
        {
            None
        } else {
            Some(username.to_string())
        }
    })
}

/// Collect all Twitter usernames from DexScreener + Birdeye social links.
fn collect_twitter_usernames(dex: &DexScreenerData, birdeye: &BirdeyePreFilter) -> Vec<String> {
    let mut usernames = Vec::new();

    // DexScreener website URLs (often x.com/kol/status/... links)
    for (_label, url) in &dex.website_urls {
        if let Some(u) = extract_twitter_username(url) {
            usernames.push(u);
        }
    }

    // DexScreener social URLs
    for (_stype, url) in &dex.social_urls {
        if let Some(u) = extract_twitter_username(url) {
            usernames.push(u);
        }
    }

    // Birdeye twitter URL
    if birdeye.has_twitter {
        if let Some(u) = extract_twitter_username(&birdeye.twitter_url) {
            usernames.push(u);
        }
    }

    // Deduplicate (case-insensitive)
    usernames.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
    usernames.dedup_by(|a, b| a.to_lowercase() == b.to_lowercase());
    usernames
}

// ── X API tweet/user metrics ─────────────────────────────

/// Metrics returned from X API tweet lookup.
#[derive(Debug, Clone)]
struct TweetMetrics {
    author_username: String,
    author_name: String,
    followers: u64,
    likes: u64,
    retweets: u64,
    replies: u64,
    views: u64,
    tweet_text: String,
}

/// Metrics returned from X API user lookup.
#[derive(Debug, Clone)]
struct UserMetrics {
    username: String,
    name: String,
    followers: u64,
}

/// Extract tweet ID from a tweet URL.
/// Handles: https://x.com/user/status/123456, https://twitter.com/user/status/123456
fn extract_tweet_id(url: &str) -> Option<String> {
    let url_lower = url.to_lowercase();
    if !url_lower.contains("/status/") {
        return None;
    }
    let after_status = url.split("/status/").nth(1)?;
    let id = after_status
        .split(&['/', '?', '#'][..])
        .next()
        .unwrap_or("");
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(id.to_string())
}

/// Find the first tweet URL in social links. Returns (tweet_url, tweet_id).
fn find_tweet_url(dex: &DexScreenerData, birdeye: &BirdeyePreFilter) -> Option<(String, String)> {
    let all_urls = dex
        .website_urls
        .iter()
        .map(|(_, u)| u.as_str())
        .chain(dex.social_urls.iter().map(|(_, u)| u.as_str()))
        .chain(std::iter::once(birdeye.twitter_url.as_str()));

    for url in all_urls {
        if let Some(id) = extract_tweet_id(url) {
            return Some((url.to_string(), id));
        }
    }
    None
}

/// Fetch tweet + author metrics from X API v2.
/// Endpoint: GET /2/tweets/:id?expansions=author_id&tweet.fields=public_metrics&user.fields=public_metrics
/// Cost: $0.005 (Posts:Read) + $0.010 (User:Read via expansion) = $0.015
async fn fetch_tweet_metrics(
    client: &reqwest::Client,
    bearer_token: &str,
    tweet_id: &str,
) -> Option<TweetMetrics> {
    let url = format!(
        "https://api.x.com/2/tweets/{}?expansions=author_id&tweet.fields=public_metrics,text&user.fields=public_metrics,username,name",
        tweet_id
    );

    let resp = match client
        .get(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            warn!(tweet_id, status = %status, "X API tweet lookup failed: {}", body);
            return None;
        }
        Err(e) => {
            warn!(tweet_id, "X API tweet request failed: {}", e);
            return None;
        }
    };

    let json: serde_json::Value = resp.json().await.ok()?;

    let tweet_data = json.get("data")?;
    let tweet_metrics = tweet_data.get("public_metrics")?;
    let tweet_text = tweet_data
        .get("text")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();

    // Author is in includes.users[0]
    let author = json
        .get("includes")
        .and_then(|i| i.get("users"))
        .and_then(|u| u.as_array())
        .and_then(|arr| arr.first())?;
    let user_metrics = author.get("public_metrics")?;

    Some(TweetMetrics {
        author_username: author
            .get("username")
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string(),
        author_name: author
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string(),
        followers: user_metrics
            .get("followers_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        likes: tweet_metrics
            .get("like_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        retweets: tweet_metrics
            .get("retweet_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        replies: tweet_metrics
            .get("reply_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        views: tweet_metrics
            .get("impression_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        tweet_text,
    })
}

/// Fetch user metrics from X API v2 by username.
/// Endpoint: GET /2/users/by/username/:username?user.fields=public_metrics
/// Cost: $0.010 (User:Read)
async fn fetch_user_metrics(
    client: &reqwest::Client,
    bearer_token: &str,
    username: &str,
) -> Option<UserMetrics> {
    let url = format!(
        "https://api.x.com/2/users/by/username/{}?user.fields=public_metrics,name",
        username
    );

    let resp = match client
        .get(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            let status = r.status();
            debug!(username, status = %status, "X API user lookup failed");
            return None;
        }
        Err(e) => {
            debug!(username, "X API user request failed: {}", e);
            return None;
        }
    };

    let json: serde_json::Value = resp.json().await.ok()?;
    let data = json.get("data")?;
    let metrics = data.get("public_metrics")?;

    Some(UserMetrics {
        username: data
            .get("username")
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string(),
        name: data
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string(),
        followers: metrics
            .get("followers_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
    })
}

/// Build social context string from X API tweet metrics for the OpenAI prompt.
/// Note: tweet text may NOT be about the token — devs often link to any tweet
/// from an influencer to signal association. What matters is the account size.
fn build_x_tweet_context(m: &TweetMetrics) -> String {
    format!(
        "X/TWITTER DATA (verified via X API — real metrics, not scraped):\n\
         - Account: @{username} ({name})\n\
         - Followers: {followers}\n\
         - Tweet engagement: {likes} likes, {retweets} retweets, {views} views\n\
         - NOTE: The tweet text below may NOT be about this token. Devs often link\n\
           to ANY tweet from an influencer to signal the account is involved.\n\
           Focus on the ACCOUNT SIZE and CREDIBILITY, not the tweet content.\n\
         - Tweet text (for reference only): \"{text}\"",
        username = m.author_username,
        name = m.author_name,
        followers = m.followers,
        likes = m.likes,
        retweets = m.retweets,
        views = m.views,
        text = m.tweet_text,
    )
}

/// Build social context string from X API user metrics for the OpenAI prompt.
fn build_x_user_context(m: &UserMetrics) -> String {
    format!(
        "X/TWITTER DATA (verified via X API — real metrics, not scraped):\n\
         - Profile: @{username} ({name})\n\
         - Followers: {followers}\n\
         - No specific tweet found — profile link only",
        username = m.username,
        name = m.name,
        followers = m.followers,
    )
}

// ── X API tweet search ──────────────────────────────────────

/// Aggregated results from searching X/Twitter for mentions of a token.
#[derive(Debug, Default, Clone)]
struct TwitterSearchResult {
    /// Number of tweets found mentioning this token (last 7 days).
    tweet_count: u16,
    /// Sum of likes across all found tweets.
    total_likes: u64,
    /// Sum of retweets across all found tweets.
    total_retweets: u64,
    /// Sum of views/impressions across all found tweets.
    total_views: u64,
    /// Sum of replies across all found tweets.
    total_replies: u64,
    /// Whether the search actually ran (false if no bearer token or error).
    searched: bool,
}

/// Search X/Twitter for recent tweets mentioning a token's CA or symbol.
/// Uses `GET /2/tweets/search/recent` — costs $0.0025 per request (no user expansion).
/// Returns aggregated engagement metrics across all matching tweets.
async fn fetch_twitter_search(
    client: &reqwest::Client,
    bearer_token: &str,
    mint: &str,
    symbol: &str,
) -> TwitterSearchResult {
    if bearer_token.is_empty() {
        return TwitterSearchResult::default();
    }

    // Search for CA (exact match, most reliable) OR $SYMBOL (catches ticker mentions).
    // Exclude retweets to avoid double-counting engagement.
    let query = if symbol.is_empty() || symbol.len() < 2 {
        format!("\"{}\" -is:retweet", mint)
    } else {
        format!("\"{}\" OR \"${}\" -is:retweet", mint, symbol)
    };

    let resp = match client
        .get("https://api.x.com/2/tweets/search/recent")
        .query(&[
            ("query", query.as_str()),
            ("tweet.fields", "public_metrics,created_at"),
            ("max_results", "10"),
        ])
        .header("Authorization", format!("Bearer {}", bearer_token))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            let status = r.status();
            debug!(mint = mint, status = %status, "X API search returned non-200");
            return TwitterSearchResult {
                searched: true,
                ..Default::default()
            };
        }
        Err(e) => {
            debug!(mint = mint, "X API search request failed: {}", e);
            return TwitterSearchResult::default();
        }
    };

    let json: serde_json::Value = match resp.json().await {
        Ok(j) => j,
        Err(_) => {
            return TwitterSearchResult {
                searched: true,
                ..Default::default()
            }
        }
    };

    let result_count = json
        .get("meta")
        .and_then(|m| m.get("result_count"))
        .and_then(|c| c.as_u64())
        .unwrap_or(0);

    if result_count == 0 {
        return TwitterSearchResult {
            searched: true,
            tweet_count: 0,
            ..Default::default()
        };
    }

    let tweets = json
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    let mut total_likes: u64 = 0;
    let mut total_retweets: u64 = 0;
    let mut total_views: u64 = 0;
    let mut total_replies: u64 = 0;

    for tweet in &tweets {
        if let Some(metrics) = tweet.get("public_metrics") {
            total_likes += metrics
                .get("like_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            total_retweets += metrics
                .get("retweet_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            total_views += metrics
                .get("impression_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            total_replies += metrics
                .get("reply_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
        }
    }

    // Use API's result_count (true total) instead of tweets.len() (capped by max_results=10)
    let true_count = result_count as u16;

    info!(
        mint = mint,
        tweet_count = true_count,
        fetched = tweets.len(),
        total_likes,
        total_retweets,
        total_views,
        "🔍 X API search — found tweets mentioning token"
    );

    TwitterSearchResult {
        tweet_count: true_count,
        total_likes,
        total_retweets,
        total_views,
        total_replies,
        searched: true,
    }
}

/// Build a context string from Twitter search results for the OpenAI prompt.
fn build_search_context(search: &TwitterSearchResult) -> String {
    if !search.searched {
        return String::new();
    }
    if search.tweet_count == 0 {
        return "\nTWITTER BUZZ (via X API search — last 7 days):\n- ZERO tweets found mentioning this token's contract address or ticker\n- This is a RED FLAG for any token claiming strong community".to_string();
    }
    format!(
        "\nTWITTER BUZZ (via X API search — last 7 days, real data):\n\
         - Tweets found: {count}\n\
         - Total likes: {likes}\n\
         - Total retweets: {retweets}\n\
         - Total views/impressions: {views}\n\
         - Total replies: {replies}\n\
         - NOTE: This is organic buzz — real people tweeting about this token's CA or $ticker.\n\
           High tweet count with engagement = genuine community interest.\n\
           Zero tweets on a pumping token = likely coordinated pump with no organic community.",
        count = search.tweet_count,
        likes = search.total_likes,
        retweets = search.total_retweets,
        views = search.total_views,
        replies = search.total_replies,
    )
}

/// Determine which tier to use.
#[derive(Debug)]
enum NarrativeTier {
    /// Tier 1: X API tweet data → pass to OpenAI for holistic judgment ($0.016)
    XTweet { x_context: String },
    /// Tier 1b: X API user data → pass to OpenAI for holistic judgment ($0.011)
    XProfile { x_context: String },
    /// Tier 2: Social links exist but no Twitter URL — OpenAI without web search (~$0.001)
    SocialLinksNoTweet { social_context: String },
    /// Tier 3: No social links — OpenAI with web search ($0.025)
    NoSocialLinks,
}

// ── Main narrative check (3-tier) ────────────────────────

/// 3-tier narrative scoring with X API tweet metrics, OpenAI fallback.
/// Pre-filters with DexScreener + Birdeye to avoid unnecessary API calls on dead tokens.
pub async fn check_narrative(
    client: &reqwest::Client,
    api_key: &str,
    birdeye_api_key: &str,
    x_bearer: &str,
    ctx: &NarrativeContext,
) -> Result<NarrativeResult> {
    let start = std::time::Instant::now();

    // 1. Fetch DexScreener + Birdeye in parallel (no X API calls yet — save $0.0025 on dead tokens)
    let (dex, birdeye) = tokio::join!(
        fetch_dexscreener(client, &ctx.mint),
        fetch_birdeye_overview(client, birdeye_api_key, &ctx.mint),
    );

    // 2. Dead token check — if DexScreener shows zero life, skip everything
    let dex_dead = dex.volume_24h == 0.0
        && dex.maker_count_24h == 0
        && dex.boosts == 0
        && dex.social_urls.is_empty()
        && dex.website_urls.is_empty();
    let birdeye_dead = !birdeye.fetched
        || (birdeye.unique_wallets_24h < 5
            && birdeye.holders < 10
            && !birdeye.has_twitter
            && !birdeye.has_telegram);

    let prefilter_ms = start.elapsed().as_millis() as u64;

    if dex_dead && birdeye_dead {
        let total_ms = start.elapsed().as_millis() as u64;
        info!(
            mint = %ctx.mint,
            name = %ctx.name,
            elapsed_ms = total_ms,
            "🔮 Narrative check SHORT-CIRCUITED — DexScreener + Birdeye both dead (saved X API + OpenAI cost)"
        );
        return Ok(NarrativeResult {
            state: NarrativeState::NoSignal,
            score: 0,
            narrative_strength: "none".to_string(),
            market_strength: "dying".to_string(),
            reasons: vec!["dead_token_prefilter".to_string()],
            risk_flags: vec![],
            web_sources_found: 0,
            tier: "tier_0_dead_market".to_string(),
            prefilter_ms,
            x_api_ms: 0,
            openai_ms: 0,
            total_ms,
            search_ms: 0,
            search_tweet_count: 0,
            diamond_ratio: if birdeye.fetched && birdeye.buy_24h > 0 {
                1.0 - (birdeye.sell_24h as f64 / birdeye.buy_24h.max(1) as f64)
            } else {
                -1.0
            },
        });
    }

    // 3. Token is alive — now fetch Twitter search (deferred past dead-token check to save cost)
    let search_timer = std::time::Instant::now();
    let twitter_search = fetch_twitter_search(client, x_bearer, &ctx.mint, &ctx.symbol).await;
    let search_ms = if twitter_search.searched {
        search_timer.elapsed().as_millis() as u64
    } else {
        0
    };

    // 3. Extract Twitter info and determine tier
    let usernames = collect_twitter_usernames(&dex, &birdeye);
    let has_social_links = !dex.website_urls.is_empty()
        || !dex.social_urls.is_empty()
        || birdeye.has_twitter
        || birdeye.has_telegram
        || birdeye.has_website;

    let x_api_start = std::time::Instant::now();
    let tier = if !x_bearer.is_empty() {
        // Try tweet URL first (best signal: engagement + followers + tweet text)
        if let Some((_tweet_url, tweet_id)) = find_tweet_url(&dex, &birdeye) {
            if let Some(metrics) = fetch_tweet_metrics(client, x_bearer, &tweet_id).await {
                NarrativeTier::XTweet {
                    x_context: build_x_tweet_context(&metrics),
                }
            } else if let Some(username) = usernames.first() {
                // Tweet lookup failed, try user lookup as fallback
                if let Some(user) = fetch_user_metrics(client, x_bearer, username).await {
                    NarrativeTier::XProfile {
                        x_context: build_x_user_context(&user),
                    }
                } else if has_social_links {
                    let social_ctx = build_social_context(&dex, &birdeye, &usernames);
                    NarrativeTier::SocialLinksNoTweet {
                        social_context: social_ctx,
                    }
                } else {
                    NarrativeTier::NoSocialLinks
                }
            } else if has_social_links {
                let social_ctx = build_social_context(&dex, &birdeye, &usernames);
                NarrativeTier::SocialLinksNoTweet {
                    social_context: social_ctx,
                }
            } else {
                NarrativeTier::NoSocialLinks
            }
        } else if let Some(username) = usernames.first() {
            // Profile URL only (no specific tweet)
            if let Some(user) = fetch_user_metrics(client, x_bearer, username).await {
                NarrativeTier::XProfile {
                    x_context: build_x_user_context(&user),
                }
            } else if has_social_links {
                let social_ctx = build_social_context(&dex, &birdeye, &usernames);
                NarrativeTier::SocialLinksNoTweet {
                    social_context: social_ctx,
                }
            } else {
                NarrativeTier::NoSocialLinks
            }
        } else if has_social_links {
            let social_ctx = build_social_context(&dex, &birdeye, &usernames);
            NarrativeTier::SocialLinksNoTweet {
                social_context: social_ctx,
            }
        } else {
            NarrativeTier::NoSocialLinks
        }
    } else if has_social_links {
        // No X bearer token configured — fall through to OpenAI tiers
        let social_ctx = build_social_context(&dex, &birdeye, &usernames);
        NarrativeTier::SocialLinksNoTweet {
            social_context: social_ctx,
        }
    } else {
        NarrativeTier::NoSocialLinks
    };
    let x_api_ms = x_api_start.elapsed().as_millis() as u64;

    // 4. Execute the appropriate tier — all tiers now go through OpenAI
    let search_ctx = build_search_context(&twitter_search);
    let openai_start = std::time::Instant::now();
    let mut result = match &tier {
        NarrativeTier::XTweet { x_context } => {
            // TIER 1: X API tweet data → OpenAI Tier 2 ($0.015 + $0.001 = $0.016)
            info!(
                mint = %ctx.mint,
                name = %ctx.name,
                "🔮⚡ TIER 1 — X API tweet data + OpenAI judgment ($0.016)"
            );
            // Combine X data with regular social context
            let social_ctx = build_social_context(&dex, &birdeye, &usernames);
            let combined = format!("{}\n\n{}", x_context, social_ctx);
            let prompt = build_prompt(ctx, &dex, &birdeye, Some(&combined), &search_ctx, false);
            let mut r = call_openai(client, api_key, &prompt, false).await?;
            r.tier = "tier_1_x_tweet".to_string();
            r
        }

        NarrativeTier::XProfile { x_context } => {
            // TIER 1b: X API profile data → OpenAI Tier 2 ($0.010 + $0.001 = $0.011)
            info!(
                mint = %ctx.mint,
                name = %ctx.name,
                "🔮⚡ TIER 1b — X API profile data + OpenAI judgment ($0.011)"
            );
            let social_ctx = build_social_context(&dex, &birdeye, &usernames);
            let combined = format!("{}\n\n{}", x_context, social_ctx);
            let prompt = build_prompt(ctx, &dex, &birdeye, Some(&combined), &search_ctx, false);
            let mut r = call_openai(client, api_key, &prompt, false).await?;
            r.tier = "tier_1b_x_profile".to_string();
            r
        }

        NarrativeTier::SocialLinksNoTweet { social_context } => {
            // TIER 2: Has social links but no Twitter URL → OpenAI WITHOUT web search (~$0.001)
            info!(
                mint = %ctx.mint,
                name = %ctx.name,
                "🔮💰 TIER 2 — social links found, calling OpenAI without web search (~$0.001)"
            );
            let prompt = build_prompt(
                ctx,
                &dex,
                &birdeye,
                Some(social_context),
                &search_ctx,
                false,
            );
            let mut r = call_openai(client, api_key, &prompt, false).await?;
            r.tier = "tier_2_openai_no_search".to_string();
            r
        }

        NarrativeTier::NoSocialLinks => {
            // TIER 3: No social links → OpenAI WITH web search ($0.025)
            info!(
                mint = %ctx.mint,
                name = %ctx.name,
                "🔮🌐 TIER 3 — no social links, calling OpenAI with web search ($0.025)"
            );
            let prompt = build_prompt(ctx, &dex, &birdeye, None, &search_ctx, true);
            let mut r = call_openai(client, api_key, &prompt, true).await?;
            r.tier = "tier_3_openai_with_search".to_string();
            r
        }
    };
    let openai_ms = openai_start.elapsed().as_millis() as u64;
    let total_ms = start.elapsed().as_millis() as u64;

    result.prefilter_ms = prefilter_ms;
    result.x_api_ms = x_api_ms;
    result.openai_ms = openai_ms;
    result.total_ms = total_ms;
    result.search_ms = search_ms;
    result.search_tweet_count = twitter_search.tweet_count;
    result.diamond_ratio = if birdeye.fetched && birdeye.buy_24h > 0 {
        1.0 - (birdeye.sell_24h as f64 / birdeye.buy_24h.max(1) as f64)
    } else {
        -1.0
    };

    info!(
        mint = %ctx.mint,
        name = %ctx.name,
        score = result.score,
        state = %result.state,
        narrative = %result.narrative_strength,
        market = %result.market_strength,
        sources = result.web_sources_found,
        tier = %result.tier,
        search_tweets = twitter_search.tweet_count,
        total_ms,
        prefilter_ms,
        x_api_ms,
        openai_ms,
        "🔮 Narrative check complete"
    );

    Ok(result)
}

// ── Social context builder ───────────────────────────────────

fn build_social_context(
    dex: &DexScreenerData,
    birdeye: &BirdeyePreFilter,
    usernames: &[String],
) -> String {
    let mut lines = Vec::new();

    if !dex.website_urls.is_empty() || !dex.social_urls.is_empty() {
        lines.push("DexScreener social links:".to_string());
        for (label, url) in &dex.website_urls {
            lines.push(format!("  - {}: {}", label, url));
        }
        for (stype, url) in &dex.social_urls {
            lines.push(format!("  - {} community: {}", stype, url));
        }
    }

    if birdeye.has_twitter || birdeye.has_telegram || birdeye.has_website {
        lines.push("Birdeye social links:".to_string());
        if birdeye.has_twitter {
            lines.push(format!("  - Twitter/X: {}", birdeye.twitter_url));
        }
        if birdeye.has_telegram {
            lines.push(format!("  - Telegram: {}", birdeye.telegram_url));
        }
        if birdeye.has_website {
            lines.push(format!("  - Website: {}", birdeye.website_url));
        }
    }

    if !usernames.is_empty() {
        lines.push(format!(
            "Twitter usernames found: @{}",
            usernames.join(", @")
        ));
    }

    lines.join("\n")
}

// ── Prompt builder ───────────────────────────────────────────

fn build_prompt(
    ctx: &NarrativeContext,
    dex: &DexScreenerData,
    birdeye: &BirdeyePreFilter,
    social_context: Option<&String>,
    search_context: &str,
    include_web_search_task: bool,
) -> String {
    let birdeye_section = if birdeye.fetched {
        let mut lines = vec!["\nBIRDEYE DATA (verified on-chain):".to_string()];
        lines.push(format!(
            "- Unique wallets (24h): {}",
            birdeye.unique_wallets_24h
        ));
        lines.push(format!("- Holders: {}", birdeye.holders));
        lines.push(format!(
            "- 24h buys: {}, 24h sells: {}",
            birdeye.buy_24h, birdeye.sell_24h
        ));
        if birdeye.buy_24h > 0 {
            let diamond = 1.0 - (birdeye.sell_24h as f64 / birdeye.buy_24h.max(1) as f64);
            lines.push(format!("- Diamond ratio (approx): {:.0}% (% of buyers still holding — higher = stronger conviction)", diamond * 100.0));
        }
        if birdeye.has_twitter {
            lines.push(format!(
                "- Twitter/X: {} (VERIFIED — token metadata)",
                birdeye.twitter_url
            ));
        }
        if birdeye.has_telegram {
            lines.push(format!(
                "- Telegram: {} (VERIFIED — token metadata)",
                birdeye.telegram_url
            ));
        }
        if birdeye.has_website {
            lines.push(format!(
                "- Website: {} (from token metadata)",
                birdeye.website_url
            ));
        }
        if !birdeye.has_twitter && !birdeye.has_telegram && !birdeye.has_website {
            lines.push("- No social links in token metadata".to_string());
        }
        lines.join("\n")
    } else {
        String::new()
    };

    let social_section = if let Some(ctx_str) = social_context {
        format!("\nSOCIAL LINKS (from DexScreener + Birdeye — already verified):\n{ctx_str}")
    } else {
        String::new()
    };

    let task_section = if include_web_search_task {
        format!(
            r#"TASK:
1. Search the web for "{mint}" and "{name} {symbol} solana token" and "{name} crypto" to find social media mentions, news articles, Twitter/X posts, Telegram buzz, or trending status.
2. Also search for "{name} CTO" or "{name} community takeover" — a CTO is when the dev sells all tokens and the community takes over. This is BULLISH in meme coins (no dev dump risk, community-driven).
3. Score the token holistically — weighing on-chain strength, social/narrative presence, and market data together as a SINGLE decision."#,
            mint = ctx.mint,
            name = ctx.name,
            symbol = ctx.symbol
        )
    } else {
        "TASK:\nBased on the on-chain metrics, market data, and social links provided above, score this token holistically. You do NOT need to search the web — all data is provided. Evaluate the strength of the social links (who posted, community size) alongside the on-chain data.".to_string()
    };

    format!(
        r#"You are a meme coin analyst deciding whether a Solana token is worth holding as a "moonbag" — a small long-term position kept after taking initial profit, betting it could 5-50x from here.

Your job: evaluate BOTH on-chain health AND social/narrative momentum together, then give a single holistic score. This score directly determines whether we keep holding or sell everything.

TOKEN INFO:
- Name: {name}
- Symbol: {symbol}
- Mint address: {mint}

ON-CHAIN METRICS (live, last 60 seconds):
- Current price: ${price:.10}
- Entry price: ${entry:.10} (we bought here)
- Peak multiplier: {peak:.2}x (highest price / entry)
- Hold time: {hold_secs}s since we bought
- Recent buys: {buys}, Recent sells: {sells}
- Buy volume: {buy_vol:.3} SOL, Sell volume: {sell_vol:.3} SOL
- Momentum ratio (buy_vol/total_vol): {momentum:.2} (>0.65 = strong buying, <0.35 = dumping)

DEXSCREENER MARKET DATA:
- 24h volume: ${vol_24h:.0}
- 24h transactions: {makers}
- Fully diluted valuation: ${fdv:.0}
- Liquidity (USD): ${liq:.0}
- 5-minute price change: {pc5m:.1}%
- 1-hour price change: {pc1h:.1}%
- Active boosts: {boosts}
{birdeye_data}
{social_links}
{twitter_buzz}

{task}

IMPORTANT SCORING PRINCIPLES:
- On-chain metrics and narrative are BOTH important but EITHER can carry the score alone.
- Strong social buzz with weak on-chain: the hype hasn't translated to buys YET but could explode. Score 40-65 depending on buzz quality.
- Strong on-chain with no social presence: organic demand, the market is voting with real money. Score 50-75 depending on flow strength.
- Both strong: clear runner. Score 70-100.
- Both weak: dead token. Score 0-25.
- CTO tokens with active communities: score at least 55+.
- TWITTER/X DATA: When X API data is provided, focus on the ACCOUNT’S SIZE and CREDIBILITY (follower count, verified status), NOT the tweet text. Token devs often link to any tweet from an influencer to signal association — the tweet itself may be unrelated. A 500K+ follower account linked to a token is a strong signal regardless of what the tweet says. A <1K follower account is weak even if the tweet mentions the token directly.
- A token linked to a major account (100K+ followers) with decent on-chain flow is a VERY strong signal — score at least 65+.
- Conversely, 50+ buys with 5+ SOL volume and >1.5 momentum in 60 seconds is exceptional flow even if nobody is talking about it yet.

Respond with ONLY a JSON object (no markdown, no explanation):
{{
  "score": <0-100 integer>,
  "narrative_strength": "<none|weak|moderate|strong|viral>",
  "market_strength": "<dying|weak|moderate|strong|explosive>",
  "reasons": ["<reason1>", "<reason2>", "<reason3>"],
  "risk_flags": ["<flag1>"],
  "web_sources_found": <0-10 integer count of distinct web sources mentioning this token>
}}

SCORING GUIDE:
- 0-25 (NO_SIGNAL): No web presence, weak flow, no reason to hold
- 26-50 (EARLY_ATTENTION): Some mentions OR decent flow, but not convincing enough alone
- 51-75 (EXPANDING_ATTENTION): Multiple social sources + solid flow, OR one side is very strong
- 76-100 (RUNNER_CONFIRMED): Viral narrative + explosive flow, strong conviction hold"#,
        name = ctx.name,
        symbol = ctx.symbol,
        mint = ctx.mint,
        price = ctx.current_price_usd,
        entry = ctx.entry_price_usd,
        peak = ctx.peak_multiplier,
        hold_secs = ctx.hold_seconds,
        buys = ctx.buy_count,
        sells = ctx.sell_count,
        buy_vol = ctx.buy_volume_sol,
        sell_vol = ctx.sell_volume_sol,
        momentum = ctx.momentum_ratio,
        vol_24h = dex.volume_24h,
        makers = dex.maker_count_24h,
        fdv = dex.fdv_usd,
        liq = dex.liquidity_usd,
        pc5m = dex.price_change_5m,
        pc1h = dex.price_change_1h,
        boosts = dex.boosts,
        birdeye_data = birdeye_section,
        social_links = social_section,
        twitter_buzz = search_context,
        task = task_section,
    )
}

// ── OpenAI caller ────────────────────────────────────────────

/// Call OpenAI Responses API. `use_web_search` controls whether web_search_preview
/// tool is included (Tier 3: $0.025) or omitted (Tier 2: ~$0.001).
async fn call_openai(
    client: &reqwest::Client,
    api_key: &str,
    prompt: &str,
    use_web_search: bool,
) -> Result<NarrativeResult> {
    let tools = if use_web_search {
        serde_json::json!([{ "type": "web_search_preview" }])
    } else {
        serde_json::json!([])
    };

    let request_body = serde_json::json!({
        "model": "gpt-4o-mini",
        "tools": tools,
        "input": prompt,
        "text": {
            "format": { "type": "text" }
        }
    });

    let resp = client
        .post("https://api.openai.com/v1/responses")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(30))
        .json(&request_body)
        .send()
        .await
        .context("OpenAI API request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "OpenAI API returned HTTP {}: {}",
            status,
            body
        ));
    }

    let response_json: serde_json::Value = resp
        .json()
        .await
        .context("Failed to parse OpenAI response JSON")?;

    let text_content = response_json
        .get("output")
        .and_then(|o| o.as_array())
        .and_then(|arr| {
            arr.iter().find_map(|item| {
                if item.get("type").and_then(|t| t.as_str()) == Some("message") {
                    item.get("content")
                        .and_then(|c| c.as_array())
                        .and_then(|content_arr| {
                            content_arr.iter().find_map(|c| {
                                if c.get("type").and_then(|t| t.as_str()) == Some("output_text") {
                                    c.get("text")
                                        .and_then(|t| t.as_str())
                                        .map(|s| s.to_string())
                                } else {
                                    None
                                }
                            })
                        })
                } else {
                    None
                }
            })
        })
        .unwrap_or_default();

    Ok(parse_narrative_response(&text_content))
}

/// Parse OpenAI's text response into a NarrativeResult.
/// Tolerant of markdown fences and minor formatting issues.
fn parse_narrative_response(text: &str) -> NarrativeResult {
    // Strip markdown code fences if present
    let cleaned = text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    // Try to find JSON object in the text
    let json_str = if let Some(start) = cleaned.find('{') {
        if let Some(end) = cleaned.rfind('}') {
            &cleaned[start..=end]
        } else {
            cleaned
        }
    } else {
        cleaned
    };

    match serde_json::from_str::<serde_json::Value>(json_str) {
        Ok(v) => {
            let score = v
                .get("score")
                .and_then(|s| s.as_u64())
                .map(|s| s.min(100) as u8)
                .unwrap_or(0);

            NarrativeResult {
                state: NarrativeState::from_score(score),
                score,
                narrative_strength: v
                    .get("narrative_strength")
                    .and_then(|s| s.as_str())
                    .unwrap_or("none")
                    .to_string(),
                market_strength: v
                    .get("market_strength")
                    .and_then(|s| s.as_str())
                    .unwrap_or("weak")
                    .to_string(),
                reasons: v
                    .get("reasons")
                    .and_then(|r| r.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|r| r.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                risk_flags: v
                    .get("risk_flags")
                    .and_then(|r| r.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|r| r.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                web_sources_found: v
                    .get("web_sources_found")
                    .and_then(|s| s.as_u64())
                    .map(|s| s.min(255) as u8)
                    .unwrap_or(0),
                tier: String::new(), // set by caller
                prefilter_ms: 0,
                x_api_ms: 0,
                openai_ms: 0,
                total_ms: 0,
                search_ms: 0,
                search_tweet_count: 0,
                diamond_ratio: -1.0,
            }
        }
        Err(e) => {
            warn!(
                "Failed to parse narrative response as JSON: {} — text: {}",
                e, text
            );
            NarrativeResult {
                state: NarrativeState::NoSignal,
                score: 0,
                narrative_strength: "none".to_string(),
                market_strength: "weak".to_string(),
                reasons: vec!["parse_error".to_string()],
                risk_flags: vec![],
                web_sources_found: 0,
                tier: String::new(), // set by caller
                prefilter_ms: 0,
                x_api_ms: 0,
                openai_ms: 0,
                total_ms: 0,
                search_ms: 0,
                search_tweet_count: 0,
                diamond_ratio: -1.0,
            }
        }
    }
}
