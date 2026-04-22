//! Fallback RPC helper for 429 (rate-limited) Solana RPC errors.
//!
//! When the primary QuickNode RPC returns HTTP 429, we retry the same
//! call against Solana's free public endpoint.  This endpoint is
//! slower (500–2 000 ms) but has no hard rate limit.

use solana_client::client_error::ClientError;

/// Returns `true` if the [`ClientError`] represents an HTTP 429
/// (Too Many Requests) rate-limit response.
pub fn is_rate_limited(err: &ClientError) -> bool {
    let msg = err.to_string();
    msg.contains("429") || msg.contains("Too Many Requests")
}
