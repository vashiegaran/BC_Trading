use anyhow::{anyhow, Result};
use reqwest::Client;
use solana_sdk::transaction::VersionedTransaction;
use tracing::{debug, info, warn};

pub struct HeliusSenderClient {
    client: Client,
    sender_url: String,
}

impl HeliusSenderClient {
    pub fn new(sender_url: String) -> Self {
        let client = Client::builder()
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .pool_max_idle_per_host(4)
            .build()
            .expect("Failed to build TX sender HTTP client");

        Self { client, sender_url }
    }

    /// Submit a signed transaction via JSON-RPC `sendTransaction`.
    ///
    /// Works with any RPC endpoint (Chainstack Warp, Helius Sender, etc.).
    /// Chainstack Growth auto-routes through bloXroute SwQoS + Jito.
    pub async fn send_transaction(
        &self,
        signed_tx: &VersionedTransaction,
    ) -> Result<solana_sdk::signature::Signature> {
        let tx_bytes = bincode::serialize(signed_tx)
            .map_err(|e| anyhow!("Failed to serialize tx: {}", e))?;
        let tx_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &tx_bytes,
        );

        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": chrono::Utc::now().timestamp_millis().to_string(),
            "method": "sendTransaction",
            "params": [
                tx_b64,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        });

        debug!(url = %self.sender_url, "Submitting tx via Warp TX sender");

        let resp = self
            .client
            .post(&self.sender_url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| anyhow!("TX sender request failed: {}", e))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse TX sender response: {}", e))?;

        if let Some(error) = body.get("error") {
            return Err(anyhow!(
                "TX sender error: {} (HTTP {})",
                error,
                status
            ));
        }

        let sig_str = body
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing 'result' in TX sender response: {}", body))?;

        info!(sig = %sig_str, "📤 Transaction submitted via Warp TX sender");

        sig_str
            .parse()
            .map_err(|e| anyhow!("Failed to parse signature '{}': {}", sig_str, e))
    }

    /// Warm the connection by sending a lightweight request.
    pub async fn warm_connection(&self) {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "warmup",
            "method": "getHealth"
        });
        match self.client.post(&self.sender_url).json(&payload).send().await {
            Ok(_) => info!("⚡ TX sender connection pre-warmed"),
            Err(e) => warn!("TX sender ping failed (non-fatal): {}", e),
        }
    }
}

/// Fetch priority fee estimate using the standard Solana RPC method
/// `getRecentPrioritizationFees`. Works with any RPC (Chainstack, Helius, etc.).
///
/// Returns the median recent fee for the given accounts, capped at `fallback_lamports`.
/// Falls back to `fallback_lamports` if the API call fails.
pub async fn get_priority_fee_estimate(
    rpc_url: &str,
    account_keys: &[&str],
    _priority_level: &str,
    fallback_lamports: u64,
) -> u64 {
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "priority-fee",
        "method": "getRecentPrioritizationFees",
        "params": [
            account_keys
        ]
    });

    let result = client
        .post(rpc_url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await;

    match result {
        Ok(resp) => {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if let Some(fees) = body.get("result").and_then(|r| r.as_array()) {
                    // Extract non-zero fees, compute percentile 75 (aggressive)
                    let mut fee_values: Vec<u64> = fees
                        .iter()
                        .filter_map(|f| f.get("prioritizationFee").and_then(|v| v.as_u64()))
                        .filter(|&f| f > 0)
                        .collect();
                    if fee_values.is_empty() {
                        debug!("getRecentPrioritizationFees: all zero — using fallback");
                        return fallback_lamports;
                    }
                    fee_values.sort_unstable();
                    let p75_idx = (fee_values.len() * 3) / 4;
                    let p75_fee = fee_values[p75_idx.min(fee_values.len() - 1)];
                    debug!(
                        p75_fee,
                        sample_count = fee_values.len(),
                        "Priority fee estimate (p75 of recent fees)"
                    );
                    return p75_fee;
                }
            }
            warn!("getRecentPrioritizationFees: unexpected response — using fallback");
            fallback_lamports
        }
        Err(e) => {
            warn!("getRecentPrioritizationFees failed: {} — using fallback", e);
            fallback_lamports
        }
    }
}


