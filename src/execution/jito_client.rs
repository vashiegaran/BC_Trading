use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    system_instruction,
    transaction::VersionedTransaction,
};
use std::str::FromStr;
use tracing::{debug, info, warn};

/// Jito tip accounts (randomly select one for each transaction)
/// These are the official Jito tip accounts as of 2024
const JITO_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// Minimum tip in lamports (0.0001 SOL)
const MIN_TIP_LAMPORTS: u64 = 100_000;

/// Default tip in lamports if API fails (0.0005 SOL)
const DEFAULT_TIP_LAMPORTS: u64 = 500_000;

/// Maximum retries for bundle status checks
const MAX_BUNDLE_STATUS_RETRIES: u32 = 30;

/// Delay between bundle status checks (milliseconds)
const BUNDLE_STATUS_POLL_MS: u64 = 400;

#[derive(Debug, Serialize)]
struct SendBundleRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct SendBundleResponse {
    result: String,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Serialize)]
struct GetBundleStatusesRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct GetBundleStatusesResponse {
    result: BundleStatusResult,
}

#[derive(Debug, Deserialize)]
struct BundleStatusResult {
    value: Vec<BundleStatus>,
}

#[derive(Debug, Deserialize)]
struct BundleStatus {
    confirmation_status: Option<String>,
    #[serde(default)]
    err: Option<serde_json::Value>,
}

pub struct JitoClient {
    client: Client,
    block_engine_url: String,
    tip_multiplier: f64,
    max_tip_lamports: u64,
}

impl JitoClient {
    /// Create a new Jito client with connection pool configured for reuse
    pub fn new(block_engine_url: String, tip_multiplier: f64, max_tip_sol: f64) -> Self {
        let max_tip_lamports = (max_tip_sol * 1_000_000_000.0) as u64;

        let client = Client::builder()
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .pool_max_idle_per_host(4)
            .build()
            .expect("Failed to build Jito HTTP client");

        Self {
            client,
            block_engine_url,
            tip_multiplier,
            max_tip_lamports,
        }
    }

    /// Get a random Jito tip account
    fn get_random_tip_account() -> Result<Pubkey> {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();

        let tip_account_str = JITO_TIP_ACCOUNTS
            .choose(&mut rng)
            .ok_or_else(|| anyhow!("Failed to select random tip account"))?;

        Pubkey::from_str(tip_account_str)
            .map_err(|e| anyhow!("Failed to parse tip account: {}", e))
    }

    /// Calculate tip amount in lamports
    ///
    /// Fetches recommended tip from Jito API, applies multiplier, and caps at max
    pub async fn calculate_tip(&self) -> u64 {
        // Try to get recommended tip from Jito
        let recommended_tip = self.get_recommended_tip().await.unwrap_or_else(|e| {
            warn!("Failed to get Jito tip recommendation: {}, using default", e);
            DEFAULT_TIP_LAMPORTS
        });

        // Apply multiplier
        let calculated_tip = (recommended_tip as f64 * self.tip_multiplier) as u64;

        // Cap at max and ensure minimum
        let final_tip = calculated_tip
            .max(MIN_TIP_LAMPORTS)
            .min(self.max_tip_lamports);

        debug!(
            recommended = recommended_tip,
            calculated = calculated_tip,
            final_tip = final_tip,
            "Jito tip calculated"
        );

        final_tip
    }

    /// Get recommended tip from Jito API (getTipAccounts endpoint)
    /// Returns tip in lamports
    pub async fn get_recommended_tip(&self) -> Result<u64> {
        // For now, return a conservative default
        // In production, you would call the Jito API endpoint
        // GET https://bundles.jito.wtf/api/v1/bundles/tip_floor

        let url = format!("{}/api/v1/bundles/tip_floor",
            self.block_engine_url.trim_end_matches("/api/v1"));

        match self.client
            .get(&url)
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                    if let Some(lamports) = data.get("result").and_then(|v| v.as_u64()) {
                        return Ok(lamports);
                    }
                }
                Ok(DEFAULT_TIP_LAMPORTS)
            }
            _ => Ok(DEFAULT_TIP_LAMPORTS),
        }
    }

    /// Create a tip transfer instruction
    pub fn create_tip_instruction(
        payer: &Pubkey,
        tip_lamports: u64,
    ) -> Result<Instruction> {
        let tip_account = Self::get_random_tip_account()?;

        debug!(
            payer = %payer,
            tip_account = %tip_account,
            tip_lamports = tip_lamports,
            tip_sol = format!("{:.6}", tip_lamports as f64 / 1_000_000_000.0),
            "Creating Jito tip instruction"
        );

        Ok(system_instruction::transfer(payer, &tip_account, tip_lamports))
    }

    /// Submit a bundle of transactions to Jito
    ///
    /// Returns the bundle ID for status tracking
    pub async fn send_bundle(
        &self,
        transactions: Vec<VersionedTransaction>,
    ) -> Result<String> {
        // Serialize transactions to base58
        let tx_strings: Vec<String> = transactions
            .iter()
            .map(|tx| {
                let serialized = bincode::serialize(tx)
                    .map_err(|e| anyhow!("Failed to serialize transaction: {}", e))?;
                Ok(bs58::encode(serialized).into_string())
            })
            .collect::<Result<Vec<_>>>()?;

        let request = SendBundleRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "sendBundle".to_string(),
            params: vec![tx_strings],
        };

        let bundle_url = format!("{}/api/v1/bundles", self.block_engine_url.trim_end_matches('/'));

        debug!(
            bundle_size = transactions.len(),
            url = %bundle_url,
            "Submitting bundle to Jito"
        );

        let response = self
            .client
            .post(&bundle_url)
            .json(&request)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| anyhow!("Failed to send bundle request: {}", e))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| anyhow!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(anyhow!("Jito bundle submission failed: HTTP {} — {}", status, body));
        }

        let response_data: SendBundleResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse Jito response: {} — Body: {}", e, body))?;

        if let Some(error) = response_data.error {
            return Err(anyhow!(
                "Jito bundle submission error: {} (code {})",
                error.message,
                error.code
            ));
        }

        info!(bundle_id = %response_data.result, "Bundle submitted to Jito");
        Ok(response_data.result)
    }

    /// Check the status of a submitted bundle
    ///
    /// Returns true if the bundle was confirmed, false if still pending
    pub async fn check_bundle_status(&self, bundle_id: &str) -> Result<bool> {
        let request = GetBundleStatusesRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "getBundleStatuses".to_string(),
            params: vec![vec![bundle_id.to_string()]],
        };

        let bundle_url = format!("{}/api/v1/bundles", self.block_engine_url.trim_end_matches('/'));

        let response = self
            .client
            .post(&bundle_url)
            .json(&request)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| anyhow!("Failed to get bundle status: {}", e))?;

        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!("Bundle status check failed: HTTP {}", status));
        }

        let response_data: GetBundleStatusesResponse = response
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse bundle status response: {}", e))?;

        if let Some(bundle_status) = response_data.result.value.first() {
            if let Some(status) = &bundle_status.confirmation_status {
                debug!(bundle_id = %bundle_id, status = %status, "Bundle status");

                // Check if bundle failed
                if bundle_status.err.is_some() {
                    return Err(anyhow!("Bundle failed: {:?}", bundle_status.err));
                }

                // Consider "processed", "confirmed", or "finalized" as success
                return Ok(status == "processed" || status == "confirmed" || status == "finalized");
            }
        }

        Ok(false) // Still pending
    }

    /// Submit a bundle and wait for confirmation
    ///
    /// This is a convenience method that combines send_bundle and polling for status
    pub async fn send_bundle_and_wait(
        &self,
        transactions: Vec<VersionedTransaction>,
        timeout_secs: u64,
    ) -> Result<String> {
        let bundle_id = self.send_bundle(transactions).await?;

        let max_iterations = (timeout_secs * 1000) / BUNDLE_STATUS_POLL_MS;
        let mut iterations = 0;

        while iterations < max_iterations {
            tokio::time::sleep(tokio::time::Duration::from_millis(BUNDLE_STATUS_POLL_MS)).await;
            iterations += 1;

            match self.check_bundle_status(&bundle_id).await {
                Ok(true) => {
                    info!(
                        bundle_id = %bundle_id,
                        elapsed_ms = iterations * BUNDLE_STATUS_POLL_MS,
                        "✅ Bundle confirmed"
                    );
                    return Ok(bundle_id);
                }
                Ok(false) => {
                    debug!(bundle_id = %bundle_id, "Bundle still pending...");
                    continue;
                }
                Err(e) => {
                    warn!(bundle_id = %bundle_id, "Bundle status check failed: {}", e);
                    // Don't fail immediately, keep trying
                }
            }
        }

        Err(anyhow!(
            "Bundle confirmation timeout after {}s. Bundle ID: {}",
            timeout_secs,
            bundle_id
        ))
    }
}
