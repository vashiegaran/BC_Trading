pub mod types;

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use tracing::info;

/// Supabase REST API client.
///
/// Uses reqwest with pre-configured headers for apikey, Authorization,
/// and Content-Type. All Supabase writes go through this client.
#[derive(Debug, Clone)]
pub struct SupabaseClient {
    pub client: Client,
    pub base_url: String, // e.g. "https://xxx.supabase.co/rest/v1"
}

impl SupabaseClient {
    /// Build and return a `SupabaseClient`, then verify the connection by
    /// doing a lightweight SELECT against the `system_events` table.
    ///
    /// Panics with a clear message if the connection test fails.
    pub async fn init(supabase_url: &str, service_key: &str) -> Self {
        // ── Build default headers ───────────────────────────
        let mut headers = HeaderMap::new();

        headers.insert(
            "apikey",
            HeaderValue::from_str(service_key).expect("INVALID SUPABASE_SERVICE_KEY: not valid as an HTTP header value"),
        );
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", service_key))
                .expect("INVALID SUPABASE_SERVICE_KEY: cannot build Authorization header"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "Prefer",
            HeaderValue::from_static("return=minimal"),
        );

        let client = Client::builder()
            .default_headers(headers)
            .build()
            .expect("FAILED TO BUILD reqwest HTTP client");

        let base_url = format!("{}/rest/v1", supabase_url.trim_end_matches('/'));

        let sc = Self { client, base_url };

        // ── Test connection ────────────────────────────────
        sc.test_connection().await;

        info!("Supabase connection verified successfully");
        sc
    }

    /// Lightweight health-check: SELECT from `system_events` with limit 1.
    /// Panics with diagnostic info on failure.
    async fn test_connection(&self) {
        let url = format!("{}{}",self.base_url, "/system_events?select=id&limit=1");

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "SUPABASE CONNECTION FAILED: could not reach `{}`.\n\
                     Error: {}\n\
                     Check that SUPABASE_URL is correct and your network is available.",
                    url, e
                );
            });

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            panic!(
                "SUPABASE CONNECTION FAILED: GET `{}` returned HTTP {}.\n\
                 Response body: {}\n\
                 Ensure the `system_events` table exists (run the SQL from SUPABASE.md) \
                 and SUPABASE_SERVICE_KEY is correct.",
                url, status, body
            );
        }
    }
}
