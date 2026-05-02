use serde::{Deserialize, Deserializer, Serialize};

use crate::detection::types::{GraduatedToken, PipelineTiming};

/// Deserializes a field that may be explicitly `null` or missing as `T::default()`.
fn deserialize_null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

// ─── Single filter check result ──────────────────────────────

/// Result of one filter check (e.g. rugcheck, liquidity, holders …).
#[derive(Debug, Clone, Serialize)]
pub struct FilterResult {
    /// Name of the check (used in logs and Supabase).
    pub check_name: String,
    /// Whether the token passed this check.
    pub passed: bool,
    /// Human-readable reason for failure (`None` when passed).
    pub fail_reason: Option<String>,
}

impl FilterResult {
    pub fn pass(check_name: &str) -> Self {
        Self {
            check_name: check_name.to_string(),
            passed: true,
            fail_reason: None,
        }
    }

    pub fn fail(check_name: &str, reason: &str) -> Self {
        Self {
            check_name: check_name.to_string(),
            passed: false,
            fail_reason: Some(reason.to_string()),
        }
    }
}

// ─── Aggregate summary ──────────────────────────────────────

/// Aggregated results from **all** filter checks for one token.
#[derive(Debug, Clone, Serialize)]
pub struct FilterSummary {
    pub results: Vec<FilterResult>,
    pub overall_passed: bool,
}

impl FilterSummary {
    pub fn from_results(results: Vec<FilterResult>) -> Self {
        let overall_passed = results.iter().all(|r| r.passed);
        Self {
            results,
            overall_passed,
        }
    }

    /// Return only the checks that failed.
    pub fn failed_checks(&self) -> Vec<&FilterResult> {
        self.results.iter().filter(|r| !r.passed).collect()
    }
}

// ─── Token that passed all filters ──────────────────────────

/// A graduated token that has **passed all filter checks** and is
/// ready for the execution engine.
#[derive(Debug, Clone)]
pub struct FilteredToken {
    /// The original graduated token from the detection engine.
    pub event: GraduatedToken,
    /// Summary of every check (pass and fail) for logging.
    pub filter_summary: FilterSummary,
    // ── Extra enrichment data (for logging / downstream) ─────
    pub market_cap_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub rugcheck_score: Option<f64>,
    /// Token price in USD at filter time (from DexScreener via market_cap filter).
    /// Used by the anti-chase check in the execution engine.
    pub filter_price_usd: Option<f64>,
    /// Accumulated pipeline timing data (carried from GraduatedToken).
    pub pipeline_timing: PipelineTiming,
    /// True when this token was injected by the re-entry watcher (not the
    /// detection → filter pipeline). Causes the dedup gate to bypass the
    /// post-exit cooldown — the watcher's own peak/dip/cap gates provide the
    /// equivalent protection. Open-position dedup still applies.
    pub is_reentry: bool,
}

// ─── RugCheck API response types ────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RugCheckReport {
    pub score: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub risks: Vec<RugCheckRiskItem>,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub markets: Vec<RugCheckMarket>,
    pub token: Option<RugCheckTokenInfo>,
    #[serde(rename = "topHolders", default, deserialize_with = "deserialize_null_as_default")]
    pub top_holders: Vec<RugCheckHolder>,
    #[serde(default)]
    pub bundled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct RugCheckRiskItem {
    pub name: Option<String>,
    pub description: Option<String>,
    pub level: Option<String>,
    pub score: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct RugCheckTokenInfo {
    #[serde(rename = "mintAuthority")]
    pub mint_authority: Option<String>,
    #[serde(rename = "freezeAuthority")]
    pub freeze_authority: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RugCheckMarket {
    #[serde(rename = "liquidityLocked", default)]
    pub liquidity_locked: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct RugCheckHolder {
    pub address: Option<String>,
    pub pct: Option<f64>,
}
