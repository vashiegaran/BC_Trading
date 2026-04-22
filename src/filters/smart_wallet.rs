use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use super::rpc_fallback::is_rate_limited;
use super::types::FilterResult;

const CHECK_NAME: &str = "holder_quality";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);

/// SPL Token program ID.
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// SPL Token-2022 program ID (used by PumpFun tokens).
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// How many top holders to analyse.
const TOP_N: usize = 10;

/// Max recent signatures to fetch per wallet (cheap RPC call).
const SIG_LIMIT: usize = 15;

/// Wallets with fewer than this many historical transactions are suspicious.
const MIN_TX_COUNT: usize = 5;

/// Wallets whose oldest visible tx is less than this many seconds ago are suspicious.
const MIN_WALLET_AGE_SECS: i64 = 3600; // 1 hour

/// If more than this fraction of analysed top holders are suspicious, reject.
/// 0.6 → reject when 6+ of 10 holders are fresh/bot wallets.
const MAX_SUSPICIOUS_RATIO: f64 = 0.6;

/// If this many (or more) top holders share the same funder, it's a wash-trade ring.
const SAME_FUNDER_THRESHOLD: usize = 4;

/// Granular metrics from the smart wallet analysis, for pattern logging.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SmartWalletMetrics {
    pub total_scored: usize,
    pub suspicious_count: usize,
    pub genuine_count: usize,
    pub suspicious_ratio: f64,
    pub max_same_funder_count: usize,
    pub avg_tx_count: f64,
    pub avg_wallet_age_secs: f64,
    pub min_wallet_age_secs: i64,
    /// Diagnostic reason when analysis bailed out early (None = completed normally).
    pub diagnostic_reason: Option<String>,
}

impl SmartWalletMetrics {
    /// Create a zeroed-out metrics struct with a diagnostic reason for bail-outs.
    fn bail(reason: &str) -> Self {
        Self {
            total_scored: 0,
            suspicious_count: 0,
            genuine_count: 0,
            suspicious_ratio: 0.0,
            max_same_funder_count: 0,
            avg_tx_count: 0.0,
            avg_wallet_age_secs: 0.0,
            min_wallet_age_secs: 0,
            diagnostic_reason: Some(reason.to_string()),
        }
    }
}

/// Known program / system addresses that own token accounts but are NOT real
/// holders (AMM pools, system program, etc.).  These are skipped.
fn is_known_program(pubkey: &Pubkey) -> bool {
    let s = pubkey.to_string();
    s == "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"   // Raydium AMM V4
        || s == "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" // Raydium LP V4
        || s == "39azUYFWPz3VHgKCf3VChY6SkC8DJ9W3jDBqPxzHMbbL" // Raydium Authority V4
        || s == "11111111111111111111111111111111"               // System Program
        || s == TOKEN_PROGRAM                                    // SPL Token
        || s == "So11111111111111111111111111111111111111112"     // Wrapped SOL
}

pub struct SmartWalletFilter;

impl SmartWalletFilter {
    pub fn new() -> Self {
        Self
    }

    /// Analyse the top holders of a token to determine if they are genuine.
    ///
    /// **Flow**:
    /// 1. `getTokenLargestAccounts` → top N token accounts (ATAs).
    /// 2. `getMultipleAccounts`     → parse owner wallet from each ATA (bytes 32-64).
    /// 3. `getSignaturesForAddress` per wallet (**parallel**) → tx count + wallet age.
    /// 4. Score: if > 60 % of analysed holders are fresh / low-activity → **reject**.
    ///
    /// Returns **pass** on any RPC failure (graceful degradation — never blocks buys).
    pub async fn check(&self, mint: &str, cfg: &AppConfig) -> (FilterResult, Option<SmartWalletMetrics>) {
        let mint_pubkey = match Pubkey::from_str(mint) {
            Ok(pk) => pk,
            Err(_) => {
                warn!(mint, "holder_quality: invalid mint pubkey — skipping");
                return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("invalid_mint_pubkey")));
            }
        };

        // Prefer Helius RPC (higher rate limits), fall back to primary
        let rpc_url = cfg.env.helius_rpc_url.clone()
            .unwrap_or_else(|| cfg.env.solana_rpc_url.clone());
        let backup_url = cfg.env.solana_rpc_backup_url.clone();

        let rpc = Arc::new(RpcClient::new_with_timeout(rpc_url.clone(), REQUEST_TIMEOUT));

        // ── Step 1: Fetch top N holder token accounts ──────────────────
        let largest = match rpc.get_token_largest_accounts(&mint_pubkey).await {
            Ok(a) => a,
            Err(e) if is_rate_limited(&e) => {
                let fb = RpcClient::new_with_timeout(backup_url.clone(), REQUEST_TIMEOUT);
                match fb.get_token_largest_accounts(&mint_pubkey).await {
                    Ok(a) => a,
                    Err(e2) => {
                        warn!(mint, error = %e2, "holder_quality: Step 1 getTokenLargestAccounts failed on BOTH RPCs");
                        return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("step1_largest_accounts_both_rpc_failed")));
                    }
                }
            }
            Err(e) => {
                warn!(mint, error = %e, "holder_quality: Step 1 getTokenLargestAccounts failed");
                return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("step1_largest_accounts_failed")));
            }
        };

        if largest.is_empty() {
            warn!(mint, "holder_quality: Step 1 returned EMPTY largest accounts list");
            return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("step1_empty_largest_accounts")));
        }

        let ata_pubkeys: Vec<Pubkey> = largest
            .iter()
            .take(TOP_N)
            .filter_map(|acct| Pubkey::from_str(&acct.address).ok())
            .collect();

        if ata_pubkeys.is_empty() {
            warn!(mint, "holder_quality: no valid ATA pubkeys after parsing largest accounts");
            return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("no_valid_ata_pubkeys")));
        }

        // ── Step 2: Batch-fetch token accounts → extract owner wallets ─
        let accounts = match rpc.get_multiple_accounts(&ata_pubkeys).await {
            Ok(a) => a,
            Err(e) if is_rate_limited(&e) => {
                let fb = RpcClient::new_with_timeout(backup_url.clone(), REQUEST_TIMEOUT);
                match fb.get_multiple_accounts(&ata_pubkeys).await {
                    Ok(a) => a,
                    Err(e2) => {
                        warn!(mint, error = %e2, "holder_quality: Step 2 getMultipleAccounts failed on BOTH RPCs");
                        return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("step2_get_accounts_both_rpc_failed")));
                    }
                }
            }
            Err(e) => {
                warn!(mint, error = %e, "holder_quality: Step 2 getMultipleAccounts failed");
                return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("step2_get_accounts_failed")));
            }
        };

        let token_program = Pubkey::from_str(TOKEN_PROGRAM).unwrap();
        let token_2022_program = Pubkey::from_str(TOKEN_2022_PROGRAM).unwrap();

        let mut owner_wallets: Vec<Pubkey> = Vec::with_capacity(TOP_N);
        for account_opt in &accounts {
            if let Some(account) = account_opt {
                // SPL token account: bytes 32..64 = owner pubkey
                // Accept both standard SPL Token and Token-2022 (PumpFun) accounts
                if account.data.len() >= 64
                    && (account.owner == token_program || account.owner == token_2022_program)
                {
                    let owner = Pubkey::try_from(&account.data[32..64]).unwrap_or_default();
                    if !is_known_program(&owner) {
                        owner_wallets.push(owner);
                    }
                }
            }
        }

        // Dedup (same wallet may own multiple token accounts)
        owner_wallets.sort();
        owner_wallets.dedup();

        if owner_wallets.is_empty() {
            warn!(
                mint,
                ata_count = ata_pubkeys.len(),
                accounts_returned = accounts.len(),
                "holder_quality: no valid owner wallets after filtering known programs — all top holders are AMM/system accounts"
            );
            return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("all_owners_are_known_programs")));
        }

        let wallet_count = owner_wallets.len();

        // ── Step 3: Check tx history per wallet (all in parallel) ──────
        let now_ts = chrono::Utc::now().timestamp();

        let mut handles = Vec::with_capacity(wallet_count);
        for wallet in &owner_wallets {
            let wallet = *wallet;
            let rpc = Arc::clone(&rpc);
            let backup = backup_url.clone();
            handles.push(tokio::spawn(async move {
                score_wallet(&rpc, &backup, wallet, now_ts).await
            }));
        }

        let mut suspicious_count: usize = 0;
        let mut total_scored: usize = 0;
        let mut scores: Vec<WalletScore> = Vec::with_capacity(wallet_count);

        for handle in handles {
            if let Ok(score) = handle.await {
                debug!(
                    mint,
                    wallet = %score.wallet,
                    tx_count = score.tx_count,
                    age_secs = score.oldest_tx_age_secs,
                    suspicious = score.suspicious,
                    funder = score.likely_funder_sig.as_deref().unwrap_or("unknown"),
                    "holder_quality: wallet score"
                );
                total_scored += 1;
                if score.suspicious {
                    suspicious_count += 1;
                }
                scores.push(score);
            }
        }

        if total_scored == 0 {
            warn!(mint, wallet_count, "holder_quality: total_scored=0 — all wallet score tasks failed or returned no data");
            return (FilterResult::pass(CHECK_NAME), Some(SmartWalletMetrics::bail("step3_all_wallet_scores_failed")));
        }

        // ── Freshness / bot wallet check ──
        let genuine = total_scored - suspicious_count;
        let suspicious_ratio = suspicious_count as f64 / total_scored as f64;

        // Compute aggregate stats for logging
        let avg_tx_count = if !scores.is_empty() {
            scores.iter().map(|s| s.tx_count as f64).sum::<f64>() / scores.len() as f64
        } else { 0.0 };

        let avg_wallet_age_secs = if !scores.is_empty() {
            scores.iter().map(|s| s.oldest_tx_age_secs as f64).sum::<f64>() / scores.len() as f64
        } else { 0.0 };

        let min_wallet_age_secs = scores.iter().map(|s| s.oldest_tx_age_secs).min().unwrap_or(0);

        info!(
            mint,
            total = total_scored,
            genuine,
            suspicious = suspicious_count,
            ratio = format!("{:.0}%", suspicious_ratio * 100.0),
            "🔍 holder_quality: {}/{} genuine holders",
            genuine,
            total_scored
        );

        if suspicious_ratio > MAX_SUSPICIOUS_RATIO {
            let metrics = SmartWalletMetrics {
                total_scored,
                suspicious_count,
                genuine_count: genuine,
                suspicious_ratio,
                max_same_funder_count: 0, // computed below only if we get past this
                avg_tx_count,
                avg_wallet_age_secs,
                min_wallet_age_secs,
                diagnostic_reason: None,
            };
            return (FilterResult::fail(
                CHECK_NAME,
                &format!(
                    "holder_quality_low: {}/{} holders are fresh/bot wallets ({:.0}% > {:.0}% max)",
                    suspicious_count,
                    total_scored,
                    suspicious_ratio * 100.0,
                    MAX_SUSPICIOUS_RATIO * 100.0,
                ),
            ), Some(metrics));
        }

        // ── Same-funder detection (wash-trade ring) ──
        // The earliest tx signature for each wallet approximates who funded it.
        // If many top holders share the same earliest-tx signer, it's one person
        // with multiple wallets creating fake demand.
        let mut funder_counts: HashMap<String, usize> = HashMap::new();
        for score in &scores {
            if let Some(ref funder) = score.likely_funder_sig {
                *funder_counts.entry(funder.clone()).or_insert(0) += 1;
            }
        }

        let max_same_funder_count = funder_counts.values().copied().max().unwrap_or(0);

        if let Some((funder, count)) = funder_counts.iter().max_by_key(|e| e.1) {
            if *count >= SAME_FUNDER_THRESHOLD {
                warn!(
                    mint,
                    funder,
                    shared_count = count,
                    threshold = SAME_FUNDER_THRESHOLD,
                    "🚨 holder_quality: wash-trade ring — {} holders share funder {}",
                    count,
                    funder
                );
                let metrics = SmartWalletMetrics {
                    total_scored,
                    suspicious_count,
                    genuine_count: genuine,
                    suspicious_ratio,
                    max_same_funder_count,
                    avg_tx_count,
                    avg_wallet_age_secs,
                    min_wallet_age_secs,
                    diagnostic_reason: None,
                };
                return (FilterResult::fail(
                    CHECK_NAME,
                    &format!(
                        "wash_trade_ring: {} of {} top holders share same funder (threshold {})",
                        count, total_scored, SAME_FUNDER_THRESHOLD,
                    ),
                ), Some(metrics));
            }
        }

        let metrics = SmartWalletMetrics {
            total_scored,
            suspicious_count,
            genuine_count: genuine,
            suspicious_ratio,
            max_same_funder_count,
            avg_tx_count,
            avg_wallet_age_secs,
            min_wallet_age_secs,
            diagnostic_reason: None,
        };
        (FilterResult::pass(CHECK_NAME), Some(metrics))
    }
}

// ─── Per-wallet scoring ──────────────────────────────────────

struct WalletScore {
    wallet: Pubkey,
    tx_count: usize,
    oldest_tx_age_secs: i64,
    suspicious: bool,
    /// The signature of the wallet's oldest visible transaction.
    /// Used to group wallets by funder — wallets funded in the same tx
    /// will share this value. For wallets funded separately, the oldest
    /// tx is their first SOL transfer, whose signer is the funder.
    /// We use the signature string as a proxy for "funder identity" because
    /// parsing the full tx to extract the actual signer would require an
    /// additional RPC call (getTransaction). Instead, wallets created by
    /// the same funder in the same batch will share the same oldest sig.
    likely_funder_sig: Option<String>,
}

/// Fetch recent signatures for a wallet and produce a quality score.
async fn score_wallet(
    rpc: &RpcClient,
    backup_url: &str,
    wallet: Pubkey,
    now_ts: i64,
) -> WalletScore {
    let sig_cfg = GetConfirmedSignaturesForAddress2Config {
        limit: Some(SIG_LIMIT),
        ..Default::default()
    };

    let sigs_result = rpc
        .get_signatures_for_address_with_config(&wallet, sig_cfg)
        .await;

    let sigs = match sigs_result {
        Ok(s) => s,
        Err(e) if is_rate_limited(&e) => {
            let fb = RpcClient::new_with_timeout(backup_url.to_string(), REQUEST_TIMEOUT);
            let cfg2 = GetConfirmedSignaturesForAddress2Config {
                limit: Some(SIG_LIMIT),
                ..Default::default()
            };
            match fb.get_signatures_for_address_with_config(&wallet, cfg2).await {
                Ok(s) => s,
                Err(_) => return benefit_of_doubt(wallet),
            }
        }
        Err(_) => return benefit_of_doubt(wallet),
    };

    let tx_count = sigs.len();

    // Oldest visible tx → how old is this wallet (at least)?
    let oldest_tx_age_secs = sigs
        .iter()
        .filter_map(|s| s.block_time)
        .min()
        .map(|t| (now_ts - t).max(0))
        .unwrap_or(0);

    // The oldest signature is likely the funding transaction.
    // Wallets created by the same funder in the same batch will share this signature.
    // For individually funded wallets, this is still useful — if the same tx funded
    // multiple wallets, they'll all have it as their oldest sig.
    let likely_funder_sig = sigs
        .iter()
        .filter(|s| s.block_time.is_some())
        .min_by_key(|s| s.block_time.unwrap_or(i64::MAX))
        .map(|s| s.signature.clone());

    // Suspicious = very few txs OR very young wallet
    let suspicious = tx_count < MIN_TX_COUNT || oldest_tx_age_secs < MIN_WALLET_AGE_SECS;

    WalletScore { wallet, tx_count, oldest_tx_age_secs, suspicious, likely_funder_sig }
}

/// When RPC fails, give the wallet benefit of the doubt (assume genuine).
fn benefit_of_doubt(wallet: Pubkey) -> WalletScore {
    WalletScore {
        wallet,
        tx_count: SIG_LIMIT,
        oldest_tx_age_secs: MIN_WALLET_AGE_SECS + 1,
        suspicious: false,
        likely_funder_sig: None,
    }
}
