//! Parallel enrichment pipeline — fires 10 async calls with 2s timeout.
//!
//! Collects data from: Solana Tracker, on-chain RPC, PumpFun PDA,
//! creator history, Birdeye (4 endpoints), GoPlus, smart wallet analysis.

use std::sync::Arc;
use std::time::{Duration, Instant};

use solana_client::nonblocking::rpc_client::RpcClient;
use tracing::{debug, info, warn};

use super::birdeye::BirdeyeClient;
use super::solana_tracker::SolanaTrackerClient;
use super::types::*;
use crate::config::AppConfig;
use crate::filters::smart_wallet::SmartWalletFilter;

const ENRICHMENT_TIMEOUT_MS: u64 = 2000;
/// Smart wallet gets a slightly longer timeout — it runs in parallel so doesn't
/// add wall-clock time, but its 3 sequential RPC steps can take ~900ms-1.5s.
const SMART_WALLET_TIMEOUT_MS: u64 = 3000;
/// gRPC migration events can reach sniper before the RPC node can serve the
/// freshly-created mint account. A few short retries avoids false
/// `mint_authority_no_data` rejects without materially slowing normal cases.
const MINT_ACCOUNT_FETCH_ATTEMPTS: usize = 4;
const MINT_ACCOUNT_RETRY_DELAYS_MS: [u64; MINT_ACCOUNT_FETCH_ATTEMPTS - 1] = [120, 250, 500];
const FAST_MINT_DATA_TIMEOUT_MS: u64 = 2500;
const FAST_GOPLUS_TIMEOUT_MS: u64 = 1500;

/// Run the full enrichment pipeline for a token that just graduated.
/// All calls fire concurrently with a 2-second overall timeout.
/// Returns whatever data was gathered before the timeout — never blocks the buy.
pub async fn enrich_token(
    cfg: &AppConfig,
    rpc: &RpcClient,
    mint: &str,
    creator_wallet: &str,
    initial_liquidity_sol: f64,
    graduation_time_ms: i64,
) -> EnrichmentResult {
    let start = Instant::now();
    let timeout = Duration::from_millis(ENRICHMENT_TIMEOUT_MS);

    // Build clients
    let st_client = SolanaTrackerClient::new(cfg.env.solana_tracker_api_key.clone());
    let be_client = cfg
        .env
        .birdeye_api_key
        .as_ref()
        .map(|k| BirdeyeClient::new(k));

    let mint_str = mint.to_string();
    let creator_str = creator_wallet.to_string();

    // Fire all enrichment calls concurrently (including smart wallet with its own timeout)
    let sw_timeout = Duration::from_millis(SMART_WALLET_TIMEOUT_MS);
    let (
        st_timed,
        mint_timed,
        curve_timed,
        creator_timed,
        be_overview_timed,
        be_security_timed,
        be_creation_timed,
        be_meme_timed,
        sw_timed,
        gp_timed,
        whale_timed,
    ) = tokio::join!(
        // Call 1: Solana Tracker risk screen
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(timeout, st_client.fetch_token(&mint_str)).await {
                Ok(r) => r,
                Err(_) => {
                    warn!(mint = %mint_str, "SolanaTracker timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 2: On-chain mint metadata
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(timeout, fetch_mint_data(rpc, &mint_str)).await {
                Ok(r) => r,
                Err(_) => {
                    warn!(mint = %mint_str, "Mint data timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 3: PumpFun bonding curve PDA
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(
                timeout,
                fetch_bonding_curve(rpc, &mint_str, graduation_time_ms),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => {
                    warn!(mint = %mint_str, "Bonding curve timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 4: Creator wallet history (via Solana Tracker deployer endpoint)
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(
                timeout,
                fetch_creator_history(&st_client, rpc, &creator_str),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => {
                    warn!(mint = %mint_str, "Creator history timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 5: Birdeye token overview
        async {
            let t = Instant::now();
            let r = match &be_client {
                Some(c) => tokio::time::timeout(timeout, c.token_overview(&mint_str))
                    .await
                    .ok()
                    .flatten(),
                None => None,
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 6: Birdeye token security
        async {
            let t = Instant::now();
            let r = match &be_client {
                Some(c) => tokio::time::timeout(timeout, c.token_security(&mint_str))
                    .await
                    .ok()
                    .flatten(),
                None => None,
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 7: Birdeye creation info
        async {
            let t = Instant::now();
            let r = match &be_client {
                Some(c) => tokio::time::timeout(timeout, c.token_creation_info(&mint_str))
                    .await
                    .ok()
                    .flatten(),
                None => None,
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 8: Birdeye meme detail
        async {
            let t = Instant::now();
            let r = match &be_client {
                Some(c) => tokio::time::timeout(timeout, c.meme_detail(&mint_str))
                    .await
                    .ok()
                    .flatten(),
                None => None,
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 9: Smart wallet analysis (3s timeout — runs in parallel)
        async {
            let t = Instant::now();
            let sw_filter = SmartWalletFilter::new();
            let r = match tokio::time::timeout(sw_timeout, sw_filter.check(&mint_str, cfg)).await {
                Ok((_filter_result, Some(metrics))) => {
                    // Convert from filters::SmartWalletMetrics to sniper::types::SmartWalletMetrics
                    if metrics.diagnostic_reason.is_some() {
                        // Bailed early — treat as no data
                        debug!(mint = %mint_str, reason = ?metrics.diagnostic_reason, "Smart wallet bailed");
                        None
                    } else {
                        Some(super::types::SmartWalletMetrics {
                            total_scored: metrics.total_scored as u32,
                            suspicious_count: metrics.suspicious_count as u32,
                            genuine_count: metrics.genuine_count as u32,
                            suspicious_ratio: metrics.suspicious_ratio,
                            max_same_funder_count: metrics.max_same_funder_count as u32,
                            avg_tx_count: metrics.avg_tx_count,
                            avg_wallet_age_secs: metrics.avg_wallet_age_secs,
                            min_wallet_age_secs: metrics.min_wallet_age_secs as f64,
                        })
                    }
                }
                Ok((_filter_result, None)) => None,
                Err(_) => {
                    warn!(mint = %mint_str, "Smart wallet timed out (3s)");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 10: GoPlus token security (restored — blocks honeypots pre-buy)
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(timeout, fetch_goplus(&mint_str)).await {
                Ok(r) => r,
                Err(_) => {
                    warn!(mint = %mint_str, "GoPlus timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
        // Call 11: ST /trades — whale buy detection (conviction signal)
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(timeout, st_client.fetch_trades(&mint_str)).await {
                Ok(Some(trades)) => {
                    let buys: Vec<_> = trades.iter().filter(|t| t.trade_type == "buy").collect();
                    let sells: Vec<_> = trades.iter().filter(|t| t.trade_type == "sell").collect();

                    let max_single_buy = buys.iter().map(|t| t.volume_sol).fold(0.0_f64, f64::max);
                    let whale_buys: Vec<_> = buys.iter().filter(|t| t.volume_sol >= 1.0).collect();
                    let whale_count = whale_buys.len() as u32;
                    let whale_vol: f64 = whale_buys.iter().map(|t| t.volume_sol).sum();
                    let avg_buy = if buys.is_empty() {
                        0.0
                    } else {
                        buys.iter().map(|t| t.volume_sol).sum::<f64>() / buys.len() as f64
                    };
                    let total_buy_vol: f64 = buys.iter().map(|t| t.volume_sol).sum();
                    let total_sell_vol: f64 = sells.iter().map(|t| t.volume_sol).sum();
                    let ratio = if total_sell_vol > 0.0 {
                        total_buy_vol / total_sell_vol
                    } else if total_buy_vol > 0.0 {
                        99.0
                    } else {
                        0.0
                    };

                    if max_single_buy >= 1.0 {
                        info!(
                            mint = %mint_str,
                            max_buy_sol = format!("{:.2}", max_single_buy),
                            whale_count,
                            whale_vol = format!("{:.2}", whale_vol),
                            buy_sell_ratio = format!("{:.2}", ratio),
                            "🐋 Whale buy activity detected at enrichment"
                        );
                    }

                    Some(WhaleBuyMetrics {
                        max_single_buy_sol: max_single_buy,
                        whale_buy_count: whale_count,
                        whale_buy_volume_sol: whale_vol,
                        avg_buy_size_sol: avg_buy,
                        total_trades: trades.len() as u32,
                        buy_sell_volume_ratio: ratio,
                    })
                }
                Ok(None) => None,
                Err(_) => {
                    warn!(mint = %mint_str, "ST trades timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
    );

    // Destructure timed results
    let (st_result, st_ms) = st_timed;
    let (mint_result, mint_ms) = mint_timed;
    let (curve_result, curve_ms) = curve_timed;
    let (creator_result, creator_ms) = creator_timed;
    let (be_overview, be_overview_ms) = be_overview_timed;
    let (be_security, be_security_ms) = be_security_timed;
    let (be_creation, be_creation_ms) = be_creation_timed;
    let (be_meme, be_meme_ms) = be_meme_timed;
    let (sw_result, sw_ms) = sw_timed;
    let (gp_result, gp_ms) = gp_timed;
    let (whale_result, whale_ms) = whale_timed;

    // Build per-source timing map
    let mut per_source_ms = std::collections::HashMap::new();
    per_source_ms.insert("solana_tracker".to_string(), st_ms);
    per_source_ms.insert("on_chain_mint".to_string(), mint_ms);
    per_source_ms.insert("pumpfun_curve".to_string(), curve_ms);
    per_source_ms.insert("creator_history".to_string(), creator_ms);
    per_source_ms.insert("birdeye_overview".to_string(), be_overview_ms);
    per_source_ms.insert("birdeye_security".to_string(), be_security_ms);
    per_source_ms.insert("birdeye_creation".to_string(), be_creation_ms);
    per_source_ms.insert("birdeye_meme".to_string(), be_meme_ms);
    per_source_ms.insert("smart_wallet".to_string(), sw_ms);
    per_source_ms.insert("goplus".to_string(), gp_ms);
    per_source_ms.insert("whale_buys".to_string(), whale_ms);

    let elapsed_ms = start.elapsed().as_millis() as u64;

    // ── Fallback: if bonding curve byte parsing failed to produce sale_duration_mins,
    // compute it from Birdeye creation info instead (independent data source). ──
    let curve_result = {
        let mut cr = curve_result;
        if let Some(ref mut bc) = cr {
            if bc.sale_duration_mins.is_none() {
                if let Some(ref be_creation_data) = be_creation {
                    if let Some(creation_block_time) = be_creation_data.block_unix_time {
                        let grad_secs = graduation_time_ms / 1000;
                        let duration = (grad_secs - creation_block_time) as f64 / 60.0;
                        if duration > 0.0 && duration < 10080.0 {
                            // sanity: < 7 days
                            info!(
                                mint = %mint_str,
                                sale_duration_mins = format!("{:.1}", duration),
                                "Bonding curve fallback: computed sale_duration from Birdeye creation info"
                            );
                            bc.sale_duration_mins = Some(duration);
                            // Also set creation_timestamp from Birdeye
                            if bc.creation_timestamp.is_none() {
                                bc.creation_timestamp = Some(creation_block_time);
                            }
                        }
                    }
                }
            }
        }
        cr
    };

    // Track which sources completed
    let mut completed = Vec::new();
    let mut timed_out = Vec::new();

    macro_rules! track_source {
        ($name:expr, $val:expr) => {
            if $val.is_some() {
                completed.push($name.to_string());
            } else {
                timed_out.push($name.to_string());
            }
        };
    }

    track_source!("solana_tracker", &st_result);
    track_source!("on_chain_mint", &mint_result);
    track_source!("pumpfun_curve", &curve_result);
    track_source!("creator_history", &creator_result);
    track_source!("birdeye_overview", &be_overview);
    track_source!("birdeye_security", &be_security);
    track_source!("birdeye_creation", &be_creation);
    track_source!("birdeye_meme", &be_meme);
    track_source!("smart_wallet", &sw_result);
    track_source!("goplus", &gp_result);
    track_source!("whale_buys", &whale_result);

    info!(
        mint = %mint_str,
        elapsed_ms = elapsed_ms,
        completed = completed.len(),
        timed_out = timed_out.len(),
        "Enrichment pipeline complete"
    );

    EnrichmentResult {
        solana_tracker: st_result,
        on_chain_mint: mint_result,
        pumpfun_curve: curve_result,
        creator_history: creator_result,
        birdeye_overview: be_overview,
        birdeye_security: be_security,
        birdeye_creation: be_creation,
        birdeye_meme: be_meme,
        goplus: gp_result,
        smart_wallets: sw_result,
        whale_buys: whale_result,
        enrichment_duration_ms: elapsed_ms,
        sources_completed: completed,
        sources_timed_out: timed_out,
        per_source_ms,
    }
}

/// Minimal enrichment for BC fast-track pipeline: only on-chain mint + GoPlus.
/// Runs 2 calls in parallel with a 1.5s timeout (~250-500ms typical).
/// Returns an EnrichmentResult with only mint + goplus populated.
pub async fn enrich_token_fast(rpc: &RpcClient, mint: &str) -> EnrichmentResult {
    let start = Instant::now();
    let mint_timeout = Duration::from_millis(FAST_MINT_DATA_TIMEOUT_MS);
    let goplus_timeout = Duration::from_millis(FAST_GOPLUS_TIMEOUT_MS);
    let mint_str = mint.to_string();

    let (mint_timed, gp_timed) = tokio::join!(
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(mint_timeout, fetch_mint_data(rpc, &mint_str)).await
            {
                Ok(r) => r,
                Err(_) => {
                    warn!(mint = %mint_str, "Fast-track: mint data timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
        async {
            let t = Instant::now();
            let r = match tokio::time::timeout(goplus_timeout, fetch_goplus(&mint_str)).await {
                Ok(r) => r,
                Err(_) => {
                    warn!(mint = %mint_str, "Fast-track: GoPlus timed out");
                    None
                }
            };
            (r, t.elapsed().as_millis() as u64)
        },
    );

    let (mint_result, mint_ms) = mint_timed;
    let (gp_result, gp_ms) = gp_timed;

    let mut per_source_ms = std::collections::HashMap::new();
    per_source_ms.insert("on_chain_mint".to_string(), mint_ms);
    per_source_ms.insert("goplus".to_string(), gp_ms);

    let elapsed_ms = start.elapsed().as_millis() as u64;

    let mut completed = Vec::new();
    let mut timed_out_sources = Vec::new();
    if mint_result.is_some() {
        completed.push("on_chain_mint".to_string());
    } else {
        timed_out_sources.push("on_chain_mint".to_string());
    }
    if gp_result.is_some() {
        completed.push("goplus".to_string());
    } else {
        timed_out_sources.push("goplus".to_string());
    }

    info!(
        mint = %mint_str,
        elapsed_ms,
        completed = completed.len(),
        "⚡ Fast-track enrichment complete"
    );

    EnrichmentResult {
        on_chain_mint: mint_result,
        goplus: gp_result,
        enrichment_duration_ms: elapsed_ms,
        sources_completed: completed,
        sources_timed_out: timed_out_sources,
        per_source_ms,
        ..Default::default()
    }
}

// ─── Individual enrichment functions ─────────────────────────

/// Fetch on-chain mint data: authority revocation status, supply, decimals.
async fn fetch_mint_data(rpc: &RpcClient, mint: &str) -> Option<MintData> {
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    let mint_pubkey = Pubkey::from_str(mint).ok()?;

    let mut account = None;
    for attempt in 0..MINT_ACCOUNT_FETCH_ATTEMPTS {
        match rpc
            .get_account_with_commitment(&mint_pubkey, CommitmentConfig::processed())
            .await
        {
            Ok(resp) => {
                if let Some(a) = resp.value {
                    account = Some(a);
                    break;
                }
                warn!(
                    mint = %mint,
                    attempt = attempt + 1,
                    max_attempts = MINT_ACCOUNT_FETCH_ATTEMPTS,
                    "Mint account not yet available at processed commitment"
                );
            }
            Err(e) => {
                warn!(
                    mint = %mint,
                    attempt = attempt + 1,
                    max_attempts = MINT_ACCOUNT_FETCH_ATTEMPTS,
                    "Failed to fetch mint account: {}",
                    e
                );
                if let Some(delay_ms) = MINT_ACCOUNT_RETRY_DELAYS_MS.get(attempt) {
                    tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
                }
            }
        }
    }
    let account = account?;

    let data = &account.data;
    if data.len() < 82 {
        warn!(mint = %mint, "Mint account data too short: {} bytes", data.len());
        return None;
    }

    // Parse COption<Pubkey> for mint_authority (bytes 0-36)
    let mint_auth_tag = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let mint_authority_revoked = mint_auth_tag == 0;

    // Parse supply (bytes 36-44)
    let supply_raw = u64::from_le_bytes([
        data[36], data[37], data[38], data[39], data[40], data[41], data[42], data[43],
    ]);

    // Parse decimals (byte 44)
    let decimals = data[44];

    // Parse COption<Pubkey> for freeze_authority (bytes 46-82)
    let freeze_auth_tag = u32::from_le_bytes([data[46], data[47], data[48], data[49]]);
    let freeze_authority_revoked = freeze_auth_tag == 0;

    let supply = supply_raw as f64 / 10_f64.powi(decimals as i32);

    debug!(
        mint = %mint,
        mint_auth_revoked = mint_authority_revoked,
        freeze_auth_revoked = freeze_authority_revoked,
        supply = supply,
        "On-chain mint data parsed"
    );

    Some(MintData {
        mint_authority_revoked,
        freeze_authority_revoked,
        supply,
        decimals,
    })
}

/// Derive PumpFun bonding curve PDA and fetch creation timestamp.
async fn fetch_bonding_curve(
    rpc: &RpcClient,
    mint: &str,
    graduation_time_ms: i64,
) -> Option<BondingCurveData> {
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    let mint_pubkey = Pubkey::from_str(mint).ok()?;
    let pumpfun_program = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").ok()?;

    // Derive bonding curve PDA: seeds = ["bonding-curve", mint_bytes]
    let (pda, _bump) =
        Pubkey::find_program_address(&[b"bonding-curve", mint_pubkey.as_ref()], &pumpfun_program);

    let account = match rpc.get_account(&pda).await {
        Ok(a) => a,
        Err(e) => {
            debug!(mint = %mint, "Bonding curve PDA not found: {}", e);
            return None;
        }
    };

    let data = &account.data;
    if data.len() < 49 {
        return None;
    }

    // PumpFun bonding curve layout (approximate):
    // bytes 8-16: virtual_token_reserves (u64)
    // bytes 16-24: virtual_sol_reserves (u64)
    // bytes 40-48: creation timestamp (i64) — varies by version
    // byte 48: complete (bool)

    let virtual_token_reserves =
        u64::from_le_bytes(data[8..16].try_into().unwrap_or([0u8; 8])) as f64 / 1_000_000.0;

    let virtual_sol_reserves =
        u64::from_le_bytes(data[16..24].try_into().unwrap_or([0u8; 8])) as f64 / 1_000_000_000.0;

    let complete = data.len() > 48 && data[48] != 0;

    // Try to extract creation timestamp from slot context
    // If not available in account data, estimate from graduation time
    let creation_timestamp: Option<i64> = if data.len() >= 48 {
        let ts = i64::from_le_bytes(data[40..48].try_into().unwrap_or([0u8; 8]));
        // Sanity check: must be a reasonable Unix timestamp (after 2024)
        if ts > 1_700_000_000 && ts < 2_000_000_000 {
            Some(ts)
        } else {
            None
        }
    } else {
        None
    };

    let sale_duration_mins = creation_timestamp.map(|ct| {
        let grad_secs = graduation_time_ms / 1000;
        (grad_secs - ct) as f64 / 60.0
    });

    debug!(
        mint = %mint,
        complete = complete,
        sale_duration_mins = ?sale_duration_mins,
        "Bonding curve data parsed"
    );

    Some(BondingCurveData {
        creation_timestamp,
        complete,
        virtual_sol_reserves: Some(virtual_sol_reserves),
        virtual_token_reserves: Some(virtual_token_reserves),
        sale_duration_mins,
    })
}

/// Fetch creator wallet history — tries Solana Tracker /tokens/deployer first (exact count),
/// falls back to RPC signature heuristic if ST is unavailable.
async fn fetch_creator_history(
    st_client: &SolanaTrackerClient,
    rpc: &RpcClient,
    creator: &str,
) -> Option<CreatorData> {
    // ── Primary: Solana Tracker deployer endpoint (exact token list) ──
    if let Some(deployer_tokens) = st_client.fetch_deployer_tokens(creator).await {
        let token_count = deployer_tokens.len() as u64;
        let rugged_count = deployer_tokens.iter().filter(|t| t.rugged).count() as u64;
        // Serial creator: >5 tokens OR any rugged tokens
        let is_serial = token_count > 5 || rugged_count > 0;

        debug!(
            creator = %creator,
            token_count = token_count,
            rugged_count = rugged_count,
            is_serial = is_serial,
            "Creator history via SolanaTracker deployer"
        );

        return Some(CreatorData {
            wallet_age_days: None, // Not available from this endpoint
            previous_token_count: Some(token_count),
            is_serial_creator: is_serial,
            total_signatures: None,
        });
    }

    // ── Fallback: RPC signature scan (less accurate) ──
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    let creator_pubkey = Pubkey::from_str(creator).ok()?;
    let sigs = match rpc.get_signatures_for_address(&creator_pubkey).await {
        Ok(s) => s,
        Err(e) => {
            warn!(creator = %creator, "Failed to fetch creator signatures: {}", e);
            return None;
        }
    };

    let total_signatures = sigs.len() as u64;

    let wallet_age_days = sigs.last().and_then(|s| {
        s.block_time.map(|bt| {
            let now = chrono::Utc::now().timestamp();
            (now - bt) as f64 / 86400.0
        })
    });

    let estimated_tokens = total_signatures / 5;
    let is_serial = estimated_tokens > 5;

    debug!(
        creator = %creator,
        total_sigs = total_signatures,
        estimated_tokens = estimated_tokens,
        wallet_age_days = ?wallet_age_days,
        "Creator history via RPC fallback"
    );

    Some(CreatorData {
        wallet_age_days,
        previous_token_count: Some(estimated_tokens),
        is_serial_creator: is_serial,
        total_signatures: Some(total_signatures),
    })
}

// ─── GoPlus lightweight fetch for enrichment ─────────────────

/// Fetch GoPlus token security data for the enrichment pipeline.
/// Returns a sniper::types::GoPlusResult for use by hard filters.
async fn fetch_goplus(mint: &str) -> Option<GoPlusResult> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .ok()?;

    let url = format!(
        "https://api.gopluslabs.io/api/v1/solana/token_security/{}",
        mint
    );
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let body: serde_json::Value = resp.json().await.ok()?;
    if body.get("code").and_then(|v| v.as_i64()) != Some(1) {
        return None;
    }

    let results = body.get("result")?.as_object()?;
    // GoPlus keys by lowercase mint
    let key = mint.to_lowercase();
    let token = results
        .iter()
        .find(|(k, _)| k.to_lowercase() == key || *k == mint)
        .map(|(_, v)| v)?;

    Some(GoPlusResult {
        is_honeypot: token
            .get("is_honeypot")
            .and_then(|v| v.as_str())
            .map(String::from),
        is_mintable: token
            .get("is_mintable")
            .and_then(|v| v.as_str())
            .map(String::from),
        transfer_pausable: token
            .get("transfer_pausable")
            .and_then(|v| v.as_str())
            .map(String::from),
        is_blacklisted: token
            .get("is_blacklisted")
            .and_then(|v| v.as_str())
            .map(String::from),
        can_take_back_ownership: token
            .get("can_take_back_ownership")
            .and_then(|v| v.as_str())
            .map(String::from),
        is_proxy: token
            .get("is_proxy")
            .and_then(|v| v.as_str())
            .map(String::from),
        is_open_source: token
            .get("is_open_source")
            .and_then(|v| v.as_str())
            .map(String::from),
        holder_count: token.get("holder_count").and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v.as_u64())
        }),
    })
}
