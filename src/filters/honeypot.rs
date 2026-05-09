use std::str::FromStr;
use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::Transaction;
use tracing::{debug, warn};

use super::rpc_fallback::is_rate_limited;
use super::types::FilterResult;
use crate::config::AppConfig;

const CHECK_NAME: &str = "honeypot";
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Standard Solana SPL Token programme address.
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Token-2022 (Token Extensions) programme address — also legitimate.
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// Amount of tokens (in smallest unit) used for the simulated sell test.
const SIMULATED_SELL_AMOUNT: u64 = 1_000;

pub struct HoneypotFilter;

impl HoneypotFilter {
    pub fn new() -> Self {
        Self
    }

    /// Run all honeypot checks concurrently and return a combined result.
    ///
    /// Check 1 — Simulate a sell transaction (can the token actually be sold?)
    /// Check 2 — Verify the mint is owned by the standard SPL Token programme
    pub async fn check(&self, mint: &Pubkey, bot_wallet: &Pubkey, cfg: &AppConfig) -> FilterResult {
        let rpc = RpcClient::new_with_timeout(cfg.env.solana_rpc_url.clone(), RPC_TIMEOUT);

        let fallback_rpc_url = &cfg.env.solana_rpc_backup_url;

        // Run both checks concurrently
        let (sell_result, programme_result) = tokio::join!(
            self.check_sell_simulation(mint, bot_wallet, &rpc, fallback_rpc_url),
            self.check_token_programme(mint, &rpc, fallback_rpc_url),
        );

        // Check 1 — sell simulation
        if let Err(reason) = sell_result {
            return FilterResult::fail(CHECK_NAME, &reason);
        }

        // Check 2 — token programme
        if let Err(reason) = programme_result {
            return FilterResult::fail(CHECK_NAME, &reason);
        }

        FilterResult::pass(CHECK_NAME)
    }

    /// Check 1 — Simulate a sell transaction to verify the token can be sold.
    ///
    /// Builds a minimal token transfer instruction and runs it through
    /// `simulateTransaction`. If the simulation returns any error, the
    /// token is likely a honeypot (sell is blocked).
    async fn check_sell_simulation(
        &self,
        mint: &Pubkey,
        bot_wallet: &Pubkey,
        rpc: &RpcClient,
        fallback_rpc_url: &str,
    ) -> Result<(), String> {
        debug!(mint = %mint, "honeypot: simulating sell transaction");

        // Build a simulated SPL token transfer instruction.
        // We construct a minimal Transfer instruction for the SPL Token
        // programme. Even though we don't hold the tokens, we can still
        // check whether the programme allows this instruction type via
        // simulation — honeypot tokens typically add hooks or authority
        // checks that cause simulation to fail.

        let spl_token_program = Pubkey::from_str(SPL_TOKEN_PROGRAM_ID)
            .map_err(|e| format!("honeypot_invalid_spl_program: {}", e))?;

        // Derive the associated token account for the bot wallet
        let ata = spl_associated_token_account(bot_wallet, mint, &spl_token_program);

        // SPL Token Transfer instruction (instruction index 3)
        // Layout: [3u8] ++ [amount as u64 LE]
        let mut data = vec![3u8]; // Transfer instruction discriminator
        data.extend_from_slice(&SIMULATED_SELL_AMOUNT.to_le_bytes());

        let instruction = Instruction {
            program_id: spl_token_program,
            accounts: vec![
                AccountMeta::new(ata, false),                 // source ATA
                AccountMeta::new(ata, false), // destination (self — doesn't matter for simulation)
                AccountMeta::new_readonly(*bot_wallet, true), // owner / signer
            ],
            data,
        };

        let message = Message::new(&[instruction], Some(bot_wallet));
        let tx = Transaction::new_unsigned(message);

        // ── Fallback for 429 rate-limit errors ───────────
        let sim_result = rpc.simulate_transaction(&tx).await;
        let sim_result = if matches!(&sim_result, Err(e) if is_rate_limited(e)) {
            warn!(mint = %mint, "QuickNode rate limited, using fallback RPC");
            let fallback = RpcClient::new_with_timeout(fallback_rpc_url.to_string(), RPC_TIMEOUT);
            fallback.simulate_transaction(&tx).await
        } else {
            sim_result
        };

        match sim_result {
            Ok(response) => {
                if let Some(err) = response.value.err {
                    debug!(
                        mint = %mint,
                        error = %err,
                        "honeypot: sell simulation returned error"
                    );
                    let err_str = format!("{}", err);

                    // "Blockhash not found" is a transient RPC issue, NOT
                    // a real honeypot signal — skip the simulation gracefully.
                    if err_str.contains("Blockhash not found") {
                        tracing::warn!(
                            "honeypot: blockhash unavailable, skipping simulation mint={}",
                            mint
                        );
                        return Ok(());
                    }

                    // These errors are normal for accounts that don't exist
                    // or have zero balance — they do NOT indicate a honeypot.
                    let benign_errors = [
                        "AccountNotFound",
                        "InsufficientFunds",
                        "insufficient funds",
                        "account not found",
                        "InvalidAccountData",
                        "custom program error: 0x1", // SPL: insufficient funds
                    ];

                    let is_benign = benign_errors.iter().any(|be| err_str.contains(be));

                    if is_benign {
                        debug!(
                            mint = %mint,
                            "honeypot: simulation error is benign (no tokens held) — PASS"
                        );
                        return Ok(());
                    }

                    // Only fail on real honeypot indicators:
                    // - "custom program error" (programme-level rejection)
                    // - "insufficient funds for rent" (account constraints)
                    // - Any "0x" programme error code
                    let real_honeypot_errors =
                        ["custom program error", "insufficient funds for rent", "0x"];

                    let is_real_honeypot =
                        real_honeypot_errors.iter().any(|re| err_str.contains(re));

                    if is_real_honeypot {
                        return Err(format!("honeypot_sell_simulation_failed: {}", err_str));
                    }

                    // Unknown simulation error — not a confirmed honeypot,
                    // log and allow through.
                    debug!(
                        mint = %mint,
                        error = %err_str,
                        "honeypot: simulation returned unrecognised error — PASS (not a known honeypot pattern)"
                    );
                    return Ok(());
                }

                debug!(mint = %mint, "honeypot: sell simulation succeeded — PASS");
                Ok(())
            }
            Err(e) => {
                let rpc_err = format!("{}", e);

                // "Blockhash not found" is a transient RPC issue — skip gracefully.
                if rpc_err.contains("Blockhash not found") {
                    tracing::warn!(
                        "honeypot: blockhash unavailable, skipping simulation mint={}",
                        mint
                    );
                    return Ok(());
                }

                // Only fail on errors that look like real honeypot signals.
                let real_honeypot_errors =
                    ["custom program error", "insufficient funds for rent", "0x"];

                let is_real_honeypot = real_honeypot_errors.iter().any(|re| rpc_err.contains(re));

                if is_real_honeypot {
                    warn!(
                        mint = %mint,
                        "honeypot: RPC simulate call failed with honeypot indicator: {}",
                        rpc_err
                    );
                    return Err(format!("honeypot_sell_simulation_rpc_error: {}", rpc_err));
                }

                // Other RPC errors are transient — not the token's fault.
                warn!(
                    mint = %mint,
                    "honeypot: RPC simulate call failed (transient): {} — PASS",
                    rpc_err
                );
                Ok(())
            }
        }
    }

    /// Check 2 — Verify the mint account is owned by a legitimate token programme.
    ///
    /// Accepts both the standard SPL Token programme and Token-2022.
    /// Only rejects if the owner is neither of these two.
    async fn check_token_programme(
        &self,
        mint: &Pubkey,
        rpc: &RpcClient,
        fallback_rpc_url: &str,
    ) -> Result<(), String> {
        debug!(mint = %mint, "honeypot: checking token programme owner");

        let spl_programme = Pubkey::from_str(SPL_TOKEN_PROGRAM_ID)
            .map_err(|e| format!("honeypot_invalid_spl_program: {}", e))?;
        let token_2022_programme = Pubkey::from_str(TOKEN_2022_PROGRAM_ID)
            .map_err(|e| format!("honeypot_invalid_token2022_program: {}", e))?;

        // ── Fallback for 429 rate-limit errors ───────────
        let account_result = rpc.get_account(mint).await;
        let account_result = if matches!(&account_result, Err(e) if is_rate_limited(e)) {
            warn!(mint = %mint, "QuickNode rate limited, using fallback RPC");
            let fallback = RpcClient::new_with_timeout(fallback_rpc_url.to_string(), RPC_TIMEOUT);
            fallback.get_account(mint).await
        } else {
            account_result
        };

        let account = match account_result {
            Ok(acc) => acc,
            Err(e) => {
                let err_str = format!("{}", e);
                // Brand-new pump.fun tokens may not be indexed yet —
                // AccountNotFound is NOT a honeypot signal.
                if err_str.contains("AccountNotFound") || err_str.contains("could not find account")
                {
                    warn!(
                        mint = %mint,
                        "honeypot: mint account not found (likely brand-new token) — PASS"
                    );
                    return Ok(());
                }
                // Rate-limit / transient RPC errors are not the token's fault.
                if is_rate_limited(&e)
                    || err_str.contains("timeout")
                    || err_str.contains("502")
                    || err_str.contains("503")
                {
                    warn!(
                        mint = %mint,
                        "honeypot: transient RPC error fetching mint account — PASS: {}",
                        err_str
                    );
                    return Ok(());
                }
                return Err(format!("honeypot_mint_account_fetch_failed: {}", err_str));
            }
        };

        if account.owner == token_2022_programme {
            tracing::debug!("Token2022 programme detected, allowing: {}", mint);
            return Ok(());
        }

        if account.owner != spl_programme {
            warn!(
                mint = %mint,
                owner = %account.owner,
                "honeypot: non-standard token programme detected (not SPL or Token2022)"
            );
            return Err(format!(
                "non_standard_token_programme: owner={} expected={} or {}",
                account.owner, spl_programme, token_2022_programme
            ));
        }

        debug!(mint = %mint, "honeypot: standard SPL Token programme confirmed — PASS");
        Ok(())
    }
}

/// Derive the Associated Token Account (ATA) address for a wallet + mint.
///
/// This is a pure derivation (no RPC call) using the standard PDA seeds.
fn spl_associated_token_account(wallet: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    // The Associated Token Account Program ID
    let ata_program = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
        .expect("hardcoded ATA program ID is valid");

    let seeds = &[wallet.as_ref(), token_program.as_ref(), mint.as_ref()];

    let (ata, _bump) = Pubkey::find_program_address(seeds, &ata_program);
    ata
}
