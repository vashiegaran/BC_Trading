pub mod types;
pub mod error;
pub mod dedup;

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::execution::helius_sender::HeliusSenderClient;
use crate::execution::jito_client::JitoClient;
use crate::execution::jupiter::JupiterClient;
use crate::execution::log_trade_cost;
use crate::execution::state::TradingState;
use crate::execution::wallet::BotWallet;
use crate::exit::dedup::DedupRegistry;
use crate::exit::error::ExitError;
use crate::logger::SupabaseClient;
use crate::monitoring::types::{ExitResult, ExitSignal};

#[derive(Debug, Clone, Default)]
struct ConfirmedExitOutcome {
    wallet_pre_tokens: Option<f64>,
    wallet_post_tokens: Option<f64>,
    sol_received: Option<f64>,
    wallet_pre_sol: Option<f64>,
    wallet_post_sol: Option<f64>,
}

pub fn start(
    cfg: Arc<AppConfig>,
    mut exit_rx: mpsc::Receiver<ExitSignal>,
    supabase: Arc<SupabaseClient>,
    wallet: Arc<BotWallet>,
    confirm_tx: broadcast::Sender<ExitResult>,
    trading_state: Arc<TradingState>,
) {
    tokio::spawn(async move {
        info!("Exit engine started — waiting for exit signals (concurrent)");

        let jupiter = Arc::new(JupiterClient::new(
            cfg.strategy.execution.api_request_timeout_secs,
            cfg.strategy.execution.max_retries,
        ));
        let rpc = Arc::new(RpcClient::new(cfg.env.solana_rpc_url.clone()));
        let backup_rpc = Arc::new(RpcClient::new(cfg.env.solana_rpc_backup_url.clone()));

        // Pre-warm Jupiter HTTP connection (pays TLS handshake now, not on first sell)
        if let Err(e) = jupiter.get_price(crate::execution::jupiter::SOL_MINT).await {
            debug!("Jupiter pre-warm (exit) failed (non-fatal): {}", e);
        } else {
            info!("⚡ Jupiter HTTP connection pre-warmed (exit engine)");
        }

        // Create JitoClient once — reuse for all exits (saves TLS handshake per sell)
        let jito_client: Option<Arc<JitoClient>> = if cfg.env.use_jito {
            let jc = Arc::new(JitoClient::new(
                cfg.env.jito_block_engine_url.clone(),
                cfg.strategy.jito.tip_multiplier,
                cfg.env.jito_max_tip_sol,
            ));
            let _ = jc.get_recommended_tip().await;
            info!("⚡ Jito HTTP connection pre-warmed (exit engine)");
            Some(jc)
        } else {
            None
        };

        // Create HeliusSenderClient once — reuse for all exits
        let helius_sender: Option<Arc<HeliusSenderClient>> = if cfg.env.use_helius_sender {
            let hs = Arc::new(HeliusSenderClient::new(cfg.env.helius_sender_url.clone()));
            hs.warm_connection().await;
            Some(hs)
        } else {
            None
        };

        // In-flight exit dedup — prevents concurrent ExitSignals for the same
        // position_id from racing each other (TP1 + trailing_stop fired within
        // the same tick, etc). The guard releases on task completion.
        let exit_dedup = DedupRegistry::new();

        while let Some(mut signal) = exit_rx.recv().await {
            // Claim this position before doing any work. If another task is
            // already selling it, drop this signal rather than spawn a second
            // parallel sell on the same wallet balance.
            let dedup_guard = match exit_dedup.try_acquire(signal.position_id) {
                Some(g) => g,
                None => {
                    warn!(
                        mint = %signal.mint,
                        position_id = signal.position_id,
                        reason = %signal.reason,
                        "⏭️  Exit already in flight for this position — dropping duplicate signal"
                    );
                    log_exit_system_event(
                        &supabase,
                        "exit_dedup_skipped",
                        &format!(
                            "position_id={} mint={} reason={} — duplicate signal dropped",
                            signal.position_id, signal.mint, signal.reason
                        ),
                    )
                    .await;
                    continue;
                }
            };

            info!(
                mint = %signal.mint,
                reason = %signal.reason,
                pct_to_sell = signal.pct_to_sell,
                token_amount = signal.token_amount,
                "🚪 Exit engine processing signal"
            );

            // If token_amount is 0, poll on-chain balance briefly.
            // This happens when post-buy verification fires an emergency exit
            // before the RPC has indexed the token account from the buy tx.
            // v5.3 latency fix: shrunk pre-poll from 5s → 1s. If still unresolved,
            // bail fast so monitoring can re-fire the exit from a later trigger
            // instead of burning the exit window here.
            if signal.token_amount <= 0.0 {
                warn!(
                    mint = %signal.mint,
                    "token_amount is 0 — polling on-chain balance (up to 1s)"
                );
                let mut resolved = false;
                for retry in 1..=4 {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    if let Some(balance) = fetch_exit_token_balance(&rpc, &wallet.pubkey(), &signal.mint).await {
                        if balance > 0.0 {
                            info!(
                                mint = %signal.mint,
                                balance,
                                retry,
                                "✅ On-chain token balance resolved after polling"
                            );
                            signal.token_amount = balance;
                            resolved = true;
                            break;
                        }
                    }
                }
                if !resolved {
                    error!(
                        mint = %signal.mint,
                        "token_amount still 0 after 1s of polling — skipping sell"
                    );
                    log_exit_system_event(
                        &supabase,
                        "exit_skipped_zero_tokens",
                        &format!("Mint: {} — token_amount was 0 after 1s polling", signal.mint),
                    )
                    .await;
                    let _ = confirm_tx.send(ExitResult {
                        mint: signal.mint.clone(),
                        reason: signal.reason.clone(),
                        success: false,
                        permanent: true,
                    });
                    continue;
                }
            }

            // Spawn each exit as a concurrent task so one slow exit doesn't block others
            let cfg = Arc::clone(&cfg);
            let jupiter = Arc::clone(&jupiter);
            let supabase = Arc::clone(&supabase);
            let rpc = Arc::clone(&rpc);
            let backup_rpc = Arc::clone(&backup_rpc);
            let wallet = Arc::clone(&wallet);
            let confirm_tx = confirm_tx.clone();
            let trading_state = Arc::clone(&trading_state);
            let jito_client = jito_client.clone();
            let helius_sender = helius_sender.clone();

            tokio::spawn(async move {
                // Holding the guard in the spawned task means the claim
                // releases when this task ends — whether by Ok, retryable
                // failure, or permanent failure.
                let _dedup_guard = dedup_guard;
                let (success, is_permanent) = if signal.is_paper_trade || cfg.env.paper_trade {
                    match execute_paper_exit(&cfg, &jupiter, &supabase, &signal).await {
                        Ok(()) => (true, false),
                        Err(e) => {
                            error!(mint = %signal.mint, "Paper exit failed: {}", e);
                            (false, false)
                        }
                    }
                } else {
                    match execute_real_exit(&cfg, &jupiter, &supabase, &rpc, &backup_rpc, &wallet, &signal, &trading_state, jito_client.as_deref(), helius_sender.as_deref()).await {
                        Ok(()) => (true, false),
                        Err(e) => {
                            let err_str = e.to_string();
                            error!(mint = %signal.mint, "Real exit failed: {}", err_str);
                            // Unified classification: the outer handler and
                            // the inner confirm loop now agree on what
                            // counts as retryable. See src/exit/error.rs.
                            let classified = ExitError::classify(&err_str);
                            log_exit_system_event(
                                &supabase,
                                "exit_error_classified",
                                &format!(
                                    "position_id={} mint={} tag={} err={}",
                                    signal.position_id,
                                    signal.mint,
                                    classified.tag(),
                                    err_str
                                ),
                            )
                            .await;
                            let is_retryable = classified.is_retryable();
                            if !is_retryable {
                                mark_exit_failed(&supabase, signal.position_id, &err_str).await;
                            } else {
                                warn!(
                                    mint = %signal.mint,
                                    position_id = signal.position_id,
                                    tag = classified.tag(),
                                    "Retryable exit failure — position stays open for next trigger"
                                );
                            }
                            (false, !is_retryable)
                        }
                    }
                };


                let _ = confirm_tx.send(ExitResult {
                    mint: signal.mint.clone(),
                    reason: signal.reason.clone(),
                    success,
                    permanent: is_permanent,
                });
            });
        }

        info!("Exit engine shutting down (exit channel closed)");
    });
}

// ─── Paper exit ──────────────────────────────────────────────

async fn execute_paper_exit(
    _cfg: &AppConfig,
    jupiter: &JupiterClient,
    supabase: &SupabaseClient,
    signal: &ExitSignal,
) -> Result<()> {
    let exit_start = std::time::Instant::now();
    let sell_fraction = signal.pct_to_sell as f64 / 100.0;
    let tokens_to_sell = (signal.token_amount * sell_fraction) as u64;
    let sol_mint = crate::execution::jupiter::SOL_MINT;

    // Simulated fees (matching real exit cost structure)
    let sim_network_fee: f64 = 0.000005;
    let sim_priority_fee: f64 = 0.0005;
    let sim_tip: f64 = 0.0; // No tip injection with Chainstack Warp
    let sim_total_fees = sim_network_fee + sim_priority_fee + sim_tip;

    // ── Phase 3 realistic exit: try Jupiter sell quote first ──
    // This captures real pool depth, price impact, and route availability —
    // bringing paper PnL accuracy from ~75% to ~95% of real money numbers.
    let mut used_jupiter_quote = false;
    let mut quote_price_impact_bps: f64 = 0.0;

    let (exit_price, sol_received_this_exit) = if tokens_to_sell > 0 {
        // Simulate execution delay before quoting (real exits take 3-8s)
        if _cfg.strategy.execution.paper_exit_delay_ms > 0 {
            let delay = _cfg.strategy.execution.paper_exit_delay_ms;
            debug!(mint = %signal.mint, delay_ms = delay, "Paper exit: simulating execution delay");
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }

        match jupiter.get_quote(&signal.mint, sol_mint, tokens_to_sell, 2000).await {
            Ok(quote) => {
                let out_lamports: f64 = quote.out_amount.parse().unwrap_or(0.0);
                let sol_value = out_lamports / 1_000_000_000.0;
                let price_impact: f64 = quote.price_impact_pct.parse().unwrap_or(0.0);
                quote_price_impact_bps = (price_impact * 100.0).abs();
                used_jupiter_quote = true;

                // Derive exit price from quote (reverse-engineer from SOL received)
                let derived_price = if signal.entry_price_usd > 0.0 && signal.sol_spent > 0.0 {
                    signal.entry_price_usd * (sol_value / (signal.sol_spent * sell_fraction))
                } else {
                    signal.current_price
                };

                // Apply same sanity checks as real exits
                let cost_for_chunk = signal.sol_spent * sell_fraction;
                let effective_return_ratio = if cost_for_chunk > 0.0 { sol_value / cost_for_chunk } else { 1.0 };

                let is_take_profit = matches!(
                    signal.reason,
                    crate::monitoring::types::ExitReason::TakeProfit1
                        | crate::monitoring::types::ExitReason::TakeProfit2
                        | crate::monitoring::types::ExitReason::TakeProfit3
                );

                // TP guard: reject if real execution would lose money
                if is_take_profit && effective_return_ratio < 1.0 {
                    warn!(
                        mint = %signal.mint,
                        effective_return_ratio = format!("{:.3}", effective_return_ratio),
                        quote_sol = format!("{:.6}", sol_value),
                        cost_for_chunk = format!("{:.6}", cost_for_chunk),
                        "⚠️ Paper exit: TP would execute at a loss (Jupiter quote) — rejecting"
                    );
                    return Err(anyhow::anyhow!(
                        "Paper TP sanity check failed: ratio={:.3}, slippage too high",
                        effective_return_ratio
                    ));
                }

                let sol_after_fees = (sol_value - sim_total_fees).max(0.0);

                info!(
                    mint = %signal.mint,
                    sol_received = format!("{:.6}", sol_after_fees),
                    price_impact_bps = format!("{:.1}", quote_price_impact_bps),
                    return_ratio = format!("{:.3}", effective_return_ratio),
                    "📊 Paper exit: Jupiter quote used (realistic simulation)"
                );

                (derived_price, sol_after_fees)
            }
            Err(e) => {
                // Fallback: Jupiter quote failed — use DexScreener price + flat slippage
                warn!(
                    mint = %signal.mint,
                    error = %e,
                    "Paper exit: Jupiter quote failed — falling back to DexScreener price"
                );
                paper_exit_fallback_pricing(_cfg, jupiter, signal, sell_fraction, sim_total_fees).await
            }
        }
    } else {
        // Zero tokens to sell — use fallback
        paper_exit_fallback_pricing(_cfg, jupiter, signal, sell_fraction, sim_total_fees).await
    };

    let pnl_sol_this_exit = sol_received_this_exit - (signal.sol_spent * sell_fraction);

    let is_full_exit = signal.pct_to_sell == 100;
    let new_status = if is_full_exit { "closed" } else { "partial" };

    let remaining_tokens = signal.token_amount * (1.0 - sell_fraction);

    // Fetch existing position state (sol_received + status + created_at)
    let pos_state = fetch_position_exit_state(supabase, signal.position_id).await;

    // Guard: if position is already closed, skip this exit to prevent double-counting
    if pos_state.status == "closed" {
        warn!(
            mint = %signal.mint,
            position_id = signal.position_id,
            "⏭️ Paper exit skipped — position already closed (duplicate signal)"
        );
        return Ok(());
    }

    // Use original sol_spent from DB (not signal.sol_spent which gets halved after TP1 partial exit)
    let original_sol_spent = if pos_state.sol_spent > 0.0 { pos_state.sol_spent } else { signal.sol_spent };

    let prev_sol_received = pos_state.sol_received;
    let prev_pnl_sol = pos_state.pnl_sol;
    let total_sol_received = prev_sol_received + sol_received_this_exit;
    // Total PnL = total received - original cost (from DB, never halved)
    let total_pnl_sol = total_sol_received - original_sol_spent;
    let total_pnl_pct = if original_sol_spent > 0.0 {
        (total_sol_received - original_sol_spent) / original_sol_spent * 100.0
    } else {
        0.0
    };

    // Sanitize: NaN/Infinity → 0.0 (safety net for upstream math errors)
    let total_pnl_pct = sanitize_f64(total_pnl_pct, "total_pnl_pct", &signal.mint);
    let total_pnl_sol = sanitize_f64(total_pnl_sol, "total_pnl_sol", &signal.mint);
    let total_sol_received = sanitize_f64(total_sol_received, "total_sol_received", &signal.mint);

    let url = format!(
        "{}/positions?id=eq.{}",
        supabase.base_url, signal.position_id
    );

    let paper_exit_reason = match &signal.sub_reason {
        Some(sr) => format!("{}:{}", signal.reason, sr),
        None => signal.reason.to_string(),
    };

    let mut payload = serde_json::json!({
        "status": new_status,
        "exit_price_usd": exit_price,
        "exit_reason": paper_exit_reason,
        "pnl_pct": total_pnl_pct,
        "pnl_sol": total_pnl_sol,
        "sol_received": total_sol_received,
        "token_amount": remaining_tokens,
    });

    // Write TP flags to DB only after confirmed sell
    match &signal.reason {
        crate::monitoring::types::ExitReason::TakeProfit1 => {
            payload.as_object_mut().unwrap().insert("tp1_triggered".to_string(), serde_json::Value::Bool(true));
        }
        crate::monitoring::types::ExitReason::TakeProfit2 => {
            payload.as_object_mut().unwrap().insert("tp2_triggered".to_string(), serde_json::Value::Bool(true));
        }
        _ => {}
    }

    if is_full_exit {
        let now = chrono::Utc::now().to_rfc3339();
        payload.as_object_mut().unwrap().insert(
            "exit_time".to_string(),
            serde_json::Value::String(now.clone()),
        );
        payload.as_object_mut().unwrap().insert(
            "closed_at".to_string(),
            serde_json::Value::String(now),
        );
        if let Some(hold_secs) = compute_hold_duration(&pos_state.created_at) {
            payload.as_object_mut().unwrap().insert(
                "hold_duration_secs".to_string(),
                serde_json::json!(hold_secs),
            );
        }
    }

    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to update position on exit: {}", body);
        }
        Err(e) => {
            warn!("Failed to update position on exit: {}", e);
        }
    }

    let pnl_symbol = if total_pnl_sol >= 0.0 { "+" } else { "" };
    println!(
        "[{}] {} mint={} | reason={} | pnl={}{:.2}% | pnl={}{:.4} SOL | sold={}% | this_exit={:.4} SOL",
        chrono::Utc::now().format("%H:%M:%S"),
        if is_full_exit { "🏁 PAPER EXIT" } else { "📤 PAPER PARTIAL" },
        signal.mint,
        signal.reason,
        pnl_symbol, total_pnl_pct,
        pnl_symbol, total_pnl_sol,
        signal.pct_to_sell,
        sol_received_this_exit,
    );

    // Use increment (new - prev) to avoid double-counting across partial exits.
    let daily_pnl_increment = total_pnl_sol - prev_pnl_sol;
    update_daily_stats(supabase, daily_pnl_increment).await;

    // Log cost breakdown for paper exit (realistic: Jupiter quote + simulated fees)
    let effective_slippage_bps = if used_jupiter_quote { quote_price_impact_bps } else { _cfg.strategy.execution.paper_slippage_bps as f64 };
    let sol_at_no_slippage = if signal.entry_price_usd > 0.0 {
        signal.sol_spent * (signal.current_price / signal.entry_price_usd) * sell_fraction
    } else {
        signal.sol_spent * sell_fraction
    };
    let slippage_cost = (sol_at_no_slippage - sol_received_this_exit - sim_total_fees).max(0.0);
    let exit_ms = exit_start.elapsed().as_millis() as i64;
    let supabase_bg = supabase.clone();
    let mint_bg = signal.mint.clone();
    let reason_bg = signal.reason.to_string();
    let position_id_bg = signal.position_id;
    let entry_sol_bg = original_sol_spent;
    tokio::spawn(async move {
        let cost_payload = serde_json::json!({
            "position_id": position_id_bg,
            "mint": mint_bg,
            "side": "sell",
            "is_paper_trade": true,
            "sol_amount": sol_received_this_exit,
            "token_price_usd": exit_price,
            "network_fee_sol": sim_network_fee,
            "priority_fee_sol": sim_priority_fee,
            "jito_tip_sol": 0.0,
            "helius_tip_sol": sim_tip,
            "total_fees_sol": sim_total_fees,
            "slippage_bps": effective_slippage_bps,
            "expected_sol": sol_at_no_slippage,
            "actual_sol": sol_received_this_exit,
            "slippage_cost_sol": slippage_cost,
            "entry_sol_spent": entry_sol_bg,
            "exit_sol_received": sol_received_this_exit,
            "total_round_trip_fees_sol": sim_total_fees,
            "gross_pnl_sol": sol_received_this_exit + sim_total_fees - (entry_sol_bg * sell_fraction),
            "net_pnl_sol": total_pnl_sol,
            "net_pnl_pct": total_pnl_pct,
            "exit_reason": reason_bg,
            "tx_sig": if used_jupiter_quote { "paper_jupiter_quote" } else { "paper_dexscreener_fallback" },
            "execution_ms": exit_ms,
        });
        log_trade_cost(&supabase_bg, &cost_payload).await;
    });

    Ok(())
}

// ─── Paper exit fallback pricing (DexScreener + flat slippage) ────────

/// Fallback pricing when Jupiter quote fails: DexScreener price + configured slippage.
async fn paper_exit_fallback_pricing(
    cfg: &AppConfig,
    jupiter: &JupiterClient,
    signal: &ExitSignal,
    sell_fraction: f64,
    sim_total_fees: f64,
) -> (f64, f64) {
    let effective_price = match jupiter.get_price(&signal.mint).await {
        Ok(fresh) if fresh > 0.0 => fresh,
        _ => signal.current_price,
    };

    let exit_price = if cfg.strategy.execution.paper_slippage_bps > 0 {
        let slippage_pct = cfg.strategy.execution.paper_slippage_bps as f64 / 10_000.0;
        effective_price * (1.0 - slippage_pct)
    } else {
        effective_price
    };

    let sol_received = if signal.entry_price_usd > 0.0 {
        let sol_value = signal.sol_spent * (exit_price / signal.entry_price_usd) * sell_fraction;
        (sol_value - sim_total_fees).max(0.0)
    } else {
        (signal.sol_spent * sell_fraction - sim_total_fees).max(0.0)
    };

    (exit_price, sol_received)
}

// ─── Real exit ───────────────────────────────────────────────

async fn execute_real_exit(
    cfg: &AppConfig,
    jupiter: &JupiterClient,
    supabase: &SupabaseClient,
    rpc: &RpcClient,
    backup_rpc: &RpcClient,
    wallet: &BotWallet,
    signal: &ExitSignal,
    trading_state: &TradingState,
    jito_client: Option<&JitoClient>,
    helius_sender: Option<&HeliusSenderClient>,
) -> Result<()> {
    let exit_start = std::time::Instant::now();
    let sol_mint = crate::execution::jupiter::SOL_MINT;

    // v5.3 latency fix: parallelize pre-sell work.
    // Previously, balance fetch → priority-fee HTTP were serialised per retry,
    // adding 0.5–2s per attempt. Now we run the balance fetch + SOL snapshot +
    // one priority-fee estimate concurrently, once per exit, and reuse the fee
    // across all retry tiers.
    let wallet_pk = wallet.pubkey();
    let fee_account_keys: [&str; 3] = [
        &signal.mint,
        crate::execution::jupiter::SOL_MINT,
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    ];
    let balance_fut = fetch_exit_token_balance(rpc, &wallet_pk, &signal.mint);
    let sol_fut = rpc.get_balance(&wallet_pk);
    // getRecentPrioritizationFees is standard Solana RPC — use Chainstack primary.
    let fee_fut = crate::execution::helius_sender::get_priority_fee_estimate(
        &cfg.env.solana_rpc_url,
        &fee_account_keys,
        &cfg.strategy.execution.priority_level,
        cfg.strategy.execution.priority_fee_max_lamports,
    );
    let (on_chain_balance_opt, sol_balance_res, dynamic_exit_fee) =
        tokio::join!(balance_fut, sol_fut, fee_fut);

    let exit_priority_fee = dynamic_exit_fee.min(cfg.strategy.execution.priority_fee_max_lamports);

    // Snapshot wallet balance before exit for cost tracking.
    // If confirmed tx metadata is available later, prefer that authoritative value.
    let pre_exit_balance_snapshot = match sol_balance_res {
        Ok(lamports) => Some(solana_sdk::native_token::lamports_to_sol(lamports)),
        Err(_) => None,
    };

    // ── Resolve token amount: always verify against on-chain balance ──
    // The DB/signal amount can be stale or slightly off (transfer taxes,
    // rounding).  Trying to sell more tokens than we hold causes 6024.
    let effective_token_amount = {
        let signal_amount = signal.token_amount;
        let on_chain = on_chain_balance_opt;

        match on_chain {
            Some(balance) if balance > 0.0 => {
                if signal_amount > 0.0 {
                    if balance + 1.0 < signal_amount {
                        warn!(
                            mint = %signal.mint,
                            signal_amount,
                            on_chain_balance = balance,
                            "⚠️ On-chain balance below tracked position size — capping exit to wallet balance"
                        );
                        balance
                    } else {
                        if balance - signal_amount > 1.0 {
                            warn!(
                                mint = %signal.mint,
                                signal_amount,
                                on_chain_balance = balance,
                                extra_wallet_tokens = balance - signal_amount,
                                "⚠️ Wallet holds extra same-mint tokens outside the tracked position — capping exit to tracked position size"
                            );
                        }
                        signal_amount
                    }
                } else {
                    balance
                }
            }
            _ if signal_amount > 0.0 => {
                warn!(
                    mint = %signal.mint,
                    signal_amount,
                    "Could not fetch on-chain balance — falling back to signal amount"
                );
                signal_amount
            }
            _ => {
                error!(
                    mint = %signal.mint,
                    "On-chain token balance is 0 and signal amount is 0 — nothing to sell"
                );
                return Err(anyhow::anyhow!("No tokens to sell — on-chain balance is 0"));
            }
        }
    };

    let sell_fraction = signal.pct_to_sell as f64 / 100.0;
    let tokens_to_sell_f64 = effective_token_amount * sell_fraction;

    if tokens_to_sell_f64 < 0.0 || tokens_to_sell_f64 > u64::MAX as f64 {
        error!(
            mint = %signal.mint,
            token_amount = effective_token_amount,
            "Token amount overflow — aborting exit"
        );
        return Err(anyhow::anyhow!("Token amount overflow: {}", effective_token_amount));
    }

    // token_amount is already in raw smallest units (e.g. pump.fun 6 decimals)
    // — no conversion needed.
    let tokens_to_sell = tokens_to_sell_f64 as u64;

    if tokens_to_sell == 0 {
        warn!(mint = %signal.mint, "tokens_to_sell is 0 — aborting exit");
        return Err(anyhow::anyhow!("tokens_to_sell is 0 after conversion"));
    }

    // ── Phase 1: Find a route (own retry counter, doesn't consume slippage attempts) ──
    // v5.3 latency fix: route retry delay slashed from 4s → 400ms. The 4s wait
    // was the single biggest exit-side time sink when Jupiter briefly returned
    // NO_ROUTES_FOUND or 429. 3 retries × 400ms = 1.2s max, vs 12s before.
    const ROUTE_RETRIES: u32 = 3;
    const ROUTE_RETRY_DELAY_MS: u64 = 400;

    // ── Phase 2: Quote → Simulate → Sign → Submit → Confirm with escalating slippage ──
    //   Slippage escalates: 2000 → 3500 → 6000 → 9000 (capped at 90%)
    // v5.1: widened from 3→4 tiers. Log analysis 2026-04-17 showed repeated
    // Custom(6024) SlippageToleranceExceeded failures during active dumps even
    // at 5000 bps. Extra 6000/9000 tiers give one last push to land before
    // bailing and marking position `exit_failed` for the next trigger.
    const EXIT_RETRIES: u32 = 4;

    let mut final_tx_sig = None;
    let mut final_quote = None;
    let mut final_slippage_bps: u64 = 0;
    let mut final_attempt: u32 = 0;
    let mut final_tokens_sold: u64 = 0;
    let mut final_priority_fee_lamports: u64 = 0;
    let mut last_exit_err: Option<anyhow::Error> = None;
    let mut kill_switch_triggered = false;

    'retry: for attempt in 1..=EXIT_RETRIES {
        // Kill-switch: if the previous attempt triggered max realized loss,
        // do NOT retry with higher slippage — it only makes the fill worse.
        if attempt > 1 && kill_switch_triggered {
            warn!(
                mint = %signal.mint,
                "🛑 Kill-switch active — skipping retry #{} (higher slippage would increase loss)",
                attempt
            );
            break;
        }

        // Progressive retry delays: minimal — speed is critical during exits
        // v5.3: tightened from 0/1/1/2s → 0/300/400/600ms. Each retry re-quotes,
        // so we aren't re-hitting stale data; the delay is only to avoid
        // hammering Jupiter/RPC. Median sell latency was 13.7s and these
        // serialised backoffs were a big chunk of it.
        let retry_delay_ms: u64 = match attempt {
            1 => 0,
            2 => 300,
            3 => 400,
            _ => 600,
        };

        // Escalate slippage: 20% → 35% → 60% → 90% on retries
        //
        // v5.2 (2026-04-18) Slippage cap for `PostBuyVerificationFailed`:
        // These exits fire AFTER we've confirmed the token is a rug (honeypot,
        // mint authority not revoked, etc.). Escalating to 60-90% slippage
        // throws good money after bad — audit showed 30 such exits costing
        // -0.24 SOL where most of the loss was slippage on a token already
        // confirmed dead. Cap at 35% (tier 2) for these; if the LP is too
        // thin to fill at 35%, we accept the bag instead of bleeding more.
        let is_rug_confirmed_exit = matches!(
            signal.reason,
            crate::monitoring::types::ExitReason::PostBuyVerificationFailed
        );
        let slippage_bps: u64 = match attempt {
            1 => cfg.strategy.execution.slippage_bps.max(2000),
            2 => 3500,
            3 => if is_rug_confirmed_exit { 3500 } else { 6000 },
            4 => if is_rug_confirmed_exit { 3500 } else { 9000 },
            _ => 9000, // emergency cap — never exceed 90%
        };

        if attempt > 1 {
            warn!(
                mint = %signal.mint,
                attempt,
                slippage_bps,
                tokens_to_sell,
                "🔄 Retrying exit with escalated slippage"
            );
        }

        // Step 1: Quote — separate route-finding loop with its own retries
        let mut quote = None;
        for route_attempt in 1..=ROUTE_RETRIES {
            match jupiter
                .get_quote(
                    &signal.mint,
                    sol_mint,
                    tokens_to_sell,
                    slippage_bps,
                )
                .await
            {
                Ok(q) => {
                    if route_attempt > 1 {
                        info!(
                            mint = %signal.mint,
                            attempt,
                            route_attempt,
                            "✅ Jupiter sell route found on retry"
                        );
                    }
                    quote = Some(q);
                    break;
                }
                Err(e) if e.to_string().contains("NO_ROUTES_FOUND") => {
                    warn!(
                        mint = %signal.mint,
                        attempt,
                        route_attempt,
                        "⏳ Jupiter sell route not found — retrying in {}ms",
                        ROUTE_RETRY_DELAY_MS
                    );
                    last_exit_err = Some(e);
                    if route_attempt < ROUTE_RETRIES {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            ROUTE_RETRY_DELAY_MS,
                        ))
                        .await;
                    }
                }
                Err(e) if e.to_string().contains("429") || e.to_string().contains("Too Many Requests") => {
                    warn!(
                        mint = %signal.mint,
                        attempt,
                        route_attempt,
                        "⏳ Jupiter rate-limited (429) — retrying in {}ms",
                        ROUTE_RETRY_DELAY_MS
                    );
                    last_exit_err = Some(e);
                    if route_attempt < ROUTE_RETRIES {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            ROUTE_RETRY_DELAY_MS,
                        ))
                        .await;
                    }
                }
                Err(e) => {
                    // Non-retryable quote error
                    return Err(e);
                }
            }
        }

        let quote = match quote {
            Some(q) => q,
            None => {
                // All route retries exhausted for this slippage level — try next
                warn!(
                    mint = %signal.mint,
                    attempt,
                    "No routes found after {} route retries — escalating slippage",
                    ROUTE_RETRIES
                );
                if attempt < EXIT_RETRIES {
                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                    .await;
                }
                continue;
            }
        };

        // ── Quote sanity check: verify the trade is still worth executing ──
        // The quote's out_amount is the REAL on-chain price, not a stale API estimate.
        // For take-profit exits: reject if we'd actually lose money (stale trigger).
        // For protective exits (stop-loss, trailing, time): always execute.
        let quote_out_lamports: f64 = quote.out_amount.parse().unwrap_or(0.0);
        let quote_sol_value = quote_out_lamports / 1_000_000_000.0;
        // Use the ORIGINAL token amount (signal.token_amount) as denominator
        // so that after partial TP sells, cost_for_chunk reflects only the
        // proportional cost of the tokens being sold — not the full position.
        let original_token_amount = if signal.token_amount > 0.0 {
            signal.token_amount
        } else {
            effective_token_amount
        };
        let cost_for_chunk = if original_token_amount > 0.0 {
            signal.sol_spent * (tokens_to_sell as f64 / original_token_amount)
        } else {
            signal.sol_spent
        };
        let effective_return_ratio = if cost_for_chunk > 0.0 {
            quote_sol_value / cost_for_chunk
        } else {
            1.0
        };

        let is_protective_exit = matches!(
            signal.reason,
            crate::monitoring::types::ExitReason::StopLoss
                | crate::monitoring::types::ExitReason::TrailingStop
                | crate::monitoring::types::ExitReason::TimeStop
                | crate::monitoring::types::ExitReason::DevWalletDumping
                | crate::monitoring::types::ExitReason::LiquidityRemoved
                | crate::monitoring::types::ExitReason::DipDeath
                | crate::monitoring::types::ExitReason::PostFillSanity
                | crate::monitoring::types::ExitReason::PostBuyVerificationFailed
                | crate::monitoring::types::ExitReason::MomentumKill
        );

        let is_take_profit = matches!(
            signal.reason,
            crate::monitoring::types::ExitReason::TakeProfit1
                | crate::monitoring::types::ExitReason::TakeProfit2
                | crate::monitoring::types::ExitReason::TakeProfit3
        );

        // TimeStop is now a protective exit — no separate slippage guard.
        // It will always execute, preventing stuck positions.

        // Take-profit guard: TP exits must NEVER execute at a loss.
        // The trigger fires based on API/oracle price, but actual on-chain
        // execution can have massive slippage in low-liquidity pools.
        // Require ratio >= 1.0 so we at least break even on the chunk.
        if is_take_profit && effective_return_ratio < 1.0 {
            warn!(
                mint = %signal.mint,
                attempt,
                effective_return_ratio = format!("{:.3}", effective_return_ratio),
                quote_sol = format!("{:.6}", quote_sol_value),
                cost_for_chunk = format!("{:.6}", cost_for_chunk),
                "⚠️ Take-profit would execute at a loss due to slippage — aborting."
            );
            return Err(anyhow::anyhow!(
                "TP sanity check failed: ratio={:.3}, slippage too high to profit",
                effective_return_ratio
            ));
        }

        if !is_protective_exit && !is_take_profit && effective_return_ratio < 0.95 {
            warn!(
                mint = %signal.mint,
                attempt,
                effective_return_ratio = format!("{:.3}", effective_return_ratio),
                quote_sol = format!("{:.6}", quote_sol_value),
                cost_for_chunk = format!("{:.6}", cost_for_chunk),
                "⚠️ Quote sanity check failed — sell would lose money. Aborting exit."
            );
            return Err(anyhow::anyhow!(
                "Quote sanity check failed: ratio={:.3}, would lose money",
                effective_return_ratio
            ));
        }

        debug!(
            mint = %signal.mint,
            attempt,
            effective_return_ratio = format!("{:.3}", effective_return_ratio),
            quote_sol = format!("{:.6}", quote_sol_value),
            "✅ Quote sanity check passed"
        );

        // ── Max realized loss kill-switch ──────────────────────────
        // If the quote already shows a loss exceeding max_realized_loss_pct,
        // force-execute THIS attempt immediately.  DO NOT escalate slippage
        // further — higher slippage only makes the fill worse.
        let max_realized_loss = cfg.strategy.exit.max_realized_loss_pct;
        let realized_loss_pct = if cost_for_chunk > 0.0 {
            (1.0 - effective_return_ratio) * 100.0
        } else {
            0.0
        };
        let force_execute = max_realized_loss > 0.0
            && realized_loss_pct > max_realized_loss
            && is_protective_exit;

        if force_execute {
            warn!(
                mint = %signal.mint,
                attempt,
                realized_loss_pct = format!("{:.1}%", realized_loss_pct),
                max_realized_loss = format!("{:.1}%", max_realized_loss),
                effective_return_ratio = format!("{:.3}", effective_return_ratio),
                "🛑 Realized loss exceeds {:.0}% cap — force-executing at current slippage (no further escalation)",
                max_realized_loss
            );
            // Execute this attempt, but mark that we should NOT retry with higher
            // slippage if it fails. Higher slippage = worse fill = bigger loss.
            kill_switch_triggered = true;
        }

        // Step 2: Swap transaction — use dynamicSlippage range so Jupiter
        //   can optimise the on-chain tolerance.  minBps = this attempt's
        //   level, maxBps = 10000 (100%) so the tx can always land.
        //   v5.3: priority fee is now fetched once up front (outside the
        //   retry loop) and reused — the previous per-attempt HTTP call
        //   added 0.5–2s to every retry.

        let swap_tx_b64 = match jupiter
            .get_swap_transaction(
                &quote.raw,
                &wallet.pubkey().to_string(),
                Some((slippage_bps, 10000)),
                Some((exit_priority_fee, &cfg.strategy.execution.priority_level)),
            )
            .await
        {
            Ok(tx) => tx,
            Err(e) => {
                warn!(mint = %signal.mint, attempt, "Swap transaction build failed: {} — retrying", e);
                last_exit_err = Some(e);
                if attempt < EXIT_RETRIES {
                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                    .await;
                }
                continue;
            }
        };

        // Step 3: Decode (no tip injection — Chainstack Warp handles routing)
        let tx_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &swap_tx_b64,
        )
        .map_err(|e| anyhow::anyhow!("Failed to decode exit tx: {}", e))?;

        let mut versioned_tx: solana_sdk::transaction::VersionedTransaction =
            bincode::deserialize(&tx_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize exit tx: {}", e))?;

        // Skip simulation entirely for exits — it burns retries without
        // hitting the chain.  A failed on-chain tx costs ~0.000005 SOL;
        // a failed exit costs the entire position.  With dynamicSlippage
        // enabled on the Jupiter swap request, on-chain tolerance is
        // already optimised.
        debug!(mint = %signal.mint, attempt, "⏩ Skipping simulation — submitting directly to chain");

        // Step 4: Sign
        let mut signed_tx = versioned_tx.clone();
        signed_tx.signatures[0] = wallet.sign_transaction(&versioned_tx)?;

        if signed_tx.signatures[0] == solana_sdk::signature::Signature::default() {
            error!(mint = %signal.mint, "Exit tx not signed — aborting");
            anyhow::bail!("Exit tx not signed");
        }

        // Step 5: Submit (Helius Sender, Jito bundle, or regular RPC)
        let tx_sig = if cfg.env.use_helius_sender {
            let hs = helius_sender.ok_or_else(|| anyhow::anyhow!("Warp TX sender enabled but client not initialized"))?;

            // Dual-submit: fire-and-forget same signed tx to backup RPC for redundancy.
            let backup_tx = signed_tx.clone();
            let backup_url = cfg.env.solana_rpc_backup_url.clone();
            let mint_log = signal.mint.clone();
            tokio::spawn(async move {
                let backup = solana_client::nonblocking::rpc_client::RpcClient::new(backup_url);
                let cfg = solana_client::rpc_config::RpcSendTransactionConfig {
                    skip_preflight: true,
                    ..Default::default()
                };
                match backup.send_transaction_with_config(&backup_tx, cfg).await {
                    Ok(sig) => tracing::debug!(mint = %mint_log, sig = %sig, "📤 Exit dual-submit: backup RPC accepted"),
                    Err(e) => tracing::debug!(mint = %mint_log, "Exit dual-submit backup failed (non-critical): {}", e),
                }
            });

            match hs.send_transaction(&signed_tx).await {
                Ok(sig) => {
                    info!(mint = %signal.mint, sig = %sig, "📤 Exit tx submitted via Chainstack Warp (+ backup)");
                    sig
                }
                Err(e) => {
                    warn!(mint = %signal.mint, attempt, "Exit Warp TX submission failed: {} — retrying", e);
                    last_exit_err = Some(e);
                    if attempt < EXIT_RETRIES {
                        tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                        .await;
                    }
                    continue;
                }
            }
        } else if cfg.env.use_jito {
            match submit_exit_via_jito(cfg, wallet, signed_tx, &signal.mint, supabase, jito_client).await {
                Ok(sig) => sig,
                Err(e) => {
                    warn!(mint = %signal.mint, attempt, "Exit Jito submission failed: {} — retrying", e);
                    last_exit_err = Some(e);
                    if attempt < EXIT_RETRIES {
                        tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                        .await;
                    }
                    continue;
                }
            }
        } else {
            match submit_exit_via_rpc(rpc, &signed_tx, &signal.mint, true).await {
                Ok(sig) => sig,
                Err(e) => {
                    let err_str = e.to_string();
                    // If primary RPC is rate-limited (429), try backup RPC
                    if err_str.contains("429") || err_str.contains("Too Many Requests") {
                        warn!(mint = %signal.mint, attempt, "Primary RPC 429 — trying backup RPC");
                        match submit_exit_via_rpc(backup_rpc, &signed_tx, &signal.mint, true).await {
                            Ok(sig) => sig,
                            Err(e2) => {
                                warn!(mint = %signal.mint, attempt, "Backup RPC also failed: {} — retrying", e2);
                                last_exit_err = Some(e2);
                                if attempt < EXIT_RETRIES {
                                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                                    .await;
                                }
                                continue;
                            }
                        }
                    } else {
                        warn!(mint = %signal.mint, attempt, "Exit RPC submission failed: {} — retrying", e);
                        last_exit_err = Some(e);
                        if attempt < EXIT_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                            .await;
                        }
                        continue;
                    }
                }
            }
        };

        // Step 6: Confirm
        let confirm_start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(cfg.strategy.execution.tx_confirm_timeout_secs);
        let poll = std::time::Duration::from_millis(cfg.strategy.execution.tx_confirm_poll_ms);
        let mut confirmed = false;

        while confirm_start.elapsed() < timeout {
            match rpc.get_signature_statuses(&[tx_sig]).await {
                Ok(statuses) => {
                    if let Some(Some(status)) = statuses.value.first() {
                        if status.err.is_none() {
                            confirmed = true;
                            break;
                        } else {
                            // On-chain failure — check if slippage, then retry with fresh quote
                            let err_str = format!("{:?}", status.err);
                            if is_slippage_error(&err_str) {
                                warn!(
                                    mint = %signal.mint,
                                    attempt,
                                    error = %err_str,
                                    "⏳ Exit tx confirmed with slippage error on-chain — re-quoting",
                                );
                                last_exit_err = Some(anyhow::anyhow!("Exit tx on-chain slippage: {}", err_str));
                                if attempt < EXIT_RETRIES {
                                    tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                                    .await;
                                }
                                continue 'retry;
                            } else {
                                error!(mint = %signal.mint, "Exit tx confirmed with non-retryable error: {:?}", status.err);
                                anyhow::bail!("Exit tx confirmed with error: {:?}", status.err);
                            }
                        }
                    }
                }
                Err(e) => debug!("Exit confirmation poll error: {}", e),
            }
            tokio::time::sleep(poll).await;
        }

        if !confirmed {
            warn!(mint = %signal.mint, sig = %tx_sig, attempt, "⏰ Exit tx timed out — retrying with fresh quote");
            last_exit_err = Some(anyhow::anyhow!("Exit tx timed out after confirmation period"));
            if attempt < EXIT_RETRIES {
                tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms.max(100)))
                .await;
            }
            continue;
        }

        info!(mint = %signal.mint, sig = %tx_sig, attempt, slippage_bps, tokens_to_sell, "✅ Exit tx confirmed");
        final_tx_sig = Some(tx_sig);
        final_quote = Some(quote);
        final_slippage_bps = slippage_bps;
        final_attempt = attempt;
        final_tokens_sold = tokens_to_sell;
        final_priority_fee_lamports = exit_priority_fee;
        break;
    }

    let tx_sig = match final_tx_sig {
        Some(sig) => sig,
        None => {
            return Err(last_exit_err.unwrap_or_else(|| {
                anyhow::anyhow!("Exit failed after {} retries", EXIT_RETRIES)
            }));
        }
    };
    let quote = final_quote.expect("quote must exist if tx_sig exists");
    let confirmed_exit = fetch_confirmed_exit_outcome(
        rpc,
        &tx_sig,
        &wallet.pubkey(),
        &signal.mint,
        0.0, // No tip injection with Chainstack Warp
    )
    .await;
    let tx_pre_tokens = confirmed_exit
        .as_ref()
        .and_then(|outcome| outcome.wallet_pre_tokens);
    let tx_post_tokens = confirmed_exit
        .as_ref()
        .and_then(|outcome| outcome.wallet_post_tokens);
    let tx_sol_received = confirmed_exit
        .as_ref()
        .and_then(|outcome| outcome.sol_received);

    // Step 7: Update DB — only runs after confirmed on-chain
    // ── Verify actual on-chain balance to get true remaining tokens ──
    //
    // Retry-on-no-change: get_signature_statuses can return `Ok(err=None)`
    // while the balance-read RPC node hasn't applied the block yet, OR the
    // inner swap instruction could have silently no-op'd. If we requested
    // a full exit (100%) and the first balance read shows the same tokens
    // as before, retry with progressive delays. This distinguishes
    // "RPC propagation lag" from "swap truly did not execute".
    //
    // v5.1 fix: previous code did 1 retry at 1.5s and silently ignored
    // `None` returns (which mean balance=0 / account closed = SUCCESS).
    // Jupiter closes the token account on full sell, so `None` after a
    // stale `Some(full_balance)` is the correct outcome. Ignoring it
    // caused false swap_no_op → exit_failed with sol_received=0 on
    // 7 confirmed-successful exits (IDs 94-100, 2026-04-17).
    // v5.2 fix: prefer confirmed tx metadata over a follow-up token-account
    // RPC read. Signature confirmation can arrive before account-index updates,
    // which still produces false `exit_failed` rows on successful sells.
    let mut remaining_tokens = if let (Some(pre_tokens), Some(post_tokens)) = (tx_pre_tokens, tx_post_tokens) {
        let wallet_tokens_sold = (pre_tokens - post_tokens).max(0.0);
        let scoped_tokens_sold = wallet_tokens_sold.min(effective_token_amount);
        if pre_tokens - effective_token_amount > 1.0 {
            debug!(
                mint = %signal.mint,
                tracked_position_tokens = effective_token_amount,
                wallet_pre_tokens = pre_tokens,
                wallet_post_tokens = post_tokens,
                "Confirmed tx included extra same-mint wallet balance outside the tracked position"
            );
        }
        (effective_token_amount - scoped_tokens_sold).max(0.0)
    } else if let Some(balance) = tx_post_tokens {
        balance
    } else {
        match fetch_exit_token_balance(rpc, &wallet.pubkey(), &signal.mint).await {
            Some(balance) => balance,
            None => {
                let calc = effective_token_amount - final_tokens_sold as f64;
                if calc < 0.0 { 0.0 } else { calc }
            }
        }
    };
    if tx_post_tokens.is_none()
        && signal.pct_to_sell == 100
        && remaining_tokens >= effective_token_amount * 0.99
    {
        // RPC propagation lag: retry up to 3 times with increasing delays.
        // Helius RPC can take 2-8s to reflect balance changes after a
        // confirmed tx, especially under load.
        let retry_delays_ms: [u64; 3] = [2000, 3000, 5000];
        for (i, delay_ms) in retry_delays_ms.iter().enumerate() {
            tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
            match fetch_exit_token_balance(rpc, &wallet.pubkey(), &signal.mint).await {
                Some(fresh) if fresh < remaining_tokens => {
                    debug!(
                        mint = %signal.mint,
                        retry = i + 1,
                        first_read = remaining_tokens,
                        fresh_read = fresh,
                        "On-chain balance updated after RPC propagation delay"
                    );
                    remaining_tokens = fresh;
                    break;
                }
                None => {
                    // None = balance is 0 / token account closed by Jupiter.
                    // This is the SUCCESS case for a full exit.
                    debug!(
                        mint = %signal.mint,
                        retry = i + 1,
                        prev_read = remaining_tokens,
                        "On-chain balance is 0 (account closed) after RPC delay — tokens fully sold"
                    );
                    remaining_tokens = 0.0;
                    break;
                }
                _ => {
                    // Balance unchanged — RPC still stale, retry
                    debug!(
                        mint = %signal.mint,
                        retry = i + 1,
                        balance = remaining_tokens,
                        "Balance unchanged after {}ms — retrying", delay_ms
                    );
                }
            }
        }
    }
    debug!(
        mint = %signal.mint,
        on_chain_balance = remaining_tokens,
        calculated_remaining = effective_token_amount - final_tokens_sold as f64,
        "On-chain token balance after exit"
    );

    // ── Derive ACTUAL tokens sold + SOL received from on-chain truth ──
    // Bug history: trusting quote.out_amount + signal.pct_to_sell caused
    // "closed" positions with thousands of tokens still in wallet (F1pp
    // ASTEROID: bot filled 72%, recorded 100%; user then sold remainder
    // for 57x manually). Fix: always reconcile against the confirmed tx.
    let actual_tokens_sold = if let (Some(pre_tokens), Some(post_tokens)) = (tx_pre_tokens, tx_post_tokens) {
        (pre_tokens - post_tokens).max(0.0).min(effective_token_amount)
    } else {
        (effective_token_amount - remaining_tokens).max(0.0)
    };
    let quote_out_sol = quote.out_amount.parse::<f64>().unwrap_or(0.0) / 1_000_000_000.0;
    // If fill ratio is good (>=99% of requested), trust quote out_amount as
    // it's usually more accurate than wallet-delta-minus-fees. If partial,
    // scale proportionally (best-effort — still better than quoted amount).
    let requested_tokens = final_tokens_sold as f64;
    let fill_ratio = if requested_tokens > 0.0 {
        actual_tokens_sold / requested_tokens
    } else {
        1.0
    };
    let sol_received_this_exit = if let Some(sol_received) = tx_sol_received {
        sol_received
    } else if fill_ratio >= 0.99 {
        quote_out_sol
    } else {
        // Partial fill: scale quote proportionally. Not perfect but closer
        // than the raw quote_out_sol which assumed full fill.
        quote_out_sol * fill_ratio
    };

    // Use actual tokens sold for accurate accounting (may be less than requested on partial fills)
    let actual_sell_fraction = if effective_token_amount > 0.0 {
        actual_tokens_sold / effective_token_amount
    } else {
        sell_fraction
    };
    let pnl_sol_this_exit = sol_received_this_exit - (signal.sol_spent * actual_sell_fraction);
    let pnl_pct = if signal.entry_price_usd > 0.0 {
        (signal.current_price - signal.entry_price_usd) / signal.entry_price_usd * 100.0
    } else {
        0.0
    };

    // ── Full-exit detection: on-chain truth only ──
    // Dust threshold: max(1 token, 0.5% of original position). This prevents
    // marking "closed" when sizable balances remain due to partial fills,
    // while still closing positions whose remainder is truly unsellable dust.
    let dust_threshold = (effective_token_amount * 0.005).max(1.0);
    let is_full_exit = remaining_tokens <= dust_threshold;

    // Loud warning on suspected partial fill (bot asked for 100%, got less).
    // No retry here — retries can block other trades. Position stays as
    // `partial` (or `exit_failed` for zero-fill) with true remaining_tokens;
    // operator or monitor handles it.
    //
    // Zero-fill detection: if fill_ratio < 1%, the swap instruction inside
    // the tx almost certainly reverted / no-op'd despite signature confirm
    // (Jupiter route slippage not met, stale pool state, etc.). Distinguish
    // from genuine partial fills (e.g. 60%) so we can alert / retry logic.
    let swap_no_op = signal.pct_to_sell == 100 && fill_ratio < 0.01;
    if signal.pct_to_sell == 100 && !is_full_exit {
        if swap_no_op {
            warn!(
                mint = %signal.mint,
                position_id = signal.position_id,
                requested_tokens = final_tokens_sold,
                remaining = remaining_tokens,
                fill_ratio_pct = format!("{:.2}%", fill_ratio * 100.0),
                tx_sig = %tx_sig,
                "💥 SWAP NO-OP — tx confirmed but sold ~0 tokens (likely silent revert); position marked `exit_failed`"
            );
        } else {
            warn!(
                mint = %signal.mint,
                position_id = signal.position_id,
                requested_tokens = final_tokens_sold,
                actual_sold = actual_tokens_sold,
                remaining = remaining_tokens,
                fill_ratio_pct = format!("{:.1}%", fill_ratio * 100.0),
                tx_sig = %tx_sig,
                "⚠️ PARTIAL FILL on 100%-sell request — position marked `partial`, NOT closed"
            );
        }
    }

    let new_status = if is_full_exit {
        "closed"
    } else if swap_no_op {
        "exit_failed"
    } else {
        "partial"
    };

    // ── Fetch existing DB values to ACCUMULATE, not overwrite ──
    let pos_state = fetch_position_exit_state(supabase, signal.position_id).await;

    // Guard: if position is already closed, skip this exit to prevent double-counting
    if pos_state.status == "closed" {
        warn!(
            mint = %signal.mint,
            position_id = signal.position_id,
            "⏭️ Real exit skipped — position already closed (duplicate signal)"
        );
        return Ok(());
    }

    // Use original sol_spent from DB (not signal.sol_spent which gets halved after TP1 partial exit)
    let original_sol_spent = if pos_state.sol_spent > 0.0 { pos_state.sol_spent } else { signal.sol_spent };

    let prev_sol_received = pos_state.sol_received;
    let prev_pnl_sol = pos_state.pnl_sol;
    let total_sol_received = prev_sol_received + sol_received_this_exit;
    // Total PnL = total received - original cost (from DB, never halved)
    let total_pnl_sol = total_sol_received - original_sol_spent;
    let total_pnl_pct = if original_sol_spent > 0.0 {
        (total_sol_received - original_sol_spent) / original_sol_spent * 100.0
    } else {
        0.0
    };

    // Sanitize: NaN/Infinity → 0.0 (safety net for upstream math errors)
    let total_pnl_pct = sanitize_f64(total_pnl_pct, "total_pnl_pct", &signal.mint);
    let total_pnl_sol = sanitize_f64(total_pnl_sol, "total_pnl_sol", &signal.mint);
    let total_sol_received = sanitize_f64(total_sol_received, "total_sol_received", &signal.mint);

    let url = format!(
        "{}/positions?id=eq.{}",
        supabase.base_url, signal.position_id
    );

    // Build exit reason with retry/slippage context and (when present) a sub-reason
    // tag — e.g. `dip_death:whale_sell_during_dip (attempt=2, slippage_bps=3000)`.
    // The sub-reason is essential for tuning dip_death sub-triggers post-hoc.
    let base_reason = match &signal.sub_reason {
        Some(sr) => format!("{}:{}", signal.reason, sr),
        None => signal.reason.to_string(),
    };
    let exit_reason_str = if final_attempt > 1 {
        format!("{} (attempt={}, slippage_bps={})", base_reason, final_attempt, final_slippage_bps)
    } else {
        base_reason
    };

    let mut payload = serde_json::json!({
        "status": new_status,
        "exit_tx_sig": tx_sig.to_string(),
        "exit_price_usd": signal.current_price,
        "exit_reason": exit_reason_str,
        "exit_slippage_bps": final_slippage_bps,
        "exit_attempts": final_attempt,
        "pnl_pct": total_pnl_pct,
        "pnl_sol": total_pnl_sol,
        "sol_received": total_sol_received,
        "token_amount": remaining_tokens,
    });

    // Write TP flags to DB only after confirmed on-chain sell
    match &signal.reason {
        crate::monitoring::types::ExitReason::TakeProfit1 => {
            payload.as_object_mut().unwrap().insert("tp1_triggered".to_string(), serde_json::Value::Bool(true));
        }
        crate::monitoring::types::ExitReason::TakeProfit2 => {
            payload.as_object_mut().unwrap().insert("tp2_triggered".to_string(), serde_json::Value::Bool(true));
        }
        _ => {}
    }

    if is_full_exit {
        let now = chrono::Utc::now().to_rfc3339();
        payload.as_object_mut().unwrap().insert(
            "exit_time".to_string(),
            serde_json::Value::String(now.clone()),
        );
        payload.as_object_mut().unwrap().insert(
            "closed_at".to_string(),
            serde_json::Value::String(now),
        );
        if let Some(hold_secs) = compute_hold_duration(&pos_state.created_at) {
            payload.as_object_mut().unwrap().insert(
                "hold_duration_secs".to_string(),
                serde_json::json!(hold_secs),
            );
        }
    }

    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to update position on exit: {}", body);
        }
        Err(e) => {
            warn!("Failed to update position on exit: {}", e);
        }
    }

    let pnl_symbol = if total_pnl_sol >= 0.0 { "+" } else { "" };
    println!(
        "[{}] {} mint={} | reason={} | pnl={}{:.2}% | pnl={}{:.4} SOL | sold={}% | this_exit={:.4} SOL",
        chrono::Utc::now().format("%H:%M:%S"),
        if is_full_exit { "🏁 REAL EXIT" } else { "📤 REAL PARTIAL" },
        signal.mint,
        signal.reason,
        pnl_symbol, total_pnl_pct,
        pnl_symbol, total_pnl_sol,
        signal.pct_to_sell,
        sol_received_this_exit,
    );

    // Log wallet SOL balance after exit
    let pre_exit_balance = confirmed_exit
        .as_ref()
        .and_then(|outcome| outcome.wallet_pre_sol)
        .or(pre_exit_balance_snapshot);
    let post_exit_balance = if let Some(bal) = confirmed_exit
        .as_ref()
        .and_then(|outcome| outcome.wallet_post_sol)
    {
        info!(
            mint = %signal.mint,
            balance_sol = format!("{:.4}", bal),
            "💰 Wallet SOL balance after EXIT"
        );
        println!("  💰 Wallet balance: {:.4} SOL", bal);
        Some(bal)
    } else {
        match rpc.get_balance(&wallet.pubkey()).await {
            Ok(lamports) => {
                let bal = solana_sdk::native_token::lamports_to_sol(lamports);
                info!(
                    mint = %signal.mint,
                    balance_sol = format!("{:.4}", bal),
                    "💰 Wallet SOL balance after EXIT"
                );
                println!("  💰 Wallet balance: {:.4} SOL", bal);
                Some(bal)
            }
            Err(e) => {
                warn!("Failed to fetch SOL balance after exit: {}", e);
                None
            }
        }
    };

    // Use increment (new - prev) to avoid double-counting across partial exits.
    let daily_pnl_increment = total_pnl_sol - prev_pnl_sol;

    // Update in-memory trading state (instant — no network)
    trading_state.record_exit(&signal.mint, signal.sol_spent, daily_pnl_increment, is_full_exit).await;

    // Background: daily stats + latency log (don't block monitoring confirmation)
    let total_exit_ms = exit_start.elapsed().as_millis() as i64;
    let supabase_bg = supabase.clone();
    let mint_bg = signal.mint.clone();
    let tx_sig_str = tx_sig.to_string();
    let use_jito = cfg.env.use_jito;
    let use_helius_sender = cfg.env.use_helius_sender;
    let position_id_bg = signal.position_id;
    let exit_reason_bg = exit_reason_str.clone();
    let balance_bg = post_exit_balance;
    let pre_exit_bal_bg = pre_exit_balance;
    let entry_sol_bg = original_sol_spent;
    let exit_price_bg = signal.current_price;
    let exit_priority_fee_sol = solana_sdk::native_token::lamports_to_sol(final_priority_fee_lamports);
    let exit_tip_sol = 0.0; // No tip injection with Chainstack Warp
    let is_helius_bg = cfg.env.use_helius_sender;
    tokio::spawn(async move {
        update_daily_stats(&supabase_bg, daily_pnl_increment).await;

        // Log balance event to Supabase
        if let Some(bal) = balance_bg {
            log_exit_system_event(&supabase_bg, "balance_after_exit",
                &format!("Mint: {} | SOL balance: {:.4} | PnL: {}{:.4} SOL",
                    mint_bg, bal,
                    if daily_pnl_increment >= 0.0 { "+" } else { "" }, daily_pnl_increment)).await;
        }

        let latency_payload = serde_json::json!({
            "position_id": position_id_bg,
            "mint": mint_bg,
            "side": "sell",
            "total_ms": total_exit_ms,
            "used_jito": use_jito,
            "used_helius_sender": use_helius_sender,
            "tx_sig": tx_sig_str,
            "exit_reason": exit_reason_bg,
            "attempts": final_attempt,
            "slippage_bps": final_slippage_bps,
        });
        log_exit_latency(&supabase_bg, &latency_payload).await;

        // Log cost breakdown for real exit
        let network_fee = 0.000005_f64; // base tx fee
        let total_fees = network_fee + exit_priority_fee_sol + exit_tip_sol;
        let wallet_change = balance_bg.and_then(|after| pre_exit_bal_bg.map(|before| after - before));
        // Gross PnL = SOL received this exit - SOL fraction sold
        let gross_pnl = sol_received_this_exit - (entry_sol_bg * actual_sell_fraction);
        // Net PnL = gross - all exit fees (buy fees tracked separately)
        let net_pnl = gross_pnl - total_fees;
        let net_pnl_pct_val = if entry_sol_bg > 0.0 {
            net_pnl / (entry_sol_bg * actual_sell_fraction) * 100.0
        } else {
            0.0
        };
        let cost_payload = serde_json::json!({
            "position_id": position_id_bg,
            "mint": mint_bg,
            "side": "sell",
            "is_paper_trade": false,
            "sol_amount": sol_received_this_exit,
            "token_price_usd": exit_price_bg,
            "network_fee_sol": network_fee,
            "priority_fee_sol": exit_priority_fee_sol,
            "jito_tip_sol": if !is_helius_bg { exit_tip_sol } else { 0.0 },
            "helius_tip_sol": if is_helius_bg { exit_tip_sol } else { 0.0 },
            "total_fees_sol": total_fees,
            "slippage_bps": final_slippage_bps,
            "wallet_sol_before": pre_exit_bal_bg,
            "wallet_sol_after": balance_bg,
            "wallet_sol_change": wallet_change,
            "entry_sol_spent": entry_sol_bg,
            "exit_sol_received": sol_received_this_exit,
            "gross_pnl_sol": gross_pnl,
            "net_pnl_sol": net_pnl,
            "net_pnl_pct": net_pnl_pct_val,
            "exit_reason": exit_reason_bg,
            "tx_sig": tx_sig_str,
            "attempt_number": final_attempt,
            "execution_ms": total_exit_ms,
        });
        log_trade_cost(&supabase_bg, &cost_payload).await;
    });

    Ok(())
}

// ─── Sanitization helpers ─────────────────────────────────────

/// Replace NaN/Infinity with 0.0. Logs a warning if sanitization was needed.
fn sanitize_f64(v: f64, field_name: &str, mint: &str) -> f64 {
    if v.is_finite() {
        v
    } else {
        warn!(mint = %mint, field = field_name, value = ?v, "⚠️ Sanitized non-finite f64 to 0.0");
        0.0
    }
}

/// Strip URLs (which may contain API keys) from error strings before writing to DB.
fn sanitize_error_string(s: &str) -> String {
    let mut result = s.to_string();
    // Find and replace all https:// URLs (which contain API keys in query params)
    while let Some(start) = result.find("https://") {
        let end = result[start..]
            .find(|c: char| c.is_whitespace() || c == ')' || c == ']' || c == '"' || c == '\'')
            .map(|i| start + i)
            .unwrap_or(result.len());
        result.replace_range(start..end, "[REDACTED_URL]");
    }
    while let Some(start) = result.find("http://") {
        let end = result[start..]
            .find(|c: char| c.is_whitespace() || c == ')' || c == ']' || c == '"' || c == '\'')
            .map(|i| start + i)
            .unwrap_or(result.len());
        result.replace_range(start..end, "[REDACTED_URL]");
    }
    result
}

// ─── DB helper: fetch existing PnL for accumulation ──────────

/// Fetch the current `sol_received` and `pnl_sol` from the DB so partial exits
/// accumulate rather than overwrite.
/// Fetched position state needed by the exit engine.
struct PositionExitState {
    sol_received: f64,
    pnl_sol: f64,
    sol_spent: f64,
    status: String,
    /// ISO-8601 created_at from Supabase (used to compute hold_duration_secs).
    created_at: Option<String>,
}

async fn fetch_position_exit_state(supabase: &SupabaseClient, position_id: i64) -> PositionExitState {
    let url = format!(
        "{}/positions?id=eq.{}&select=sol_received,pnl_sol,sol_spent,status,created_at",
        supabase.base_url, position_id
    );
    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
            if let Some(row) = rows.first() {
                let sol_received = row
                    .get("sol_received")
                    .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64()))
                    .unwrap_or(0.0);
                let pnl_sol = row
                    .get("pnl_sol")
                    .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64()))
                    .unwrap_or(0.0);
                let sol_spent = row
                    .get("sol_spent")
                    .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64()))
                    .unwrap_or(0.0);
                let status = row
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let created_at = row
                    .get("created_at")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                PositionExitState { sol_received, pnl_sol, sol_spent, status, created_at }
            } else {
                PositionExitState { sol_received: 0.0, pnl_sol: 0.0, sol_spent: 0.0, status: "unknown".to_string(), created_at: None }
            }
        }
        _ => {
            warn!(position_id, "Failed to fetch position exit state — defaulting");
            PositionExitState { sol_received: 0.0, pnl_sol: 0.0, sol_spent: 0.0, status: "unknown".to_string(), created_at: None }
        }
    }
}

/// Compute hold_duration_secs from a Supabase created_at timestamp.
fn compute_hold_duration(created_at: &Option<String>) -> Option<i64> {
    created_at.as_ref().and_then(|ts| {
        chrono::DateTime::parse_from_rfc3339(ts)
            .ok()
            .map(|entry_time| (chrono::Utc::now() - entry_time.with_timezone(&chrono::Utc)).num_seconds())
    })
}

// ─── Slippage error detection ─────────────────────────────────

/// Check if an error string indicates a slippage/AMM error that is retryable
/// with a fresh quote.
fn is_slippage_error(err_str: &str) -> bool {
    err_str.contains("0x1788")               // Raydium CLMM / Orca: ExceededSlippage
        || err_str.contains("6024")          // Same in decimal
        || err_str.contains("0x1789")        // Raydium: InvalidTickArraySequence (price moved too far)
        || err_str.contains("6025")          // Same in decimal
        || err_str.contains("0x1786")        // Raydium: AmountTooSmall
        || err_str.contains("6001")          // AMM InsufficientInputAmount / pool not ready
        || err_str.contains("0x1771")        // Same in hex
        || err_str.contains("SlippageToleranceExceeded")
        || err_str.contains("ExceededSlippage")
}

// ─── Mark exit failed ─────────────────────────────────────────

async fn mark_exit_failed(supabase: &SupabaseClient, position_id: i64, reason: &str) {
    let url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
    let sanitized_reason = sanitize_error_string(reason);
    let payload = serde_json::json!({
        "status": "exit_failed",
        "exit_reason": format!("exit_failed: {}", sanitized_reason),
        "exit_time": chrono::Utc::now().to_rfc3339(),
    });
    match supabase.client.patch(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            warn!(position_id, "Position marked as exit_failed");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to mark exit_failed: {}", body);
        }
        Err(e) => {
            warn!("Failed to mark exit_failed: {}", e);
        }
    }
}

// ─── Jito / RPC submission ────────────────────────────────────

async fn submit_exit_via_jito(
    cfg: &AppConfig,
    wallet: &BotWallet,
    signed_swap_tx: solana_sdk::transaction::VersionedTransaction,
    mint: &str,
    supabase: &SupabaseClient,
    shared_jito: Option<&JitoClient>,
) -> Result<solana_sdk::signature::Signature> {
    use solana_sdk::transaction::Transaction;

    info!(mint = %mint, "🚀 Submitting exit via Jito bundle");

    // Use shared client if available, otherwise create a new one
    let owned_jito;
    let jito = match shared_jito {
        Some(j) => j,
        None => {
            owned_jito = JitoClient::new(
                cfg.env.jito_block_engine_url.clone(),
                cfg.strategy.jito.tip_multiplier,
                cfg.env.jito_max_tip_sol,
            );
            &owned_jito
        }
    };

    let tip_lamports = jito.calculate_tip().await;
    let tip_sol = tip_lamports as f64 / 1_000_000_000.0;
    info!(mint = %mint, tip_sol = format!("{:.6}", tip_sol), "💰 Jito exit tip");

    let tip_instruction =
        JitoClient::create_tip_instruction(&wallet.pubkey(), tip_lamports)?;

    let recent_blockhash = signed_swap_tx.message.recent_blockhash();
    let tip_tx = Transaction::new_signed_with_payer(
        &[tip_instruction],
        Some(&wallet.pubkey()),
        &[wallet.keypair()],
        *recent_blockhash,
    );

    let tip_versioned_tx = solana_sdk::transaction::VersionedTransaction::from(tip_tx);
    let bundle = vec![signed_swap_tx.clone(), tip_versioned_tx];

    match jito
        .send_bundle_and_wait(bundle, cfg.strategy.execution.tx_confirm_timeout_secs)
        .await
    {
        Ok(bundle_id) => {
            info!(mint = %mint, bundle_id = %bundle_id, "✅ Jito exit bundle confirmed");
            Ok(signed_swap_tx.signatures[0])
        }
        Err(e) => {
            error!(mint = %mint, "❌ Jito exit bundle failed: {}", e);
            log_exit_system_event(
                supabase,
                "jito_exit_bundle_failed",
                &format!("Mint: {} — Error: {}", mint, e),
            )
            .await;
            Err(e)
        }
    }
}

async fn submit_exit_via_rpc(
    rpc: &RpcClient,
    signed_tx: &solana_sdk::transaction::VersionedTransaction,
    mint: &str,
    skip_preflight: bool,
) -> Result<solana_sdk::signature::Signature> {
    use solana_client::rpc_config::RpcSendTransactionConfig;
    use solana_sdk::commitment_config::CommitmentLevel;

    info!(mint = %mint, skip_preflight, "📤 Submitting exit via RPC");
    let config = RpcSendTransactionConfig {
        skip_preflight,
        preflight_commitment: Some(CommitmentLevel::Confirmed),
        ..Default::default()
    };
    match rpc.send_transaction_with_config(signed_tx, config).await {
        Ok(sig) => {
            info!(mint = %mint, sig = %sig, "📤 Exit tx submitted");
            Ok(sig)
        }
        Err(e) => {
            error!(mint = %mint, "❌ Exit tx submission failed: {}", e);
            Err(anyhow::anyhow!("Exit transaction submission failed: {}", e))
        }
    }
}

async fn log_exit_system_event(supabase: &SupabaseClient, event_type: &str, message: &str) {
    let payload = serde_json::json!({
        "event_type": event_type,
        "message": message,
    });
    let url = format!("{}/system_events", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to log exit system event: {}", body);
        }
        Err(e) => {
            warn!("Failed to log exit system event: {}", e);
        }
    }
}

// ─── On-chain token balance helper (for exit engine) ─────────

/// Fetch the actual SPL token balance (raw smallest units) for a wallet+mint from chain.
/// Used as a fallback when signal.token_amount is 0.
async fn fetch_exit_token_balance(
    rpc: &RpcClient,
    wallet: &Pubkey,
    mint_str: &str,
) -> Option<f64> {
    let mint = Pubkey::from_str(mint_str).ok()?;

    let accounts = rpc
        .get_token_accounts_by_owner(
            wallet,
            solana_client::rpc_request::TokenAccountsFilter::Mint(mint),
        )
        .await
        .ok()?;

    let balance: f64 = accounts
        .iter()
        .filter_map(|account| {
            let data: serde_json::Value =
                serde_json::to_value(&account.account.data).ok()?;
            data.get("parsed")
                .and_then(|p| p.get("info"))
                .and_then(|i| i.get("tokenAmount"))
                .and_then(|t| t.get("amount"))
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse::<f64>().ok())
        })
        .sum();

    if balance > 0.0 { Some(balance) } else { None }
}

async fn fetch_confirmed_exit_outcome(
    rpc: &RpcClient,
    tx_sig: &solana_sdk::signature::Signature,
    wallet: &Pubkey,
    mint_str: &str,
    same_tx_tip_sol: f64,
) -> Option<ConfirmedExitOutcome> {
    let tx_config = solana_client::rpc_config::RpcTransactionConfig {
        encoding: Some(solana_transaction_status::UiTransactionEncoding::JsonParsed),
        commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    let mut tx_response = None;
    for attempt in 0..5 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        }

        match rpc.get_transaction_with_config(tx_sig, tx_config.clone()).await {
            Ok(resp) => {
                tx_response = Some(resp);
                break;
            }
            Err(e) => {
                debug!(
                    sig = %tx_sig,
                    attempt = attempt + 1,
                    error = %e,
                    "getTransaction retry after exit confirmation"
                );
            }
        }
    }

    let tx_response = tx_response?;
    let tx_value = serde_json::to_value(&tx_response.transaction).ok()?;
    let wallet_str = wallet.to_string();

    let account_keys: Vec<String> = tx_value
        .get("transaction")
        .and_then(|tx| tx.get("message"))
        .and_then(|msg| msg.get("accountKeys"))
        .and_then(|keys| keys.as_array())
        .map(|keys| {
            keys.iter()
                .filter_map(|key| {
                    key.as_str()
                        .map(|s| s.to_string())
                        .or_else(|| {
                            key.get("pubkey")
                                .and_then(|p| p.as_str())
                                .map(|s| s.to_string())
                        })
                })
                .collect()
        })
        .unwrap_or_default();

    let meta = tx_value.get("meta")?;
    let wallet_index = account_keys.iter().position(|key| key == &wallet_str);

    let wallet_pre_sol = wallet_index
        .and_then(|idx| meta.get("preBalances").and_then(|v| v.get(idx)))
        .and_then(|v| v.as_u64())
        .map(solana_sdk::native_token::lamports_to_sol);
    let wallet_post_sol = wallet_index
        .and_then(|idx| meta.get("postBalances").and_then(|v| v.get(idx)))
        .and_then(|v| v.as_u64())
        .map(solana_sdk::native_token::lamports_to_sol);

    let sum_token_balances = |field: &str| -> (f64, bool) {
        let mut total = 0.0;
        let mut found = false;

        if let Some(entries) = meta.get(field).and_then(|v| v.as_array()) {
            for entry in entries {
                let owner = entry.get("owner").and_then(|v| v.as_str());
                let mint = entry.get("mint").and_then(|v| v.as_str());
                if owner == Some(wallet_str.as_str()) && mint == Some(mint_str) {
                    if let Some(amount) = entry
                        .get("uiTokenAmount")
                        .and_then(|v| v.get("amount"))
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        total += amount;
                        found = true;
                    }
                }
            }
        }

        (total, found)
    };

    let (pre_token_raw, saw_pre_token) = sum_token_balances("preTokenBalances");
    let (post_token_raw, saw_post_token) = sum_token_balances("postTokenBalances");
    let wallet_pre_tokens = if saw_pre_token {
        Some(pre_token_raw)
    } else {
        None
    };
    let wallet_post_tokens = if saw_post_token {
        Some(post_token_raw)
    } else if saw_pre_token {
        Some(0.0)
    } else {
        None
    };

    let jupiter_return_sol = meta
        .get("logMessages")
        .and_then(|v| v.as_array())
        .and_then(|logs| {
            logs.iter().rev().filter_map(|log| log.as_str()).find_map(|log| {
                log.strip_prefix(
                    "Program return: JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 ",
                )
                .and_then(|encoded| {
                    base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        encoded,
                    )
                    .ok()
                })
                .and_then(|raw| {
                    let bytes: [u8; 8] = raw.get(..8)?.try_into().ok()?;
                    Some(solana_sdk::native_token::lamports_to_sol(
                        u64::from_le_bytes(bytes),
                    ))
                })
            })
        });

    let fee_sol = meta
        .get("fee")
        .and_then(|v| v.as_u64())
        .map(solana_sdk::native_token::lamports_to_sol);
    let sol_received = if let Some(sol) = jupiter_return_sol {
        Some(sol)
    } else {
        match (wallet_pre_sol, wallet_post_sol, fee_sol) {
            (Some(pre), Some(post), Some(fee)) => Some((post - pre) + fee + same_tx_tip_sol),
            _ => None,
        }
    };

    Some(ConfirmedExitOutcome {
        wallet_pre_tokens,
        wallet_post_tokens,
        sol_received,
        wallet_pre_sol,
        wallet_post_sol,
    })
}

// ─── Daily stats ─────────────────────────────────────────────

async fn update_daily_stats(supabase: &SupabaseClient, pnl_sol: f64) {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let fetch_url = format!("{}/daily_stats?date=eq.{}", supabase.base_url, today);
    let existing: Vec<serde_json::Value> = match supabase.client.get(&fetch_url).send().await {
        Ok(resp) if resp.status().is_success() => resp.json().await.unwrap_or_default(),
        _ => vec![],
    };

    let (new_total, new_won, new_lost, new_pnl) = if let Some(row) = existing.first() {
        let prev_total = row.get("trades_total").and_then(|v| v.as_i64()).unwrap_or(0);
        let prev_won   = row.get("trades_won").and_then(|v| v.as_i64()).unwrap_or(0);
        let prev_lost  = row.get("trades_lost").and_then(|v| v.as_i64()).unwrap_or(0);
        let prev_pnl   = row.get("pnl_sol").and_then(|v| v.as_f64()).unwrap_or(0.0);
        (
            prev_total + 1,
            prev_won + if pnl_sol > 0.0 { 1 } else { 0 },
            prev_lost + if pnl_sol <= 0.0 { 1 } else { 0 },
            prev_pnl + pnl_sol,
        )
    } else {
        (
            1,
            if pnl_sol > 0.0 { 1 } else { 0 },
            if pnl_sol <= 0.0 { 1 } else { 0 },
            pnl_sol,
        )
    };

    let payload = serde_json::json!({
        "date": today,
        "trades_total": new_total,
        "trades_won": new_won,
        "trades_lost": new_lost,
        "pnl_sol": new_pnl,
    });

    let result = if existing.is_empty() {
        // INSERT new row
        let insert_url = format!("{}/daily_stats", supabase.base_url);
        supabase.client.post(&insert_url).json(&payload).send().await
    } else {
        // PATCH existing row
        let patch_url = format!("{}/daily_stats?date=eq.{}", supabase.base_url, today);
        supabase.client.patch(&patch_url).json(&payload).send().await
    };
    match result {
        Ok(resp) if resp.status().is_success() => {
            debug!(today = %today, pnl = new_pnl, "Daily stats updated");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to update daily stats: {}", body);
        }
        Err(e) => {
            warn!("Failed to update daily stats: {}", e);
        }
    }
}

/// Log exit latency breakdown to Supabase `trade_latency` table.
async fn log_exit_latency(supabase: &SupabaseClient, payload: &serde_json::Value) {
    let url = format!("{}/trade_latency", supabase.base_url);
    match supabase.client.post(&url).json(payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            debug!("Exit latency logged to Supabase");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to log exit latency: {}", body);
        }
        Err(e) => {
            warn!("Failed to log exit latency: {}", e);
        }
    }
}
