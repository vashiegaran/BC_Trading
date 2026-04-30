pub mod helius_sender;
pub mod jito_client;
pub mod jupiter;
pub mod state;
pub mod types;
pub mod wallet;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use chrono::Timelike;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::native_token::lamports_to_sol;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::filters::types::FilteredToken;
use crate::filters::post_buy::{self, PostBuyAlert};
use crate::logger::SupabaseClient;
use crate::sniper::log_pipeline_latency;

use jupiter::{JupiterClient, SOL_MINT};
use jito_client::JitoClient;
use helius_sender::HeliusSenderClient;
use state::TradingState;
use types::PositionOpened;
use wallet::BotWallet;

/// Channel capacity for execution → monitoring pipeline.
const CHANNEL_CAPACITY: usize = 50;

/// Minimum liquidity floor for dynamic sizing scale (SOL).
const DYNAMIC_SIZE_LIQ_FLOOR: f64 = 30.0;
/// Liquidity at which position size reaches max (SOL).
const DYNAMIC_SIZE_LIQ_CAP: f64 = 80.0;

/// Compute dynamic position size based on pool liquidity.
/// Scales linearly from min_buy_sol (at FLOOR) to max_buy_sol (at CAP+).
fn compute_dynamic_buy_amount(cfg: &AppConfig, initial_liquidity_sol: f64) -> f64 {
    let exec = &cfg.strategy.execution;
    let min_buy = match exec.min_buy_sol {
        Some(v) if v > 0.0 => v,
        _ => return exec.buy_amount_sol, // dynamic sizing disabled
    };
    let max_buy = exec.max_buy_sol.unwrap_or(exec.buy_amount_sol);

    if initial_liquidity_sol <= 0.0 {
        return max_buy; // unknown liquidity — use default
    }

    let ratio = ((initial_liquidity_sol - DYNAMIC_SIZE_LIQ_FLOOR) / (DYNAMIC_SIZE_LIQ_CAP - DYNAMIC_SIZE_LIQ_FLOOR)).clamp(0.0, 1.0);
    min_buy + ratio * (max_buy - min_buy)
}

/// Start the execution engine.
///
/// Consumes `FilteredToken`s from the filter channel and produces
/// `PositionOpened` events for the monitoring engine.
///
/// Returns `(position_rx, alert_rx)` where `alert_rx` carries
/// post-buy verification alerts for emergency exits.
pub fn start(
    cfg: Arc<AppConfig>,
    mut filter_rx: mpsc::Receiver<FilteredToken>,
    supabase: Arc<SupabaseClient>,
    wallet: Arc<BotWallet>,
    trading_state: Arc<TradingState>,
) -> (mpsc::Receiver<PositionOpened>, mpsc::Receiver<PostBuyAlert>) {
    let (tx, rx) = mpsc::channel::<PositionOpened>(CHANNEL_CAPACITY);
    let (alert_tx, alert_rx) = mpsc::channel::<PostBuyAlert>(CHANNEL_CAPACITY);

    tokio::spawn(async move {
        info!("Execution engine started — waiting for filtered tokens");

        let jupiter = JupiterClient::new(
            cfg.strategy.execution.api_request_timeout_secs,
            cfg.strategy.execution.max_retries,
        );
        let rpc = RpcClient::new(cfg.env.solana_rpc_url.clone());
        let backup_rpc = RpcClient::new(cfg.env.solana_rpc_backup_url.clone());

        // Pre-warm Jupiter HTTP connection (pays TLS handshake now, not on first trade)
        if let Err(e) = jupiter.get_price(SOL_MINT).await {
            debug!("Jupiter pre-warm failed (non-fatal): {}", e);
        } else {
            info!("⚡ Jupiter HTTP connection pre-warmed");
        }

        // Create JitoClient once — reuse for all trades (saves TLS handshake per trade)
        let jito_client = if cfg.env.use_jito {
            Some(Arc::new(JitoClient::new(
                cfg.env.jito_block_engine_url.clone(),
                cfg.strategy.jito.tip_multiplier,
                cfg.env.jito_max_tip_sol,
            )))
        } else {
            None
        };

        // Pre-warm Jito HTTP connection
        if let Some(ref jito) = jito_client {
            let _ = jito.get_recommended_tip().await;
            info!("⚡ Jito HTTP connection pre-warmed");
        }

        // Create HeliusSenderClient once — reuse for all trades
        let helius_sender = if cfg.env.use_helius_sender {
            let hs = Arc::new(HeliusSenderClient::new(cfg.env.helius_sender_url.clone()));
            hs.warm_connection().await;
            Some(hs)
        } else {
            None
        };

        while let Some(token) = filter_rx.recv().await {
            let mint_str = token.event.mint.to_string();
            let detected_at_ms = token.event.detected_at;
            let pipeline_start = Instant::now();
            let mut timing = token.pipeline_timing.clone();
            info!(mint = %mint_str, "📦 Execution engine received filtered token");

            // ── Pre-execution safety checks (in-memory — no Supabase reads) ─────────
            let pre_check_start = Instant::now();
            match pre_execution_checks_cached(&cfg, &trading_state, &rpc, &backup_rpc, &wallet).await {
                Ok(()) => {
                    let precheck_ms = pre_check_start.elapsed().as_millis() as u64;
                    timing.precheck_total_ms = Some(precheck_ms);
                    info!(
                        mint = %mint_str,
                        elapsed_ms = precheck_ms,
                        "⏱️ Pre-execution checks passed (cached)"
                    );
                }
                Err(reason) => {
                    let precheck_ms = pre_check_start.elapsed().as_millis() as u64;
                    timing.precheck_total_ms = Some(precheck_ms);
                    timing.outcome = Some("rejected_precheck".to_string());
                    timing.rejection_stage = Some("precheck".to_string());
                    timing.rejection_reason = Some(reason.clone());
                    let timing_payload = timing.to_json(&mint_str);
                    warn!(mint = %mint_str, reason = %reason, "⏭️ Skipping token — pre-execution check failed");
                    let supabase_bg = supabase.clone();
                    let reason_bg = reason.clone();
                    let mint_bg = mint_str.clone();
                    tokio::spawn(async move {
                        log_system_event(&supabase_bg, "pre_execution_check_failed",
                            &format!("Mint: {} — Reason: {}", mint_bg, reason_bg)).await;
                        log_pipeline_latency(&supabase_bg, &timing_payload).await;
                    });
                    continue;
                }
            }
        
            // ── Dedup: skip if we already have a position in this mint (in-memory) ──
            // ATOMIC reserve-or-skip — protects against the TOCTOU race where two
            // near-simultaneous events (e.g. pump.fun migrate+complete double-emit,
            // or rapid duplicate signals) both passed a non-atomic read before
            // either had called record_buy. The reservation is consumed by
            // record_buy on success, or released on every failure path below.
            let dedup_start = Instant::now();
            if !trading_state.try_reserve_for_mint(&mint_str).await {
                warn!(mint = %mint_str, "⏭️ Skipping — mint already open or reserved (dedup)");
                timing.outcome = Some("rejected_precheck".to_string());
                timing.rejection_stage = Some("precheck".to_string());
                timing.rejection_reason = Some("duplicate_mint".to_string());
                let timing_payload = timing.to_json(&mint_str);
                let supabase_bg = supabase.clone();
                tokio::spawn(async move {
                    log_pipeline_latency(&supabase_bg, &timing_payload).await;
                });
                continue;
            }
            info!(
                mint = %mint_str,
                elapsed_ms = dedup_start.elapsed().as_millis() as u64,
                "⏱️ Dedup check (reserved)"
            );

            // ── Dev wallet blacklist check ────────────────────────────────────────────
            let dev_wallet_str = token.event.creator_wallet.to_string();
            if trading_state.is_dev_blacklisted(&dev_wallet_str).await {
                // Release the reservation we just took — this mint is rejected.
                trading_state.release_reservation(&mint_str).await;
                warn!(
                    mint = %mint_str,
                    dev_wallet = %dev_wallet_str,
                    "🚫 Skipping — dev wallet is blacklisted (known dumper)"
                );
                timing.outcome = Some("rejected_precheck".to_string());
                timing.rejection_stage = Some("precheck".to_string());
                timing.rejection_reason = Some(format!("dev_blacklisted:{}", dev_wallet_str));
                let timing_payload = timing.to_json(&mint_str);
                let supabase_bg = supabase.clone();
                let mint_bg = mint_str.clone();
                let dev_bg = dev_wallet_str.clone();
                tokio::spawn(async move {
                    log_system_event(&supabase_bg, "dev_blacklist_skip",
                        &format!("Mint: {} — Dev {} is blacklisted", mint_bg, dev_bg)).await;
                    log_pipeline_latency(&supabase_bg, &timing_payload).await;
                });
                continue;
            }

            // Route pre-check REMOVED — the quote inside execute_real_trade()
            // will retry on NO_ROUTES_FOUND directly, saving 0-30s of latency.

            // Anti-chase check moved into execute_paper_trade / execute_real_trade
            // where the buy quote already provides current pricing — no extra API call needed.

            info!(
                mint = %mint_str,
                pipeline_elapsed_ms = pipeline_start.elapsed().as_millis() as u64,
                "⏱️ Pre-trade pipeline complete — entering trade execution"
            );

            // ── Execute trade ─────────────────────────────────────────────────────────
            let trade_exec_start = Instant::now();
            let buy_amount_sol = cfg.strategy.execution.buy_amount_sol;
            if cfg.env.paper_trade {
                match execute_paper_trade(&cfg, &jupiter, &supabase, &token, &tx, &trading_state).await {
                    Ok(Some(result)) => {
                        let exec_ms = trade_exec_start.elapsed().as_millis() as u64;
                        let pipeline_ms = {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            (now_ms - detected_at_ms) as u64
                        };
                        timing.execution_total_ms = Some(exec_ms);
                        timing.pipeline_total_ms = Some(pipeline_ms);
                        timing.outcome = Some("bought".to_string());
                        timing.position_id = Some(result.position_id);
                        let timing_payload = timing.to_json(&mint_str);

                        info!(mint = %mint_str, exec_ms, pipeline_ms, "📝 Paper trade recorded");

                        // Write pipeline_latency (background)
                        let supabase_bg = Arc::clone(&supabase);
                        tokio::spawn(async move {
                            log_pipeline_latency(&supabase_bg, &timing_payload).await;
                        });

                        // ── Spawn post-buy verification (background) ──
                        let cfg_bg = Arc::clone(&cfg);
                        let supabase_bg = Arc::clone(&supabase);
                        let alert_tx_bg = alert_tx.clone();
                        let mint_bg = mint_str.clone();
                        tokio::spawn(async move {
                            post_buy::verify(
                                cfg_bg, supabase_bg, alert_tx_bg,
                                mint_bg, result.position_id, result.entry_price_usd, buy_amount_sol,
                                result.token_amount, true,
                            ).await;
                        });
                    }
                    Ok(None) => {
                        // Trade did not open a position — free the reservation.
                        trading_state.release_reservation(&mint_str).await;
                        timing.outcome = Some("execution_failed".to_string());
                        timing.rejection_stage = Some("execution".to_string());
                        timing.rejection_reason = Some("no_position_produced".to_string());
                        timing.execution_total_ms = Some(trade_exec_start.elapsed().as_millis() as u64);
                        let timing_payload = timing.to_json(&mint_str);
                        let supabase_bg = Arc::clone(&supabase);
                        tokio::spawn(async move {
                            log_pipeline_latency(&supabase_bg, &timing_payload).await;
                        });
                        debug!(mint = %mint_str, "Paper trade did not produce a position");
                    }
                    Err(e) => {
                        // Trade errored — free the reservation.
                        trading_state.release_reservation(&mint_str).await;
                        timing.outcome = Some("execution_failed".to_string());
                        timing.rejection_stage = Some("execution".to_string());
                        timing.rejection_reason = Some(e.to_string());
                        timing.execution_total_ms = Some(trade_exec_start.elapsed().as_millis() as u64);
                        let timing_payload = timing.to_json(&mint_str);
                        error!(mint = %mint_str, "Paper trade failed: {}", e);
                        let supabase_bg = Arc::clone(&supabase);
                        let mint_bg = mint_str.clone();
                        let err_msg = e.to_string();
                        tokio::spawn(async move {
                            log_system_event(
                                &supabase_bg,
                                "paper_trade_failed",
                                &format!("Mint: {} — Error: {}", mint_bg, err_msg),
                            ).await;
                            log_pipeline_latency(&supabase_bg, &timing_payload).await;
                        });
                    }
                }
            } else {
                match execute_real_trade(&cfg, &jupiter, &supabase, &rpc, &backup_rpc, &wallet, &token, &tx, &trading_state, jito_client.as_ref().map(|j| j.as_ref()), helius_sender.as_ref().map(|h| h.as_ref()))
                    .await
                {
                    Ok(Some(result)) => {
                        let exec_ms = trade_exec_start.elapsed().as_millis() as u64;
                        let pipeline_ms = {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            (now_ms - detected_at_ms) as u64
                        };
                        timing.execution_total_ms = Some(exec_ms);
                        timing.pipeline_total_ms = Some(pipeline_ms);
                        timing.outcome = Some("bought".to_string());
                        timing.position_id = Some(result.position_id);
                        let timing_payload = timing.to_json(&mint_str);

                        debug!(mint = %mint_str, exec_ms, pipeline_ms, "Real trade flow finished");

                        let supabase_bg2 = Arc::clone(&supabase);
                        tokio::spawn(async move {
                            log_pipeline_latency(&supabase_bg2, &timing_payload).await;
                        });

                        // ── Spawn post-buy verification (background) ──
                        let cfg_bg = Arc::clone(&cfg);
                        let supabase_bg = Arc::clone(&supabase);
                        let alert_tx_bg = alert_tx.clone();
                        let mint_bg = mint_str.clone();
                        tokio::spawn(async move {
                            post_buy::verify(
                                cfg_bg, supabase_bg, alert_tx_bg,
                                mint_bg, result.position_id, result.entry_price_usd, buy_amount_sol,
                                result.token_amount, false,
                            ).await;
                        });
                    }
                    Ok(None) => {
                        // Trade did not open a position — free the reservation.
                        trading_state.release_reservation(&mint_str).await;
                        timing.outcome = Some("execution_failed".to_string());
                        timing.rejection_stage = Some("execution".to_string());
                        timing.rejection_reason = Some("no_position_produced".to_string());
                        timing.execution_total_ms = Some(trade_exec_start.elapsed().as_millis() as u64);
                        let timing_payload = timing.to_json(&mint_str);
                        let supabase_bg = Arc::clone(&supabase);
                        tokio::spawn(async move {
                            log_pipeline_latency(&supabase_bg, &timing_payload).await;
                        });
                        debug!(mint = %mint_str, "Real trade did not produce a position");
                    }
                    Err(e) => {
                        // Trade errored — free the reservation.
                        trading_state.release_reservation(&mint_str).await;
                        timing.outcome = Some("execution_failed".to_string());
                        timing.rejection_stage = Some("execution".to_string());
                        timing.rejection_reason = Some(e.to_string());
                        timing.execution_total_ms = Some(trade_exec_start.elapsed().as_millis() as u64);
                        let timing_payload = timing.to_json(&mint_str);
                        error!(mint = %mint_str, "Real trade failed: {}", e);
                        let supabase_bg = Arc::clone(&supabase);
                        let mint_bg = mint_str.clone();
                        let err_msg = e.to_string();
                        tokio::spawn(async move {
                            log_system_event(
                                &supabase_bg,
                                "real_trade_failed",
                                &format!("Mint: {} — Error: {}", mint_bg, err_msg),
                            ).await;
                            log_pipeline_latency(&supabase_bg, &timing_payload).await;
                        });
                    }
                }
            }
        }

        info!("Execution engine shutting down (filter channel closed)");
    });

    (rx, alert_rx)
}

// ─── Pre-execution safety checks (in-memory cached) ─────────

async fn pre_execution_checks_cached(
    cfg: &AppConfig,
    state: &TradingState,
    rpc: &RpcClient,
    backup_rpc: &RpcClient,
    wallet: &BotWallet,
) -> Result<(), String> {
    let check_start = Instant::now();

    // Read safety values from in-memory cache (< 1ms)
    let open_count = state.open_position_count().await;
    let current_exposure = state.total_exposure().await;
    let today_pnl = state.today_pnl().await;

    // SAFETY Rule 6: Check open position count
    if open_count >= cfg.strategy.execution.max_open_positions as i64 {
        return Err(format!(
            "Max open positions reached ({}/{})",
            open_count, cfg.strategy.execution.max_open_positions
        ));
    }

    // SAFETY Rule 6: Check portfolio exposure
    let would_be_exposure = current_exposure + cfg.strategy.execution.buy_amount_sol;
    if would_be_exposure > cfg.strategy.risk.max_portfolio_exposure_sol {
        return Err(format!(
            "Portfolio exposure cap would be exceeded ({:.4} + {:.4} > {:.4})",
            current_exposure,
            cfg.strategy.execution.buy_amount_sol,
            cfg.strategy.risk.max_portfolio_exposure_sol
        ));
    }

    // SAFETY Rule 2: Check daily PnL loss limit
    if today_pnl <= -cfg.strategy.risk.daily_loss_limit_sol {
        return Err(format!("Daily loss limit hit (PnL: {:.4} SOL)", today_pnl));
    }

    // SAFETY Rule 5: Check wallet SOL balance (only RPC call remaining)
    if !cfg.env.paper_trade {
        let balance_result = match rpc.get_balance(&wallet.pubkey()).await {
            Ok(lamports) => Ok(lamports),
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("429") || err_msg.contains("Too Many Requests") {
                    warn!("Primary RPC 429 on balance check — trying backup RPC");
                    backup_rpc.get_balance(&wallet.pubkey()).await
                } else {
                    Err(e)
                }
            }
        };

        match balance_result {
            Ok(lamports) => {
                let balance_sol = lamports_to_sol(lamports);
                if balance_sol < cfg.strategy.risk.min_sol_balance {
                    return Err(format!(
                        "Insufficient SOL balance ({:.4} < {:.4})",
                        balance_sol, cfg.strategy.risk.min_sol_balance
                    ));
                }
            }
            Err(e) => {
                warn!("Failed to check SOL balance: {}", e);
                return Err("Could not verify SOL balance".to_string());
            }
        }
    }

    info!(
        elapsed_ms = check_start.elapsed().as_millis() as u64,
        "⏱️ Pre-execution checks completed (cached + 1 RPC)"
    );

    Ok(())
}

// ─── Paper trade execution ───────────────────────────────────

async fn execute_paper_trade(
    cfg: &AppConfig,
    jupiter: &JupiterClient,
    supabase: &SupabaseClient,
    token: &FilteredToken,
    tx: &mpsc::Sender<PositionOpened>,
    trading_state: &TradingState,
) -> Result<Option<TradeResult>> {
    let trade_start = Instant::now();
    let mint_str = token.event.mint.to_string();
    let buy_amount_sol = compute_dynamic_buy_amount(cfg, token.event.initial_liquidity_sol);

    info!(
        mint = %mint_str,
        initial_liquidity_sol = token.event.initial_liquidity_sol,
        buy_amount_sol = format!("{:.4}", buy_amount_sol),
        "📐 Dynamic position size computed"
    );

    // ── Realistic-fill guard: reject buys that would consume too much of the
    // pool. Without this, paper trades into <$1 liquidity pools "fill" 0.05 SOL
    // and produce fictional 50x outcomes that no real tx could ever achieve.
    let realistic = cfg.strategy.execution.paper_realistic_fills;
    if realistic && token.event.initial_liquidity_sol > 0.0 {
        let max_fill = token.event.initial_liquidity_sol * (cfg.strategy.execution.paper_max_pool_fill_pct / 100.0);
        if buy_amount_sol > max_fill {
            warn!(
                mint = %mint_str,
                buy_amount_sol = format!("{:.4}", buy_amount_sol),
                pool_sol = format!("{:.4}", token.event.initial_liquidity_sol),
                max_fill_sol = format!("{:.4}", max_fill),
                cap_pct = cfg.strategy.execution.paper_max_pool_fill_pct,
                "⏭️ Paper buy skipped — size exceeds pool fill cap (would move price too much)"
            );
            log_system_event(
                supabase,
                "paper_fill_rejected",
                &format!(
                    "Mint: {} — buy {:.4} SOL > {:.1}% of pool {:.4} SOL",
                    mint_str,
                    buy_amount_sol,
                    cfg.strategy.execution.paper_max_pool_fill_pct,
                    token.event.initial_liquidity_sol
                ),
            )
            .await;
            return Ok(None);
        }
    }

    // ── Entry price + token amount ─────────────────────────────────────
    // Realistic mode: use a real Jupiter quote for the buy (mirrors the
    // exit path), so price impact and route availability gate the entry the
    // same way they gate a real tx.
    // Legacy mode: DexScreener mid-price + flat configured slippage. Cheap
    // but cheats on thin pools — kept only for old-data parity.
    let price_start = Instant::now();
    let sol_usd = jupiter.get_price(SOL_MINT).await.unwrap_or(150.0);
    let slippage_bps = cfg.strategy.execution.paper_slippage_bps;
    let mut entry_price_usd: f64;
    let mut token_amount: f64;
    let mut used_jupiter_buy_quote = false;
    let mut buy_quote_impact_bps: f64 = 0.0;

    if realistic {
        // Quote SOL -> mint for the dynamic buy size
        let lamports = (buy_amount_sol * 1_000_000_000.0) as u64;
        // Use the configured slippage as the quote tolerance
        let quote_slippage = if slippage_bps > 0 { slippage_bps } else { 500 };
        match jupiter.get_quote(SOL_MINT, &mint_str, lamports, quote_slippage).await {
            Ok(q) => {
                let out_raw: f64 = q.out_amount.parse().unwrap_or(0.0);
                let impact: f64 = q.price_impact_pct.parse().unwrap_or(0.0);
                buy_quote_impact_bps = (impact * 100.0).abs();
                if out_raw <= 0.0 {
                    warn!(mint = %mint_str, "Skipping paper trade — Jupiter buy quote returned zero out_amount");
                    return Ok(None);
                }
                token_amount = out_raw; // already in raw token units
                let tokens_ui = out_raw / 1_000_000.0; // pump.fun = 6 decimals
                let value_usd = buy_amount_sol * sol_usd;
                entry_price_usd = if tokens_ui > 0.0 { value_usd / tokens_ui } else { 0.0 };
                used_jupiter_buy_quote = true;
                info!(
                    mint = %mint_str,
                    buy_amount_sol = format!("{:.4}", buy_amount_sol),
                    tokens_received = format!("{:.0}", out_raw),
                    entry_price_usd = format!("{:.10}", entry_price_usd),
                    impact_bps = format!("{:.1}", buy_quote_impact_bps),
                    "📊 Paper buy: Jupiter quote used (realistic fill)"
                );
            }
            Err(e) => {
                warn!(
                    mint = %mint_str,
                    error = %e,
                    "Paper buy: Jupiter quote failed — falling back to DexScreener mid + flat slippage"
                );
                let px = match jupiter.get_price(&mint_str).await { Ok(p) if p > 0.0 => p, _ => 0.0 };
                entry_price_usd = if slippage_bps > 0 && px > 0.0 {
                    px * (1.0 + slippage_bps as f64 / 10_000.0)
                } else { px };
                token_amount = if entry_price_usd > 0.0 {
                    (buy_amount_sol * sol_usd / entry_price_usd) * 1_000_000.0
                } else { 0.0 };
            }
        }
    } else {
        // Legacy DexScreener path
        let px = match jupiter.get_price(&mint_str).await { Ok(p) if p > 0.0 => p, _ => 0.0 };
        let pre_slippage_price = px;
        entry_price_usd = if slippage_bps > 0 && px > 0.0 {
            px * (1.0 + slippage_bps as f64 / 10_000.0)
        } else { px };
        token_amount = if entry_price_usd > 0.0 {
            (buy_amount_sol * sol_usd / entry_price_usd) * 1_000_000.0
        } else { 0.0 };
        if slippage_bps > 0 && pre_slippage_price > 0.0 {
            info!(
                mint = %mint_str,
                original_price = pre_slippage_price,
                slipped_price = entry_price_usd,
                slippage_bps,
                "📊 Paper buy slippage applied (legacy mode)"
            );
        }
    }
    let price_ms = price_start.elapsed().as_millis() as i64;

    // ── Anti-chase: check if price moved too much since filter time ──
    let max_move_pct = cfg.strategy.execution.max_entry_price_move_pct;
    if max_move_pct > 0.0 && entry_price_usd > 0.0 {
        if let Some(filter_price) = token.filter_price_usd {
            if filter_price > 0.0 {
                let move_pct = ((entry_price_usd - filter_price) / filter_price) * 100.0;
                debug!(
                    mint = %mint_str,
                    entry_price_usd,
                    filter_price,
                    move_pct = format!("{:.1}%", move_pct),
                    "Anti-chase: price move since filter (paper)"
                );
                if move_pct > max_move_pct || move_pct < -max_move_pct {
                    warn!(
                        mint = %mint_str,
                        move_pct = format!("{:.1}%", move_pct),
                        max_allowed = format!("{:.1}%", max_move_pct),
                        "⏭️ Anti-chase: price moved {:.1}% since filter — skipping paper trade",
                        move_pct
                    );
                    log_system_event(supabase, "anti_chase_skip",
                        &format!("Mint: {} — price moved {:.1}% since filter (max {}%)",
                            mint_str, move_pct, max_move_pct)).await;
                    return Ok(None);
                }
            }
        }
    }

    // Apply simulated slippage for paper trades (entry = you pay MORE)
    // (Already applied above per realistic/legacy branch — keep no-op here.)

    // Guard: if we have no price, skip the trade entirely
    if entry_price_usd == 0.0 {
        warn!(
            mint = %mint_str,
            "Skipping paper trade — DexScreener returned no price"
        );
        return Ok(None);
    }

    // Resolve pool address via DexScreener if detection didn't provide one
    let pool_address = match token.event.pool_address {
        Some(p) => Some(p.to_string()),
        None => match jupiter.get_pool_address(&mint_str).await {
            Some(addr) => {
                info!(mint = %mint_str, pool = %addr, "🔍 Resolved pool address via DexScreener");
                Some(addr)
            }
            None => {
                warn!(mint = %mint_str, "Pool address unknown — LP monitoring unavailable");
                None
            }
        },
    };

    // Write position to Supabase
    let detection_latency_ms = {
        let now_ms = chrono::Utc::now().timestamp_millis();
        (now_ms - token.event.detected_at) as i64
    };
    let position_payload = serde_json::json!({
        "mint": mint_str,
        "name": token.event.name,
        "symbol": token.event.symbol,
        "status": "paper",
        "is_paper_trade": true,
        "entry_price_usd": entry_price_usd,
        "sol_spent": buy_amount_sol,
        "token_amount": token_amount,
        "jito_tip_sol": 0.0,
        "tp1_triggered": false,
        "tp2_triggered": false,
        "peak_price_usd": entry_price_usd,
        "peak_multiplier": 1.0,
        "pool_address": pool_address.clone(),
        "dev_wallet": token.event.creator_wallet.to_string(),
        "detection_latency_ms": detection_latency_ms,
        "detection_source": token.event.source.to_string(),
        "entry_hour_utc": chrono::Utc::now().hour(),
        "concurrent_positions": trading_state.open_position_count().await,
        "strategy_version": cfg.strategy.strategy_version.as_deref().unwrap_or("unknown"),
        "sniper_features": token.event.sniper_features,
    });

    let position_id = insert_position(supabase, &position_payload).await?;

    println!(
        "[{}] 📝 PAPER BUY: mint={} | price={:.10} | tokens={:.0} | spent={:.4} SOL",
        chrono::Utc::now().format("%H:%M:%S"),
        mint_str,
        entry_price_usd,
        token_amount,
        buy_amount_sol,
    );

    // Send PositionOpened to monitoring engine
    let mint_for_opened = mint_str.clone();
    let opened = PositionOpened {
        position_id,
        mint: mint_for_opened,
        entry_price_usd,
        sol_spent: buy_amount_sol,
        token_amount,
        is_paper_trade: true,
        dev_wallet: Some(token.event.creator_wallet.to_string()),
        dev_initial_balance: None,
        pool_address,
        // BUG FIX (2026-04-27): was None — broke v14 paper-paths B/C/D because
        // monitor.evaluate_paper_paths_detail(None) returns no match. Pass the
        // actual features so Path B (liq_floor), C (off_hours_low_vol), and D
        // (bc_score>=70) can fire from the early-promote intercept.
        sniper_features: token.event.sniper_features.clone(),
        initial_liquidity_sol: token.event.initial_liquidity_sol,
        detection_source: token.event.source.to_string(),
        token_name: token.event.name.clone(),
        token_symbol: token.event.symbol.clone(),
    };

    if tx.send(opened).await.is_err() {
        warn!("Execution → monitoring channel closed");
    }

    // Log paper trade latency
    let total_ms = trade_start.elapsed().as_millis() as i64;
    info!(
        mint = %mint_str,
        price_ms,
        total_ms,
        "⏱️ Paper trade latency"
    );
    let supabase_bg = supabase.clone();
    let mint_bg = mint_str.clone();
    let effective_buy_slippage_bps: f64 = if used_jupiter_buy_quote {
        buy_quote_impact_bps
    } else {
        slippage_bps as f64
    };
    let tx_sig_label = if used_jupiter_buy_quote { "paper_jupiter_quote" } else { "paper_dexscreener" };
    tokio::spawn(async move {
        let latency_payload = serde_json::json!({
            "position_id": position_id,
            "mint": mint_bg,
            "side": "buy",
            "quote_ms": 0,
            "swap_tx_ms": 0,
            "sign_ms": 0,
            "submit_confirm_ms": 0,
            "price_derive_ms": price_ms,
            "total_ms": total_ms,
            "used_jito": false,
            "used_helius_sender": false,
            "tx_sig": tx_sig_label,
        });
        log_latency(&supabase_bg, &latency_payload).await;

        // Paper buy: all fees are 0, only simulated slippage has cost
        let slippage_cost = buy_amount_sol * (effective_buy_slippage_bps / 10_000.0);
        let cost_payload = serde_json::json!({
            "position_id": position_id,
            "mint": mint_bg,
            "side": "buy",
            "is_paper_trade": true,
            "sol_amount": buy_amount_sol,
            "token_amount": token_amount,
            "token_price_usd": entry_price_usd,
            "sol_usd_price": sol_usd,
            "network_fee_sol": 0.0,
            "priority_fee_sol": 0.0,
            "jito_tip_sol": 0.0,
            "helius_tip_sol": 0.0,
            "total_fees_sol": 0.0,
            "slippage_bps": effective_buy_slippage_bps,
            "expected_sol": buy_amount_sol,
            "actual_sol": buy_amount_sol,
            "slippage_cost_sol": slippage_cost,
            "tx_sig": tx_sig_label,
            "execution_ms": total_ms,
        });
        log_trade_cost(&supabase_bg, &cost_payload).await;
    });

    Ok(Some(TradeResult {
        position_id,
        entry_price_usd,
        token_amount,
    }))
}

// ─── Real trade execution ────────────────────────────────────

/// Position data returned after a successful trade, for post-buy verification.
struct TradeResult {
    position_id: i64,
    entry_price_usd: f64,
    token_amount: f64,
}

async fn execute_real_trade(
    cfg: &AppConfig,
    jupiter: &JupiterClient,
    supabase: &SupabaseClient,
    rpc: &RpcClient,
    backup_rpc: &RpcClient,
    wallet: &BotWallet,
    token: &FilteredToken,
    tx: &mpsc::Sender<PositionOpened>,
    trading_state: &TradingState,
    jito_client: Option<&JitoClient>,
    helius_sender: Option<&HeliusSenderClient>,
) -> Result<Option<TradeResult>> {
    let trade_start = Instant::now();
    let mint_str = token.event.mint.to_string();
    let buy_amount_sol = compute_dynamic_buy_amount(cfg, token.event.initial_liquidity_sol);
    let amount_lamports = (buy_amount_sol * 1_000_000_000.0) as u64;

    info!(
        mint = %mint_str,
        initial_liquidity_sol = token.event.initial_liquidity_sol,
        buy_amount_sol = format!("{:.4}", buy_amount_sol),
        "📐 Dynamic position size computed"
    );

    // Snapshot wallet balance before buy for cost tracking
    let pre_buy_balance = match rpc.get_balance(&wallet.pubkey()).await {
        Ok(lamports) => Some(lamports_to_sol(lamports)),
        Err(_) => None,
    };

    // Step 1: Get Jupiter quote (with built-in NO_ROUTES retry)
    // v4: reduced from 5→3 retries to cut Jupiter API burn.
    let step1_start = Instant::now();
    let quote = {
        const QUOTE_RETRIES: u32 = 3;
        const QUOTE_RETRY_DELAY_SECS: u64 = 2;
        let mut last_err: Option<anyhow::Error> = None;
        let mut result_quote = None;

        for attempt in 1..=QUOTE_RETRIES {
            match jupiter
                .get_quote(SOL_MINT, &mint_str, amount_lamports, cfg.strategy.execution.slippage_bps)
                .await
            {
                Ok(q) => {
                    if attempt > 1 {
                        info!(mint = %mint_str, attempt, "✅ Jupiter route found on retry");
                    }
                    result_quote = Some(q);
                    break;
                }
                Err(e) if e.to_string().contains("NO_ROUTES_FOUND") => {
                    warn!(
                        mint = %mint_str, attempt,
                        elapsed_ms = step1_start.elapsed().as_millis() as u64,
                        "⏳ Jupiter route not ready — retrying in {}s", QUOTE_RETRY_DELAY_SECS
                    );
                    last_err = Some(e);
                    if attempt < QUOTE_RETRIES {
                        tokio::time::sleep(std::time::Duration::from_secs(QUOTE_RETRY_DELAY_SECS)).await;
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        match result_quote {
            Some(q) => q,
            None => {
                log_system_event(supabase, "no_route_skip",
                    &format!("Mint: {} — no Jupiter route after {} retries", mint_str, QUOTE_RETRIES)).await;
                return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("No route found")));
            }
        }
    };
    let quote_ms = step1_start.elapsed().as_millis() as i64;
    info!(
        mint = %mint_str,
        step = "quote",
        elapsed_ms = quote_ms,
        "⏱️ Step 1: Jupiter quote"
    );

    let out_amount: f64 = quote.out_amount.parse().unwrap_or(0.0);

    // ── Anti-chase: check if price moved too much since filter time ──
    // Uses the buy quote we already have — zero extra API calls.
    let max_move_pct = cfg.strategy.execution.max_entry_price_move_pct;
    if max_move_pct > 0.0 && out_amount > 0.0 {
        if let Some(filter_price) = token.filter_price_usd {
            if filter_price > 0.0 {
                // Derive SOL-denominated price from quote: sol_spent / tokens_received
                let token_ui = out_amount / 1_000_000.0; // 6 decimals
                let quote_price_sol = buy_amount_sol / token_ui;
                // Convert filter_price (USD) to SOL-denominated
                if let Ok(sol_usd) = jupiter.get_price(SOL_MINT).await {
                    if sol_usd > 0.0 {
                        let filter_price_sol = filter_price / sol_usd;
                        if filter_price_sol > 0.0 {
                            let move_pct = ((quote_price_sol - filter_price_sol) / filter_price_sol) * 100.0;
                            debug!(
                                mint = %mint_str,
                                quote_price_sol = format!("{:.12}", quote_price_sol),
                                filter_price_sol = format!("{:.12}", filter_price_sol),
                                move_pct = format!("{:.1}%", move_pct),
                                "Anti-chase: price move since filter (from buy quote)"
                            );
                            if move_pct > max_move_pct || move_pct < -max_move_pct {
                                warn!(
                                    mint = %mint_str,
                                    move_pct = format!("{:.1}%", move_pct),
                                    max_allowed = format!("{:.1}%", max_move_pct),
                                    "⏭️ Anti-chase: price moved {:.1}% since filter — aborting trade",
                                    move_pct
                                );
                                log_system_event(supabase, "anti_chase_skip",
                                    &format!("Mint: {} — price moved {:.1}% since filter (max {}%)",
                                        mint_str, move_pct, max_move_pct)).await;
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }
    }

    // Step 2: Get swap transaction IMMEDIATELY after quote
    // Use standard getRecentPrioritizationFees via Chainstack primary RPC.
    let step2_start = Instant::now();
    let priority_fee_rpc = cfg.env.solana_rpc_url.as_str();
    let dynamic_priority_fee = helius_sender::get_priority_fee_estimate(
        priority_fee_rpc,
        &[SOL_MINT, &mint_str, "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"],
        &cfg.strategy.execution.priority_level,
        cfg.strategy.execution.priority_fee_max_lamports,
    ).await;
    // Cap at the configured maximum to prevent runaway fees
    let priority_fee_lamports = dynamic_priority_fee.min(cfg.strategy.execution.priority_fee_max_lamports);
    debug!(
        mint = %mint_str,
        dynamic_priority_fee,
        capped = priority_fee_lamports,
        "Priority fee estimate (buy)"
    );

    let swap_tx_b64 = jupiter
        .get_swap_transaction(
            &quote.raw,
            &wallet.pubkey().to_string(),
            None,
            Some((priority_fee_lamports, &cfg.strategy.execution.priority_level)),
        )
        .await?;
    let swap_tx_ms = step2_start.elapsed().as_millis() as i64;
    info!(
        mint = %mint_str,
        step = "swap_tx",
        elapsed_ms = swap_tx_ms,
        "⏱️ Step 2: Jupiter swap tx"
    );

    // Step 3: Decode, deserialize, and sign (local — fast)
    let step3_start = Instant::now();
    let tx_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &swap_tx_b64,
    )
    .map_err(|e| anyhow::anyhow!("Failed to decode swap tx base64: {}", e))?;

    let mut versioned_tx: solana_sdk::transaction::VersionedTransaction =
        bincode::deserialize(&tx_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize versioned tx: {}", e))?;

    // No tip injection needed — Chainstack Warp handles routing via bloXroute SwQoS + Jito.
    let helius_tip_sol = 0.0;

    let mut signed_tx = versioned_tx.clone();
    signed_tx.signatures[0] = wallet.sign_transaction(&versioned_tx)?;

    if signed_tx.signatures[0] == solana_sdk::signature::Signature::default() {
        error!(mint = %mint_str, "Transaction signature is default — aborting");
        return Ok(None);
    }
    let sign_ms = step3_start.elapsed().as_millis() as i64;
    info!(
        mint = %mint_str,
        step = "sign",
        elapsed_ms = sign_ms,
        "⏱️ Step 3: Decode + sign"
    );

    // Step 4: Submit transaction (Chainstack Warp, Jito bundle, or regular RPC)
    let step4_start = Instant::now();
    let (tx_sig, jito_tip_sol) = if cfg.env.use_helius_sender {
        let hs = helius_sender.ok_or_else(|| anyhow::anyhow!("Warp TX sender enabled but client not initialized"))?;

        // Dual-submit: fire-and-forget same signed tx to backup RPC for redundancy.
        let backup_tx = signed_tx.clone();
        let backup_url = cfg.env.solana_rpc_backup_url.clone();
        let mint_log = mint_str.to_string();
        tokio::spawn(async move {
            let backup = RpcClient::new(backup_url);
            let cfg = solana_client::rpc_config::RpcSendTransactionConfig {
                skip_preflight: true,
                ..Default::default()
            };
            match backup.send_transaction_with_config(&backup_tx, cfg).await {
                Ok(sig) => tracing::debug!(mint = %mint_log, sig = %sig, "📤 Dual-submit: backup RPC accepted"),
                Err(e) => tracing::debug!(mint = %mint_log, "Dual-submit backup failed (non-critical): {}", e),
            }
        });

        let sig = hs.send_transaction(&signed_tx).await?;
        info!(mint = %mint_str, sig = %sig, "📤 Buy tx submitted via Chainstack Warp (+ backup)");
        (sig, 0.0)
    } else if cfg.env.use_jito {
        submit_via_jito(&cfg, &wallet, signed_tx, &mint_str, supabase, jito_client).await?
    } else {
        let tx_sig = submit_via_rpc(rpc, backup_rpc, &signed_tx, &mint_str, supabase).await?;
        (tx_sig, 0.0)
    };

    // When using Jito, send_bundle_and_wait already confirms the bundle.
    // Helius Sender and regular RPC need polling for confirmation.
    if !cfg.env.use_jito || cfg.env.use_helius_sender {
        let confirm_start = Instant::now();
        let mut confirmed = false;
        let tx_confirm_timeout =
            std::time::Duration::from_secs(cfg.strategy.execution.tx_confirm_timeout_secs);
        let tx_confirm_poll =
            std::time::Duration::from_millis(cfg.strategy.execution.tx_confirm_poll_ms);

        while confirm_start.elapsed() < tx_confirm_timeout {
            match rpc.get_signature_statuses(&[tx_sig]).await {
                Ok(statuses) => {
                    if let Some(Some(status)) = statuses.value.first() {
                        if status.err.is_none() {
                            confirmed = true;
                            break;
                        } else {
                            error!(
                                mint = %mint_str,
                                sig = %tx_sig,
                                "Transaction confirmed with error: {:?}",
                                status.err
                            );
                            return Ok(None);
                        }
                    }
                }
                Err(e) => {
                    debug!("Confirmation poll error: {}", e);
                }
            }
            tokio::time::sleep(tx_confirm_poll).await;
        }

        if !confirmed {
            error!(
                mint = %mint_str,
                sig = %tx_sig,
                "⏰ Transaction confirmation timed out after {}s",
                cfg.strategy.execution.tx_confirm_timeout_secs
            );
            return Ok(None);
        }
    }
    let submit_confirm_ms = step4_start.elapsed().as_millis() as i64;
    info!(
        mint = %mint_str,
        step = "submit_confirm",
        elapsed_ms = submit_confirm_ms,
        sig = %tx_sig,
        "⏱️ Step 4: Submit + confirm"
    );

    // Step 5: Derive entry price from quote (instant — no API calls)
    // Use the quote out_amount + SOL/USD price to compute entry price.
    // This avoids the 0-16s DexScreener retry loop entirely.
    let step5_start = Instant::now();
    let entry_price_usd = {
        let mut price = 0.0;

        // Primary: derive from quote (always available immediately)
        if out_amount > 0.0 {
            if let Ok(sol_usd) = jupiter.get_price(SOL_MINT).await {
                if sol_usd > 0.0 {
                    let token_decimals: f64 = 6.0; // pump.fun standard
                    let token_amount_ui = out_amount / 10_f64.powf(token_decimals);
                    if token_amount_ui > 0.0 {
                        price = (buy_amount_sol * sol_usd) / token_amount_ui;
                    }
                }
            }
        }

        // Fallback: try DexScreener once (no retries — don't block)
        if price <= 0.0 {
            if let Ok(p) = jupiter.get_price(&mint_str).await {
                if p > 0.0 {
                    price = p;
                }
            }
        }

        if price <= 0.0 {
            warn!(
                mint = %mint_str,
                "⚠️ entry_price_usd is 0 — price derivation failed. \
                 Triggers using price multiplier will be unreliable."
            );
        }

        price
    };
    let price_derive_ms = step5_start.elapsed().as_millis() as i64;
    info!(
        mint = %mint_str,
        step = "price",
        elapsed_ms = price_derive_ms,
        entry_price_usd,
        "⏱️ Step 5: Entry price derived"
    );

    // Step 6: Use quote out_amount as token amount (instant — no RPC calls).
    // Verification against on-chain balance happens in background.
    let token_amount = if out_amount > 0.0 {
        out_amount
    } else {
        // Last resort: try one RPC balance check (no retries)
        let bal = fetch_token_balance(rpc, &wallet.pubkey(), &mint_str).await;
        bal.unwrap_or(0.0)
    };

    if token_amount <= 0.0 {
        error!(
            mint = %mint_str,
            out_amount,
            "token_amount is 0 after buy confirmation — skipping position creation"
        );
        log_system_event(
            supabase,
            "buy_token_amount_zero",
            &format!(
                "Mint: {} — out_amount={} but resolved token_amount=0. Position not created.",
                mint_str, out_amount
            ),
        )
        .await;
        return Ok(None);
    }

    // Step 7: Update in-memory state immediately, write Supabase in background
    // This means monitoring starts ASAP without waiting for DB.
    trading_state.record_buy(&mint_str, buy_amount_sol).await;

    let pool_address = match token.event.pool_address {
        Some(p) => Some(p.to_string()),
        None => {
            // Try DexScreener once — don't block with retries
            jupiter.get_pool_address(&mint_str).await
        }
    };

    let detection_latency_ms = {
        let now_ms = chrono::Utc::now().timestamp_millis();
        (now_ms - token.event.detected_at) as i64
    };
    let position_payload = serde_json::json!({
        "mint": mint_str,
        "name": token.event.name,
        "symbol": token.event.symbol,
        "status": "open",
        "is_paper_trade": false,
        "entry_tx_sig": tx_sig.to_string(),
        "entry_price_usd": entry_price_usd,
        "sol_spent": buy_amount_sol,
        "token_amount": token_amount,
        "jito_tip_sol": jito_tip_sol,
        "tp1_triggered": false,
        "tp2_triggered": false,
        "peak_price_usd": entry_price_usd,
        "peak_multiplier": 1.0,
        "pool_address": pool_address.clone(),
        "dev_wallet": token.event.creator_wallet.to_string(),
        "detection_latency_ms": detection_latency_ms,
        "detection_source": token.event.source.to_string(),
        "entry_hour_utc": chrono::Utc::now().hour(),
        "concurrent_positions": trading_state.open_position_count().await,
        "strategy_version": cfg.strategy.strategy_version.as_deref().unwrap_or("unknown"),
        "sniper_features": token.event.sniper_features,
    });

    // Insert position to Supabase — still awaited because monitoring needs position_id
    let position_id = insert_position(supabase, &position_payload).await?;

    let total_elapsed = trade_start.elapsed();
    println!(
        "[{}] ✅ REAL BUY: mint={} | price={:.10} | tokens={:.0} | spent={:.4} SOL | sig={} | ⏱️ {}ms total",
        chrono::Utc::now().format("%H:%M:%S"),
        mint_str,
        entry_price_usd,
        token_amount,
        buy_amount_sol,
        tx_sig,
        total_elapsed.as_millis(),
    );

    // Log wallet SOL balance after buy
    let post_buy_balance = match rpc.get_balance(&wallet.pubkey()).await {
        Ok(lamports) => {
            let bal = lamports_to_sol(lamports);
            info!(
                mint = %mint_str,
                balance_sol = format!("{:.4}", bal),
                "💰 Wallet SOL balance after BUY"
            );
            println!("  💰 Wallet balance: {:.4} SOL", bal);
            Some(bal)
        }
        Err(e) => {
            warn!("Failed to fetch SOL balance after buy: {}", e);
            None
        }
    };
    info!(
        mint = %mint_str,
        total_ms = total_elapsed.as_millis() as u64,
        "⏱️ TOTAL BUY LATENCY: {}ms",
        total_elapsed.as_millis()
    );

    // Send PositionOpened to monitoring engine BEFORE background DB writes
    let opened = PositionOpened {
        position_id,
        mint: mint_str.clone(),
        entry_price_usd,
        sol_spent: buy_amount_sol,
        token_amount,
        is_paper_trade: false,
        dev_wallet: Some(token.event.creator_wallet.to_string()),
        dev_initial_balance: None,
        pool_address,
        // BUG FIX (2026-04-27): see paper-buy branch — None broke v14 B/C/D paths.
        sniper_features: token.event.sniper_features.clone(),
        initial_liquidity_sol: token.event.initial_liquidity_sol,
        detection_source: token.event.source.to_string(),
        token_name: token.event.name.clone(),
        token_symbol: token.event.symbol.clone(),
    };

    if tx.send(opened).await.is_err() {
        warn!("Execution → monitoring channel closed");
    }

    // Background: log latency + verify on-chain token balance
    // None of this blocks the monitoring start
    let supabase_bg = supabase.clone();
    let rpc_url = cfg.env.solana_rpc_url.clone();
    let mint_bg = mint_str.clone();
    let wallet_pubkey = wallet.pubkey();
    let use_jito = cfg.env.use_jito;
    let use_helius_sender = cfg.env.use_helius_sender;
    let tx_sig_str = tx_sig.to_string();
    let balance_bg = post_buy_balance;
    let slippage_bps_bg = cfg.strategy.execution.slippage_bps;
    let priority_fee_sol = lamports_to_sol(priority_fee_lamports);
    let network_fee_sol = 0.000005_f64; // base tx fee: 5000 lamports
    // helius_tip_sol or jito_tip_sol is already captured in jito_tip_sol
    let tip_sol_bg = jito_tip_sol;
    let is_helius = cfg.env.use_helius_sender;
    let pre_buy_bal_bg = pre_buy_balance;
    tokio::spawn(async move {
        // Log balance event to Supabase (fire-and-forget)
        if let Some(bal) = balance_bg {
            log_system_event(&supabase_bg, "balance_after_buy",
                &format!("Mint: {} | SOL balance: {:.4}", mint_bg, bal)).await;
        }

        // Log latency to Supabase (fire-and-forget)
        let latency_payload = serde_json::json!({
            "position_id": position_id,
            "mint": mint_bg,
            "side": "buy",
            "quote_ms": quote_ms,
            "swap_tx_ms": swap_tx_ms,
            "sign_ms": sign_ms,
            "submit_confirm_ms": submit_confirm_ms,
            "price_derive_ms": price_derive_ms,
            "total_ms": total_elapsed.as_millis() as i64,
            "used_jito": use_jito,
            "used_helius_sender": use_helius_sender,
            "tx_sig": tx_sig_str,
        });
        log_latency(&supabase_bg, &latency_payload).await;

        // Log cost breakdown for the buy side
        let total_fees = network_fee_sol + priority_fee_sol + tip_sol_bg;
        let wallet_change = balance_bg.and_then(|after| pre_buy_bal_bg.map(|before| after - before));
        let cost_payload = serde_json::json!({
            "position_id": position_id,
            "mint": mint_bg,
            "side": "buy",
            "is_paper_trade": false,
            "sol_amount": buy_amount_sol,
            "token_amount": token_amount,
            "token_price_usd": entry_price_usd,
            "network_fee_sol": network_fee_sol,
            "priority_fee_sol": priority_fee_sol,
            "jito_tip_sol": if !is_helius { tip_sol_bg } else { 0.0 },
            "helius_tip_sol": if is_helius { tip_sol_bg } else { 0.0 },
            "total_fees_sol": total_fees,
            "slippage_bps": slippage_bps_bg,
            "wallet_sol_before": pre_buy_bal_bg,
            "wallet_sol_after": balance_bg,
            "wallet_sol_change": wallet_change,
            "tx_sig": tx_sig_str,
            "execution_ms": total_elapsed.as_millis() as i64,
        });
        log_trade_cost(&supabase_bg, &cost_payload).await;

        // Verify on-chain balance after a short delay
        let bg_rpc = RpcClient::new(rpc_url);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        if let Some(on_chain_bal) = fetch_token_balance(&bg_rpc, &wallet_pubkey, &mint_bg).await {
            if on_chain_bal > 0.0 && (on_chain_bal - token_amount).abs() > 1.0 {
                warn!(
                    mint = %mint_bg,
                    quote_amount = token_amount,
                    on_chain_balance = on_chain_bal,
                    "🔧 Background: token amount mismatch — updating Supabase"
                );
                let update = serde_json::json!({
                    "token_amount": on_chain_bal,
                });
                let _ = update_position_field(&supabase_bg, position_id, &update).await;
            }
        }
    });

    Ok(Some(TradeResult {
        position_id,
        entry_price_usd,
        token_amount,
    }))
}

// ─── Transaction submission helpers ──────────────────────────────

/// Submit transaction via Jito bundle
async fn submit_via_jito(
    cfg: &AppConfig,
    wallet: &BotWallet,
    signed_swap_tx: solana_sdk::transaction::VersionedTransaction,
    mint: &str,
    supabase: &SupabaseClient,
    shared_jito: Option<&JitoClient>,
) -> Result<(solana_sdk::signature::Signature, f64)> {
    use solana_sdk::transaction::Transaction;

    let jito_start = Instant::now();
    info!(mint = %mint, "🚀 Submitting via Jito bundle");

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

    info!(
        mint = %mint,
        tip_lamports = tip_lamports,
        tip_sol = format!("{:.6}", tip_sol),
        elapsed_ms = jito_start.elapsed().as_millis() as u64,
        "⏱️ Jito tip calculated"
    );

    let tip_instruction = JitoClient::create_tip_instruction(&wallet.pubkey(), tip_lamports)?;

    let recent_blockhash = signed_swap_tx.message.recent_blockhash();
    let tip_tx = Transaction::new_signed_with_payer(
        &[tip_instruction],
        Some(&wallet.pubkey()),
        &[wallet.keypair()],
        *recent_blockhash,
    );

    let tip_versioned_tx = solana_sdk::transaction::VersionedTransaction::from(tip_tx);
    let bundle = vec![signed_swap_tx.clone(), tip_versioned_tx];

    let bundle_start = Instant::now();
    match jito
        .send_bundle_and_wait(bundle, cfg.strategy.execution.tx_confirm_timeout_secs)
        .await
    {
        Ok(bundle_id) => {
            info!(
                mint = %mint,
                bundle_id = %bundle_id,
                tip_sol = format!("{:.6}", tip_sol),
                bundle_ms = bundle_start.elapsed().as_millis() as u64,
                total_jito_ms = jito_start.elapsed().as_millis() as u64,
                "⏱️ Jito bundle confirmed"
            );
            let tx_sig = signed_swap_tx.signatures[0];
            Ok((tx_sig, tip_sol))
        }
        Err(e) => {
            error!(mint = %mint, "❌ Jito bundle submission failed: {}", e);
            log_system_event(
                supabase,
                "jito_bundle_failed",
                &format!("Mint: {} — Error: {}", mint, e),
            )
            .await;
            Err(e)
        }
    }
}

/// Submit transaction via regular RPC with automatic backup fallback on 429
async fn submit_via_rpc(
    rpc: &RpcClient,
    backup_rpc: &RpcClient,
    signed_tx: &solana_sdk::transaction::VersionedTransaction,
    mint: &str,
    supabase: &SupabaseClient,
) -> Result<solana_sdk::signature::Signature> {
    use solana_client::rpc_config::RpcSendTransactionConfig;

    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        encoding: None,
        max_retries: None,
        min_context_slot: None,
    };

    info!(mint = %mint, "📤 Submitting via primary RPC");

    let send_result = rpc
        .send_transaction_with_config(signed_tx, send_config)
        .await;

    match send_result {
        Ok(sig) => {
            info!(mint = %mint, sig = %sig, "📤 Transaction submitted via primary RPC");
            return Ok(sig);
        }
        Err(e) => {
            let err_msg = e.to_string();
            if err_msg.contains("429") || err_msg.contains("Too Many Requests") {
                warn!(mint = %mint, "Primary RPC 429 — trying backup RPC");
            } else {
                error!(mint = %mint, "❌ Transaction submission failed: {}", e);
                log_system_event(
                    supabase,
                    "tx_submit_failed",
                    &format!("Mint: {} — Error: {}", mint, e),
                )
                .await;
                return Err(anyhow::anyhow!("Transaction submission failed: {}", e));
            }
        }
    }

    // Backup RPC attempt
    let backup_config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        encoding: None,
        max_retries: None,
        min_context_slot: None,
    };

    match backup_rpc
        .send_transaction_with_config(signed_tx, backup_config)
        .await
    {
        Ok(sig) => {
            info!(mint = %mint, sig = %sig, "📤 Transaction submitted via backup RPC");
            Ok(sig)
        }
        Err(e2) => {
            error!(mint = %mint, "❌ Backup RPC also failed: {}", e2);
            log_system_event(
                supabase,
                "tx_submit_failed",
                &format!("Mint: {} — Primary: 429, Backup: {}", mint, e2),
            )
            .await;
            Err(anyhow::anyhow!("Transaction submission failed on both RPCs: {}", e2))
        }
    }
}

// ─── Token balance helper ─────────────────────────────────────

/// Fetch the actual SPL token balance (as human-readable units) for a
/// wallet+mint from the chain after a confirmed buy.
/// Uses `amount` (raw smallest-unit string) so the value is consistent
/// with what Jupiter expects for sell quotes.
async fn fetch_token_balance(
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

    if balance > 0.0 {
        Some(balance)
    } else {
        None
    }
}

// ─── Supabase helpers ────────────────────────────────────────

/// Insert a position into Supabase and return the assigned ID.
async fn insert_position(
    supabase: &SupabaseClient,
    payload: &serde_json::Value,
) -> Result<i64> {
    let url = format!("{}/positions", supabase.base_url);
    let resp = supabase
        .client
        .post(&url)
        .header("Prefer", "return=representation")
        .json(payload)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Failed to insert position: HTTP {} — {}", status, body);
    }

    let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
    let id = rows
        .first()
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    Ok(id)
}

/// Update specific fields on an existing position row.
async fn update_position_field(
    supabase: &SupabaseClient,
    position_id: i64,
    payload: &serde_json::Value,
) -> Result<()> {
    let url = format!("{}/positions?id=eq.{}", supabase.base_url, position_id);
    let resp = supabase
        .client
        .patch(&url)
        .json(payload)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Failed to update position {}: HTTP {} — {}", position_id, status, body);
    }

    Ok(())
}

/// Get count of open positions from Supabase (includes partial positions).
async fn get_open_position_count(supabase: &SupabaseClient, paper_trade: bool) -> i64 {
    let status_filter = if paper_trade {
        "or=(status.eq.paper,status.eq.partial)"
    } else {
        "or=(status.eq.open,status.eq.partial)"
    };
    // Exclude positions that already have an exit transaction — they are
    // effectively closed even if still marked "partial" (dust tokens).
    let url = format!(
        "{}/positions?select=id&{}&is_paper_trade=eq.{}&exit_tx_sig=is.null",
        supabase.base_url, status_filter, paper_trade
    );

    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
            rows.len() as i64
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to count open positions: {}", body);
            0
        }
        Err(e) => {
            warn!("Failed to count open positions: {}", e);
            0
        }
    }
}

/// Get total portfolio exposure (sum of sol_spent for open/partial positions).
async fn get_portfolio_exposure(supabase: &SupabaseClient, paper_trade: bool) -> f64 {
    let status_filter = if paper_trade {
        "or=(status.eq.paper,status.eq.partial)"
    } else {
        "or=(status.eq.open,status.eq.partial)"
    };
    let url = format!(
        "{}/positions?select=sol_spent&{}&is_paper_trade=eq.{}&exit_tx_sig=is.null",
        supabase.base_url, status_filter, paper_trade
    );

    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
            rows.iter()
                .filter_map(|r| r.get("sol_spent").and_then(|v| v.as_f64()))
                .sum()
        }
        _ => 0.0,
    }
}

/// Get today's total PnL from closed positions.
/// Excludes "ghost" positions (recovery_closed with 0 SOL received) where the
/// buy confirmed but no tokens were actually received — these are phantom losses
/// that should not block new trades.
async fn get_today_pnl(supabase: &SupabaseClient, paper_trade: bool) -> f64 {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let url = format!(
        "{}/positions?select=pnl_sol,sol_received,exit_reason&status=eq.closed&is_paper_trade=eq.{}&exit_time=gte.{}",
        supabase.base_url, paper_trade, today
    );

    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
            rows.iter()
                .filter_map(|r| {
                    let pnl = r.get("pnl_sol").and_then(|v| v.as_f64())?;
                    let sol_received = r.get("sol_received").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let exit_reason = r.get("exit_reason").and_then(|v| v.as_str()).unwrap_or("");
                    // Skip ghost positions: recovery closed with no SOL returned
                    if exit_reason.contains("recovery_closed") && sol_received <= 0.0 {
                        debug!(
                            exit_reason,
                            pnl,
                            "Excluding ghost position from daily PnL"
                        );
                        return None;
                    }
                    Some(pnl)
                })
                .sum()
        }
        _ => 0.0,
    }
}

/// Check if there's already an active position for this mint.
async fn has_existing_position(supabase: &SupabaseClient, mint: &str, paper_trade: bool) -> bool {
    let status_filter = if paper_trade {
        "or=(status.eq.paper,status.eq.partial)"
    } else {
        "or=(status.eq.open,status.eq.partial)"
    };
    let url = format!(
        "{}/positions?select=id&{}&mint=eq.{}&limit=1",
        supabase.base_url, status_filter, mint
    );
    match supabase.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let rows: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
            !rows.is_empty()
        }
        _ => false,
    }
}

/// Log a system event to Supabase.
async fn log_system_event(supabase: &SupabaseClient, event_type: &str, message: &str) {
    let payload = serde_json::json!({
        "event_type": event_type,
        "message": message,
    });
    let url = format!("{}/system_events", supabase.base_url);
    match supabase.client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to log system event: {}", body);
        }
        Err(e) => {
            warn!("Failed to log system event: {}", e);
        }
    }
}

/// Log trade latency breakdown to Supabase `trade_latency` table.
async fn log_latency(supabase: &SupabaseClient, payload: &serde_json::Value) {
    let url = format!("{}/trade_latency", supabase.base_url);
    match supabase.client.post(&url).json(payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            debug!("Latency logged to Supabase");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to log latency: {}", body);
        }
        Err(e) => {
            warn!("Failed to log latency: {}", e);
        }
    }
}

/// Write a row to the `trade_costs` table (fire-and-forget from background tasks).
pub async fn log_trade_cost(supabase: &SupabaseClient, payload: &serde_json::Value) {
    let url = format!("{}/trade_costs", supabase.base_url);
    match supabase.client.post(&url).json(payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            debug!("Trade cost logged to Supabase");
        }
        Ok(resp) => {
            let body = resp.text().await.unwrap_or_default();
            warn!("Failed to log trade cost: {}", body);
        }
        Err(e) => {
            warn!("Failed to log trade cost: {}", e);
        }
    }
}
