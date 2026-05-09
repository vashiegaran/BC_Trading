//! Build the comprehensive sniper_features JSONB from enrichment results.
//!
//! This is the training data — 60+ fields logged for every candidate.

use chrono::Utc;
use serde_json::json;
use tracing::debug;

use super::types::*;

/// Bonding curve data from the GraduatedToken detection event.
pub struct BondingCurveContext {
    pub bonding_curve_volume_sol: f64,
    pub buy_pressure_pct: f64,
    pub time_to_graduate_seconds: f64,
    pub unique_buyer_count: usize,
    pub buy_count: u64,
    pub sell_count: u64,
}

/// Assemble the full sniper_features JSONB from enrichment data + detection context.
pub fn build_sniper_features(
    enrichment: &EnrichmentResult,
    initial_liquidity_sol: f64,
    initial_liquidity_usd: f64,
    detected_at_ms: i64,
    detection_latency_ms: i64,
    sol_price_usd: f64,
    soft_flags: &SoftFlags,
    bonding: &BondingCurveContext,
) -> serde_json::Value {
    let now = Utc::now();
    let hour_utc = now.hour() as u8;
    let day_of_week = now.weekday().num_days_from_monday() as u8;
    let is_us_hours = hour_utc >= 13 && hour_utc <= 23; // 9am-7pm EST

    // Detection-time price from Birdeye if available (Gap 2)
    let detection_price_usd = enrichment.birdeye_overview.as_ref().and_then(|bo| bo.price);

    let mut features = json!({
        // ── Detection context ──
        "detected_at_ms": detected_at_ms,
        "detection_latency_ms": detection_latency_ms,
        "initial_liquidity_sol": initial_liquidity_sol,
        "initial_liquidity_usd": initial_liquidity_usd,
        "detection_price_usd": detection_price_usd,

        // ── Bonding curve data (from GraduatedToken) ──
        "bonding_curve_volume_sol": bonding.bonding_curve_volume_sol,
        "bonding_buy_pressure_pct": bonding.buy_pressure_pct,
        "time_to_graduate_seconds": bonding.time_to_graduate_seconds,
        "bonding_unique_buyer_count": bonding.unique_buyer_count,
        "bonding_buy_count": bonding.buy_count,
        "bonding_sell_count": bonding.sell_count,

        // ── Enrichment metadata ──
        "enrichment_duration_ms": enrichment.enrichment_duration_ms,
        "sources_completed": enrichment.sources_completed,
        "sources_timed_out": enrichment.sources_timed_out,

        // ── Market context ──
        "sol_price_usd": sol_price_usd,
        "hour_utc": hour_utc,
        "day_of_week": day_of_week,
        "is_us_hours": is_us_hours,

        // ── Soft flags ──
        "flag_high_bundlers": soft_flags.flag_high_bundlers,
        "flag_serial_creator": soft_flags.flag_serial_creator,
        "flag_fast_graduation": soft_flags.flag_fast_graduation,
        "flag_low_holders": soft_flags.flag_low_holders,
        "flag_high_concentration": soft_flags.flag_high_concentration,
        "flag_honeypot_goplus": soft_flags.flag_honeypot_goplus,
        "flag_wash_trade_ring": soft_flags.flag_wash_trade_ring,
        "flag_suspicious_holders": soft_flags.flag_suspicious_holders,
        "flag_no_socials": soft_flags.flag_no_socials,
        "flag_mintable_any_source": soft_flags.flag_mintable_any_source,
        "flag_lpi_divergence": soft_flags.flag_lpi_divergence,
        "flag_dev_holds": soft_flags.flag_dev_holds,
        "flag_sniper_heavy": soft_flags.flag_sniper_heavy,
        "flag_lp_not_burned": soft_flags.flag_lp_not_burned,
        "flag_transfer_pausable": soft_flags.flag_transfer_pausable,
        "flag_ownership_reclaimable": soft_flags.flag_ownership_reclaimable,
        "flag_low_risk_score": soft_flags.flag_low_risk_score,
    });

    let map = features.as_object_mut().unwrap();

    // ── Solana Tracker fields ──
    if let Some(st) = &enrichment.solana_tracker {
        map.insert("st_dev_pct".into(), json!(st.dev_pct));
        map.insert("st_insiders_pct".into(), json!(st.insiders_pct));
        map.insert("st_bundlers_pct".into(), json!(st.bundlers_pct));
        map.insert("st_snipers_pct".into(), json!(st.snipers_pct));
        map.insert("st_top10_pct".into(), json!(st.top10_pct));
        map.insert("st_risk_score".into(), json!(st.risk_score));
        map.insert("st_holders".into(), json!(st.holders));
        map.insert("st_sniper_count".into(), json!(st.sniper_count));
        map.insert("st_bundler_count".into(), json!(st.bundler_count));
        map.insert("st_insider_count".into(), json!(st.insider_count));
        map.insert("st_total_buys".into(), json!(st.total_buys));
        map.insert("st_total_sells".into(), json!(st.total_sells));
        map.insert("st_lp_burn_pct".into(), json!(st.lp_burn_pct));
        map.insert(
            "st_has_freeze_authority".into(),
            json!(st.has_freeze_authority),
        );
        map.insert("st_has_mint_authority".into(), json!(st.has_mint_authority));
        map.insert("st_jupiter_verified".into(), json!(st.jupiter_verified));
        map.insert("st_rugged".into(), json!(st.rugged));

        // ── Advanced tier: volume / momentum / fees ──
        map.insert("st_volume_5m".into(), json!(st.volume_5m));
        map.insert("st_volume_15m".into(), json!(st.volume_15m));
        map.insert("st_volume_1h".into(), json!(st.volume_1h));
        map.insert("st_volume_24h".into(), json!(st.volume_24h));
        map.insert("st_price_change_5m".into(), json!(st.price_change_5m));
        map.insert("st_price_change_1h".into(), json!(st.price_change_1h));
        map.insert("st_fees_total_sol".into(), json!(st.fees_total_sol));
        map.insert("st_fees_total_tips".into(), json!(st.fees_total_tips));
        map.insert("st_fees_total_trading".into(), json!(st.fees_total_trading));
        map.insert("st_deployer".into(), json!(st.deployer));
        map.insert("st_market".into(), json!(st.market));

        // Volume acceleration (15m vs 5m)
        if let (Some(v5m), Some(v15m)) = (st.volume_5m, st.volume_15m) {
            if v15m > 0.0 {
                map.insert("volume_accel_5m_vs_15m".into(), json!(v5m / (v15m / 3.0)));
            }
        }

        // Tips-to-fees ratio (high = Jito smart money)
        if let (Some(tips), Some(trading)) = (st.fees_total_tips, st.fees_total_trading) {
            if trading > 0.0 {
                map.insert("tips_to_trading_ratio".into(), json!(tips / trading));
            }
        }

        // Derived ratios
        if let (Some(holders), Some(bundlers)) = (st.holders, st.bundler_count) {
            if holders > 0 {
                map.insert(
                    "bundler_to_holder_ratio".into(),
                    json!(bundlers as f64 / holders as f64),
                );
            }
        }
        if let (Some(holders), Some(snipers)) = (st.holders, st.sniper_count) {
            if holders > 0 {
                map.insert(
                    "sniper_to_holder_ratio".into(),
                    json!(snipers as f64 / holders as f64),
                );
            }
        }
        if let (Some(holders), Some(insiders)) = (st.holders, st.insider_count) {
            if holders > 0 {
                map.insert(
                    "insider_to_holder_ratio".into(),
                    json!(insiders as f64 / holders as f64),
                );
            }
        }
        if let (Some(buys), Some(sells)) = (st.total_buys, st.total_sells) {
            if sells > 0 {
                map.insert("buy_sell_ratio".into(), json!(buys as f64 / sells as f64));
            }
        }
    }

    // ── On-chain mint data ──
    if let Some(m) = &enrichment.on_chain_mint {
        map.insert(
            "mint_authority_revoked".into(),
            json!(m.mint_authority_revoked),
        );
        map.insert(
            "freeze_authority_revoked".into(),
            json!(m.freeze_authority_revoked),
        );
        map.insert("token_supply".into(), json!(m.supply));
    }

    // ── PumpFun bonding curve ──
    if let Some(bc) = &enrichment.pumpfun_curve {
        map.insert("sale_duration_mins".into(), json!(bc.sale_duration_mins));
        map.insert("bonding_curve_complete".into(), json!(bc.complete));
    }

    // ── Creator analysis ──
    if let Some(c) = &enrichment.creator_history {
        map.insert("creator_wallet_age_days".into(), json!(c.wallet_age_days));
        map.insert(
            "creator_previous_tokens".into(),
            json!(c.previous_token_count),
        );
        map.insert("creator_is_serial".into(), json!(c.is_serial_creator));
    }

    // ── Birdeye token overview ──
    if let Some(bo) = &enrichment.birdeye_overview {
        map.insert("be_price_usd".into(), json!(bo.price));
        map.insert("be_market_cap_usd".into(), json!(bo.mc));
        map.insert("be_volume_24h_usd".into(), json!(bo.v24h_usd));
        map.insert("be_unique_wallets_24h".into(), json!(bo.unique_wallet_24h));
        map.insert("be_trades_24h".into(), json!(bo.trade_24h));
        map.insert("be_buys_24h".into(), json!(bo.buy_24h));
        map.insert("be_sells_24h".into(), json!(bo.sell_24h));
        map.insert("be_liquidity_usd".into(), json!(bo.liquidity));
        map.insert("be_holder_count".into(), json!(bo.holder));
        map.insert("be_last_trade_unix".into(), json!(bo.last_trade_unix_time));
        if let Some(ext) = &bo.extensions {
            map.insert("be_has_twitter".into(), json!(ext.twitter.is_some()));
            map.insert("be_has_telegram".into(), json!(ext.telegram.is_some()));
            map.insert("be_has_website".into(), json!(ext.website.is_some()));
        }
    }

    // ── Birdeye token security ──
    if let Some(bs) = &enrichment.birdeye_security {
        map.insert("be_owner_balance_pct".into(), json!(bs.owner_percentage));
        map.insert(
            "be_creator_balance_pct".into(),
            json!(bs.creator_percentage),
        );
        map.insert("be_top10_holder_pct".into(), json!(bs.top10_holder_percent));
        map.insert("be_is_token_2022".into(), json!(bs.is_token_2022));
        map.insert("be_is_proxy".into(), json!(bs.is_proxy));
        map.insert("be_is_mintable".into(), json!(bs.is_mintable));
        map.insert("be_is_mutable".into(), json!(bs.is_mutable));
        if let Some(lock) = &bs.lock_info {
            map.insert("be_lock_pct".into(), json!(lock.lock_percent));
            map.insert("be_lock_tag".into(), json!(lock.lock_tag));
        }
    }

    // ── Birdeye creation info ──
    if let Some(bc) = &enrichment.birdeye_creation {
        map.insert("be_creation_slot".into(), json!(bc.slot));
        map.insert("be_creation_block_time".into(), json!(bc.block_unix_time));
        map.insert("be_creation_tx".into(), json!(bc.tx_hash));
    }

    // ── GoPlus security ──
    if let Some(gp) = &enrichment.goplus {
        map.insert(
            "gp_is_honeypot".into(),
            json!(GoPlusResult::is_flag_set(&gp.is_honeypot)),
        );
        map.insert(
            "gp_is_mintable".into(),
            json!(GoPlusResult::is_flag_set(&gp.is_mintable)),
        );
        map.insert(
            "gp_transfer_pausable".into(),
            json!(GoPlusResult::is_flag_set(&gp.transfer_pausable)),
        );
        map.insert(
            "gp_is_blacklisted".into(),
            json!(GoPlusResult::is_flag_set(&gp.is_blacklisted)),
        );
        map.insert(
            "gp_can_reclaim_ownership".into(),
            json!(GoPlusResult::is_flag_set(&gp.can_take_back_ownership)),
        );
        map.insert(
            "gp_is_proxy".into(),
            json!(GoPlusResult::is_flag_set(&gp.is_proxy)),
        );
        map.insert(
            "gp_is_open_source".into(),
            json!(GoPlusResult::is_flag_set(&gp.is_open_source)),
        );
        map.insert("gp_holder_count".into(), json!(gp.holder_count));
    }

    // ── Smart wallet analysis ──
    if let Some(sw) = &enrichment.smart_wallets {
        map.insert("sw_total_scored".into(), json!(sw.total_scored));
        map.insert("sw_suspicious_count".into(), json!(sw.suspicious_count));
        map.insert("sw_genuine_count".into(), json!(sw.genuine_count));
        map.insert("sw_suspicious_ratio".into(), json!(sw.suspicious_ratio));
        map.insert(
            "sw_max_same_funder_count".into(),
            json!(sw.max_same_funder_count),
        );
        map.insert("sw_avg_tx_count".into(), json!(sw.avg_tx_count));
        map.insert(
            "sw_avg_wallet_age_secs".into(),
            json!(sw.avg_wallet_age_secs),
        );
        map.insert(
            "sw_min_wallet_age_secs".into(),
            json!(sw.min_wallet_age_secs),
        );
    }

    // ── Whale buy metrics (conviction signal) ──
    if let Some(wb) = &enrichment.whale_buys {
        map.insert(
            "whale_max_single_buy_sol".into(),
            json!(wb.max_single_buy_sol),
        );
        map.insert("whale_buy_count".into(), json!(wb.whale_buy_count));
        map.insert(
            "whale_buy_volume_sol".into(),
            json!(wb.whale_buy_volume_sol),
        );
        map.insert("whale_avg_buy_size_sol".into(), json!(wb.avg_buy_size_sol));
        map.insert("whale_total_trades".into(), json!(wb.total_trades));
        map.insert(
            "whale_buy_sell_volume_ratio".into(),
            json!(wb.buy_sell_volume_ratio),
        );
    }

    // ── Cross-source consistency checks ──
    if let (Some(st), Some(bo)) = (&enrichment.solana_tracker, &enrichment.birdeye_overview) {
        if let (Some(st_holders), Some(be_holders)) = (st.holders, bo.holder) {
            map.insert(
                "holder_count_delta_st_be".into(),
                json!((st_holders as i64 - be_holders as i64).abs()),
            );
        }
    }
    if let (Some(st), Some(bs)) = (&enrichment.solana_tracker, &enrichment.birdeye_security) {
        if let (Some(st_top10), Some(be_top10)) = (st.top10_pct, bs.top10_holder_percent) {
            map.insert(
                "top10_pct_delta_st_be".into(),
                json!((st_top10 - be_top10).abs()),
            );
        }
    }

    debug!(field_count = map.len(), "sniper_features JSONB built");

    features
}

/// Compute soft flags from enrichment data using plan thresholds.
pub fn compute_soft_flags(enrichment: &EnrichmentResult) -> SoftFlags {
    let mut flags = SoftFlags::default();

    if let Some(st) = &enrichment.solana_tracker {
        flags.flag_high_bundlers = st.bundlers_pct.map_or(false, |v| v > 20.0);
        flags.flag_low_risk_score = st.risk_score.map_or(false, |v| v < 4.0);
        flags.flag_high_concentration = st.top10_pct.map_or(false, |v| v > 50.0);
        flags.flag_low_holders = st.holders.map_or(false, |v| v < 50);
        flags.flag_dev_holds = st.dev_pct.map_or(false, |v| v > 5.0);
        flags.flag_sniper_heavy = st.snipers_pct.map_or(false, |v| v > 20.0);
        flags.flag_lp_not_burned = st.lp_burn_pct.map_or(false, |v| v < 90.0);
    }

    if let Some(c) = &enrichment.creator_history {
        flags.flag_serial_creator = c.previous_token_count.map_or(false, |v| v > 5);
    }

    if let Some(bc) = &enrichment.pumpfun_curve {
        flags.flag_fast_graduation = bc.sale_duration_mins.map_or(false, |v| v < 10.0);
    }

    if let Some(gp) = &enrichment.goplus {
        flags.flag_honeypot_goplus = GoPlusResult::is_flag_set(&gp.is_honeypot);
        flags.flag_mintable_any_source = GoPlusResult::is_flag_set(&gp.is_mintable);
        flags.flag_transfer_pausable = GoPlusResult::is_flag_set(&gp.transfer_pausable);
        flags.flag_ownership_reclaimable = GoPlusResult::is_flag_set(&gp.can_take_back_ownership);
    }

    if let Some(sw) = &enrichment.smart_wallets {
        flags.flag_suspicious_holders = sw.suspicious_ratio > 0.6;
        flags.flag_wash_trade_ring = sw.max_same_funder_count >= 4;
    }

    // No socials flag
    if let Some(bo) = &enrichment.birdeye_overview {
        if let Some(ext) = &bo.extensions {
            flags.flag_no_socials = ext.twitter.is_none() && ext.telegram.is_none();
        }
    }

    // Birdeye mintable cross-check
    if let Some(bs) = &enrichment.birdeye_security {
        if bs.is_mintable == Some(true) {
            flags.flag_mintable_any_source = true;
        }
    }

    flags
}

use chrono::Datelike;
use chrono::Timelike;
