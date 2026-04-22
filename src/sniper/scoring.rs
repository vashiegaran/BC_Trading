//! Phase 3 scoring — data-calibrated weights from 88-position analysis.
//!
//! Still non-blocking (informational only). Score is logged to sniper_candidates
//! but does NOT gate entry decisions. Will be used for gating after 200+ candidates.

use super::types::SniperScore;

/// Compute a sniper score from enrichment features.
/// Phase 3: weights calibrated from real trade data (88 positions).
pub fn compute_sniper_score(features: &serde_json::Value) -> SniperScore {
    let mut score: f64 = 50.0;
    let mut components = serde_json::json!({});
    let map = components.as_object_mut().unwrap();

    // ── Data-calibrated weights ──

    // Bundlers: strong negative signal (proven: DOCATI -28.3%)
    // Penalty scales 0-15 points for 0-30%, hard-reject above 40% is handled by hard filters
    if let Some(bundlers) = features.get("st_bundlers_pct").and_then(|v| v.as_f64()) {
        let penalty = (bundlers / 2.0).min(15.0);
        score -= penalty;
        map.insert("bundler_penalty".into(), serde_json::json!(-penalty));
    }

    // Holder count: strong positive (more holders = more organic demand)
    if let Some(holders) = features.get("st_holders").and_then(|v| v.as_u64()) {
        let bonus = if holders >= 200 {
            10.0
        } else if holders >= 100 {
            7.0
        } else if holders >= 50 {
            4.0
        } else {
            (holders as f64 / 50.0) * 4.0
        };
        score += bonus;
        map.insert("holder_bonus".into(), serde_json::json!(bonus));
    }

    // Fast graduation is risky (MemeTrans: #1 feature — tokens graduating in <10 min
    // tend to be pump-and-dumps)
    if let Some(sale_mins) = features.get("sale_duration_mins").and_then(|v| v.as_f64()) {
        if sale_mins < 5.0 {
            score -= 8.0;
            map.insert("fast_grad_penalty".into(), serde_json::json!(-8.0));
        } else if sale_mins < 10.0 {
            score -= 4.0;
            map.insert("fast_grad_penalty".into(), serde_json::json!(-4.0));
        } else if sale_mins > 60.0 {
            score += 3.0;
            map.insert("slow_grad_bonus".into(), serde_json::json!(3.0));
        }
    }

    // Social presence: mild positive (twitter = community effort)
    if features.get("be_has_twitter") == Some(&serde_json::json!(true)) {
        score += 3.0;
        map.insert("twitter_bonus".into(), serde_json::json!(3.0));
    }

    // Website: additional positive (real project effort)
    if features.get("be_has_website") == Some(&serde_json::json!(true)) {
        score += 2.0;
        map.insert("website_bonus".into(), serde_json::json!(2.0));
    }

    // Serial creator: strong negative (repeat deployers rarely produce winners)
    if features.get("creator_is_serial") == Some(&serde_json::json!(true)) {
        score -= 8.0;
        map.insert("serial_creator_penalty".into(), serde_json::json!(-8.0));
    }

    // Liquidity depth: higher liq = lower rug risk, better exits
    if let Some(liq) = features.get("initial_liquidity_sol").and_then(|v| v.as_f64()) {
        let bonus = if liq >= 80.0 {
            5.0
        } else if liq >= 50.0 {
            2.0
        } else {
            -3.0 // low-liq penalty
        };
        score += bonus;
        map.insert("liquidity_bonus".into(), serde_json::json!(bonus));
    }

    // Top-10 holder concentration: high = dump risk
    if let Some(top10) = features.get("st_top10_pct").and_then(|v| v.as_f64()) {
        if top10 > 50.0 {
            let penalty = ((top10 - 50.0) / 5.0).min(8.0);
            score -= penalty;
            map.insert("top10_penalty".into(), serde_json::json!(-penalty));
        }
    }

    // ── Advanced tier: Volume momentum ──
    // 5-minute volume > $500 = active interest; acceleration vs 15m = buildup
    if let Some(v5m) = features.get("st_volume_5m").and_then(|v| v.as_f64()) {
        if v5m >= 1000.0 {
            score += 5.0;
            map.insert("volume_5m_bonus".into(), serde_json::json!(5.0));
        } else if v5m >= 500.0 {
            score += 2.0;
            map.insert("volume_5m_bonus".into(), serde_json::json!(2.0));
        }
    }

    // Volume acceleration: 5m normalized rate > 15m normalized rate = momentum building
    if let Some(accel) = features.get("volume_accel_5m_vs_15m").and_then(|v| v.as_f64()) {
        if accel > 1.5 {
            score += 4.0;
            map.insert("volume_accel_bonus".into(), serde_json::json!(4.0));
        } else if accel > 1.0 {
            score += 2.0;
            map.insert("volume_accel_bonus".into(), serde_json::json!(2.0));
        } else if accel < 0.5 {
            score -= 3.0;
            map.insert("volume_decel_penalty".into(), serde_json::json!(-3.0));
        }
    }

    // Price momentum: positive 5m change = buying pressure
    if let Some(pc5m) = features.get("st_price_change_5m").and_then(|v| v.as_f64()) {
        if pc5m > 10.0 {
            score += 3.0;
            map.insert("price_momentum_bonus".into(), serde_json::json!(3.0));
        } else if pc5m < -15.0 {
            score -= 4.0;
            map.insert("price_momentum_penalty".into(), serde_json::json!(-4.0));
        }
    }

    // ── Advanced tier: Smart money signal (Jito tips) ──
    // High tips-to-trading ratio = sophisticated traders using priority fees
    if let Some(tips_ratio) = features.get("tips_to_trading_ratio").and_then(|v| v.as_f64()) {
        if tips_ratio > 0.3 {
            score += 4.0;
            map.insert("jito_tips_bonus".into(), serde_json::json!(4.0));
        } else if tips_ratio > 0.1 {
            score += 2.0;
            map.insert("jito_tips_bonus".into(), serde_json::json!(2.0));
        }
    }

    // ── Detection latency: strongest predictor in Phase 3 data ──
    // <120s = 83% win rate, +109% avg PnL. <300s = 68% win rate.
    if let Some(latency_ms) = features.get("detection_latency_ms").and_then(|v| v.as_i64()) {
        let latency_secs = latency_ms as f64 / 1000.0;
        let bonus = if latency_secs < 120.0 {
            10.0
        } else if latency_secs < 300.0 {
            5.0
        } else {
            0.0
        };
        if bonus > 0.0 {
            score += bonus;
            map.insert("detection_speed_bonus".into(), serde_json::json!(bonus));
        }
    }

    // Clamp to 0-100
    score = score.clamp(0.0, 100.0);

    SniperScore {
        score,
        components,
        hard_reject: false,
    }
}
