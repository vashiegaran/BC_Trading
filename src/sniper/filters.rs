//! Hard filters — 7 proven killers. Everything else is logged, not blocked.
//!
//! v5 (data-driven loosening from Supabase audit, 69 positions + 275 candidates):
//!   - Rejected $WW3 (33.9x) at bundlers=81.4%, ASSDAQ (8.6x)/EPHYRA (7.1x)/CLI (3.1x)
//!     all in bundlers 60-80% zone with safety<60.
//!   - Rejected human (162.5x) at top10=84.9% with safety<60 despite liq=85/dev=0/LP burned.
//!   - Rejected Stimmy (26.3x), $SENDSOL (4.8x), MSBR (3.7x) at liquidity <20 SOL.
//!   - Saw bundlers_pct values of 118%, 166%, 173% — data bug, must sanitize.
//!
//! v5 changes:
//!   3. bundlers > 90% hard (was 80); 60-90% soft zone requires safety >= 50 (was 60)
//!   4. liquidity < 5 SOL hard (was 10); 5-20 soft zone requires safety >= 50
//!   6. top-10 > 95% hard (was 90); 80-95% soft zone requires safety >= 50 (was 60)
//!   + Sanitize bundlers_pct > 100 (data bug) — treat as missing
//!
//! 1. mint_authority not revoked → BLOCK
//! 2. freeze_authority not revoked → BLOCK
//! 3. bundlers > 90% hard reject, 60-90% soft zone (score-based)
//! 4. liquidity < 5 SOL → BLOCK; 5-20 SOL soft zone (score-based)
//! 5. GoPlus honeypot → BLOCK
//! 6. Top-10 holder concentration > 95% hard, 80-95% soft zone (score-based)
//! 7. GoPlus critical flags (mintable, transfer_pausable, blacklist, reclaim) → BLOCK

use tracing::{info, warn};

use super::types::*;

/// Apply the 5 hard filters from SNIPER_PLAN Phase 1.
/// Returns HardFilterResult with pass/fail and rejection reason.
pub fn apply_hard_filters(
    enrichment: &EnrichmentResult,
    initial_liquidity_sol: f64,
) -> HardFilterResult {
    // ── Filter 1: mint_authority must be revoked ──
    // Cross-source: on-chain + Birdeye + Solana Tracker
    match check_mint_authority_active(enrichment) {
        Some(true) => {
            warn!("HARD FILTER: mint_authority not revoked");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("mint_authority_not_revoked".to_string()),
                filter_name: Some("mint_authority".to_string()),
            };
        }
        None => {
            warn!("HARD FILTER: no data source available for mint_authority check");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("mint_authority_no_data".to_string()),
                filter_name: Some("mint_authority".to_string()),
            };
        }
        Some(false) => { /* revoked — pass */ }
    }

    // ── Filter 2: freeze_authority must be revoked ──
    // Cross-source: on-chain + Birdeye + Solana Tracker
    match check_freeze_authority_active(enrichment) {
        Some(true) => {
            warn!("HARD FILTER: freeze_authority not revoked");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("freeze_authority_not_revoked".to_string()),
                filter_name: Some("freeze_authority".to_string()),
            };
        }
        None => {
            warn!("HARD FILTER: no data source available for freeze_authority check");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("freeze_authority_no_data".to_string()),
                filter_name: Some("freeze_authority".to_string()),
            };
        }
        Some(false) => { /* revoked — pass */ }
    }

    // ── Filter 3: bundlers soft zone (v5) ──
    // v5: hard reject > 90% (was 80). Soft zone 60-90% requires safety ≥ 50 (was 60).
    // Data: $WW3 33.9x at 81.4%, ASSDAQ 8.6x at 65.2%, EPHYRA 7.1x at 62.8% all rejected
    // by v4 thresholds. Pure rugs typically show bundlers >95%.
    // Sanitize: values > 100 are a data bug (seen: 118%, 166%, 173%) — treat as missing.
    //
    // v10 NOTE: earlier draft added a 30-50% death-zone reject based on recent data,
    // but historical pre-bug data (53 trades, +2.63 SOL) shows 30-50% was 100% WR
    // (+0.298 SOL). The "death zone" was an artifact of the momentum-kill bug. Reverted.
    if let Some(st) = &enrichment.solana_tracker {
        if let Some(bundlers_pct) = st.bundlers_pct {
            if bundlers_pct > 100.0 {
                warn!(
                    bundlers_pct = bundlers_pct,
                    "bundlers_pct > 100% — data bug, skipping filter"
                );
            } else if bundlers_pct > 90.0 {
                warn!(bundlers_pct = bundlers_pct, "HARD FILTER: bundlers > 90%");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("bundlers_pct={:.1}% > 90%", bundlers_pct)),
                    filter_name: Some("bundlers".to_string()),
                };
            } else if bundlers_pct > 60.0 {
                let safety = compute_concentration_safety(enrichment, initial_liquidity_sol);
                if safety < 50.0 {
                    warn!(
                        bundlers_pct = bundlers_pct,
                        safety_score = format!("{:.0}", safety),
                        "HARD FILTER: bundlers {:.1}% in soft zone but safety score {:.0} < 50",
                        bundlers_pct, safety
                    );
                    return HardFilterResult {
                        passed: false,
                        rejection_reason: Some(format!(
                            "bundlers_pct={:.1}% > 60% (safety={:.0} < 50)",
                            bundlers_pct, safety
                        )),
                        filter_name: Some("bundlers".to_string()),
                    };
                }
                info!(
                    bundlers_pct = bundlers_pct,
                    safety_score = format!("{:.0}", safety),
                    "✅ Bundlers {:.1}% in soft zone — safety score {:.0} ≥ 50, passing",
                    bundlers_pct, safety
                );
            }
        }
    }

    // ── Filter 4: initial liquidity (v7) ──
    // v7: hard reject < 5 SOL (was 10). 5-20 SOL soft zone requires safety ≥ 50.
    // Data (146 rejected tokens): 0-2 SOL = 0% moon rate (pure junk).
    // 2-5 SOL = 28.6% moon rate. 5-10 SOL = 42.9% moon rate (incl. 26.3x Stimmy).
    // Lowered floor from 10 → 5 to capture the promising 5-10 SOL range.
    if initial_liquidity_sol < 5.0 {
        warn!(
            liquidity_sol = initial_liquidity_sol,
            "HARD FILTER: liquidity < 5 SOL"
        );
        return HardFilterResult {
            passed: false,
            rejection_reason: Some(format!(
                "initial_liquidity={:.1} SOL < 5 SOL",
                initial_liquidity_sol
            )),
            filter_name: Some("liquidity".to_string()),
        };
    }
    if initial_liquidity_sol < 20.0 {
        let safety = compute_concentration_safety(enrichment, initial_liquidity_sol);
        if safety < 50.0 {
            warn!(
                liquidity_sol = initial_liquidity_sol,
                safety_score = format!("{:.0}", safety),
                "HARD FILTER: liquidity {:.1} SOL in soft zone but safety {:.0} < 50",
                initial_liquidity_sol, safety
            );
            return HardFilterResult {
                passed: false,
                rejection_reason: Some(format!(
                    "initial_liquidity={:.1} SOL in 10-20 soft zone (safety={:.0} < 50)",
                    initial_liquidity_sol, safety
                )),
                filter_name: Some("liquidity".to_string()),
            };
        }
        info!(
            liquidity_sol = initial_liquidity_sol,
            safety_score = format!("{:.0}", safety),
            "✅ Liquidity {:.1} SOL in soft zone — safety {:.0} ≥ 50, passing",
            initial_liquidity_sol, safety
        );
    }

    // ── Filter 5: GoPlus honeypot ──
    if let Some(gp) = &enrichment.goplus {
        if GoPlusResult::is_flag_set(&gp.is_honeypot) {
            warn!("HARD FILTER: GoPlus detected honeypot");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_honeypot_detected".to_string()),
                filter_name: Some("honeypot".to_string()),
            };
        }
    }

    // ── Filter 6: Top-10 holder concentration soft zone (v5) ──
    // v5: hard reject > 95% (was 90). Soft zone 80-95% requires safety ≥ 50 (was 60).
    // Data: "human" token hit 162.5x at 84.9% top10 with safety<60 — we missed it.
    // Pure rugs are almost always >95% (single wallet holds everything).
    {
        let st_top10 = enrichment.solana_tracker.as_ref()
            .and_then(|st| st.top10_pct);
        let be_top10 = enrichment.birdeye_security.as_ref()
            .and_then(|bs| bs.top10_holder_percent);

        let top10_pct = match (st_top10, be_top10) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        if let Some(pct) = top10_pct {
            if pct > 95.0 {
                warn!(top10_pct = pct, "HARD FILTER: top-10 holders > 95%");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("top10_holders={:.1}% > 95%", pct)),
                    filter_name: Some("top10_holders".to_string()),
                };
            }
            if pct > 80.0 {
                let safety = compute_concentration_safety(enrichment, initial_liquidity_sol);
                if safety < 50.0 {
                    warn!(
                        top10_pct = pct,
                        safety_score = format!("{:.0}", safety),
                        "HARD FILTER: top-10 {:.1}% in soft zone but safety score {:.0} < 50",
                        pct, safety
                    );
                    return HardFilterResult {
                        passed: false,
                        rejection_reason: Some(format!(
                            "top10_holders={:.1}% > 80% (safety={:.0} < 50)",
                            pct, safety
                        )),
                        filter_name: Some("top10_holders".to_string()),
                    };
                }
                info!(
                    top10_pct = pct,
                    safety_score = format!("{:.0}", safety),
                    "✅ Top-10 {:.1}% in soft zone — safety score {:.0} ≥ 50, passing",
                    pct, safety
                );
            }
        }
    }

    // ── Filter 7: GoPlus critical flags (mintable, pausable, blacklist, reclaim) ──
    if let Some(gp) = &enrichment.goplus {
        if GoPlusResult::is_flag_set(&gp.is_mintable) {
            warn!("HARD FILTER: GoPlus detected mintable token");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_mintable".to_string()),
                filter_name: Some("goplus_mintable".to_string()),
            };
        }
        if GoPlusResult::is_flag_set(&gp.transfer_pausable) {
            warn!("HARD FILTER: GoPlus detected transfer_pausable");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_transfer_pausable".to_string()),
                filter_name: Some("goplus_transfer_pausable".to_string()),
            };
        }
        if GoPlusResult::is_flag_set(&gp.is_blacklisted) {
            warn!("HARD FILTER: GoPlus detected blacklist functionality");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_blacklisted".to_string()),
                filter_name: Some("goplus_blacklisted".to_string()),
            };
        }
        if GoPlusResult::is_flag_set(&gp.can_take_back_ownership) {
            warn!("HARD FILTER: GoPlus detected reclaimable ownership");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_reclaim_ownership".to_string()),
                filter_name: Some("goplus_reclaim_ownership".to_string()),
            };
        }
    }

    // REMOVED (2026-04-20): ST risk_score filter. Was blocking 21% of candidates
    // including tokens that peaked at 2.2x and 8.5x. Sample size too small (6 trades)
    // to justify hard-blocking. Risk score still logged in sniper_features for analysis.

    // ── Filter 8: dev holding % (v9, data-driven) ──
    // Data: dev_pct >= 48% produced ONLY losers (-0.048 to -0.049 SOL each).
    // Winners median dev_pct = 2.36%, losers median = 4.74%.
    // Hard reject at 50% — clear rug indicator.
    {
        let dev_pct = enrichment.solana_tracker.as_ref()
            .and_then(|st| st.dev_pct);
        if let Some(pct) = dev_pct {
            if pct > 50.0 {
                warn!(dev_pct = pct, "HARD FILTER: dev holding > 50%");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("dev_hold_pct={:.1}% > 50%", pct)),
                    filter_name: Some("dev_holding".to_string()),
                };
            }
        }
    }

    // ── Filter 9: minimum holders (v9, data-driven) ──
    // Data: 0-10 holders = serial losers. Winners median 143 holders.
    // Hard reject below 25 — tokens with <25 holders have no real community.
    {
        let st_holders = enrichment.solana_tracker.as_ref()
            .and_then(|st| st.holders);
        let be_holders = enrichment.goplus.as_ref()
            .and_then(|gp| gp.holder_count);
        let holders = match (st_holders, be_holders) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(h) = holders {
            if h < 25 {
                warn!(holders = h, "HARD FILTER: holder count < 25");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("holders={} < 25", h)),
                    filter_name: Some("min_holders".to_string()),
                };
            }
        }
    }

    info!("All hard filters passed (v10 thresholds)");
    HardFilterResult {
        passed: true,
        rejection_reason: None,
        filter_name: None,
    }
}

/// Fast-track filters: minimal safety checks for BC-validated tokens.
/// Only checks filters 1 (mint_auth), 2 (freeze_auth), 5 (honeypot), 7 (GoPlus critical).
/// Skips bundlers, liquidity, top10, dev holding, holders — those run in deferred verification.
pub fn apply_fast_track_filters(enrichment: &EnrichmentResult) -> HardFilterResult {
    // ── Filter 1: mint_authority must be revoked ──
    match check_mint_authority_active(enrichment) {
        Some(true) => {
            warn!("FAST-TRACK FILTER: mint_authority not revoked");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("mint_authority_not_revoked".to_string()),
                filter_name: Some("ft_mint_authority".to_string()),
            };
        }
        None => {
            warn!("FAST-TRACK FILTER: no data for mint_authority check");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("mint_authority_no_data".to_string()),
                filter_name: Some("ft_mint_authority".to_string()),
            };
        }
        Some(false) => { /* revoked — pass */ }
    }

    // ── Filter 2: freeze_authority must be revoked ──
    match check_freeze_authority_active(enrichment) {
        Some(true) => {
            warn!("FAST-TRACK FILTER: freeze_authority not revoked");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("freeze_authority_not_revoked".to_string()),
                filter_name: Some("ft_freeze_authority".to_string()),
            };
        }
        None => {
            warn!("FAST-TRACK FILTER: no data for freeze_authority check");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("freeze_authority_no_data".to_string()),
                filter_name: Some("ft_freeze_authority".to_string()),
            };
        }
        Some(false) => { /* revoked — pass */ }
    }

    // ── Filter 5: GoPlus honeypot ──
    if let Some(gp) = &enrichment.goplus {
        if GoPlusResult::is_flag_set(&gp.is_honeypot) {
            warn!("FAST-TRACK FILTER: GoPlus detected honeypot");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_honeypot_detected".to_string()),
                filter_name: Some("ft_honeypot".to_string()),
            };
        }

        // ── Filter 7: GoPlus critical flags ──
        if GoPlusResult::is_flag_set(&gp.is_mintable) {
            warn!("FAST-TRACK FILTER: GoPlus detected mintable token");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_mintable".to_string()),
                filter_name: Some("ft_goplus_mintable".to_string()),
            };
        }
        if GoPlusResult::is_flag_set(&gp.transfer_pausable) {
            warn!("FAST-TRACK FILTER: GoPlus detected transfer_pausable");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_transfer_pausable".to_string()),
                filter_name: Some("ft_goplus_transfer_pausable".to_string()),
            };
        }
        if GoPlusResult::is_flag_set(&gp.is_blacklisted) {
            warn!("FAST-TRACK FILTER: GoPlus detected blacklist functionality");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_blacklisted".to_string()),
                filter_name: Some("ft_goplus_blacklisted".to_string()),
            };
        }
        if GoPlusResult::is_flag_set(&gp.can_take_back_ownership) {
            warn!("FAST-TRACK FILTER: GoPlus detected reclaimable ownership");
            return HardFilterResult {
                passed: false,
                rejection_reason: Some("goplus_reclaim_ownership".to_string()),
                filter_name: Some("ft_goplus_reclaim_ownership".to_string()),
            };
        }
    }

    info!("All fast-track filters passed");
    HardFilterResult {
        passed: true,
        rejection_reason: None,
        filter_name: None,
    }
}

/// Apply deferred verification filters post-buy for fast-track tokens.
/// Checks the filters that were skipped during fast-track entry:
/// bundlers, liquidity, top10, dev holding, holders.
/// Returns HardFilterResult — if failed, caller should trigger emergency exit.
pub fn apply_deferred_filters(
    enrichment: &EnrichmentResult,
    initial_liquidity_sol: f64,
) -> HardFilterResult {
    // ── Filter 3: bundlers > 90% ──
    if let Some(st) = &enrichment.solana_tracker {
        if let Some(bundlers_pct) = st.bundlers_pct {
            if bundlers_pct <= 100.0 && bundlers_pct > 90.0 {
                warn!(bundlers_pct = bundlers_pct, "DEFERRED FILTER: bundlers > 90%");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("deferred_bundlers_pct={:.1}% > 90%", bundlers_pct)),
                    filter_name: Some("deferred_bundlers".to_string()),
                };
            }
        }
    }

    // ── Filter 6: top10 > 95% ──
    {
        let st_top10 = enrichment.solana_tracker.as_ref().and_then(|st| st.top10_pct);
        let be_top10 = enrichment.birdeye_security.as_ref().and_then(|bs| bs.top10_holder_percent);
        let top10_pct = match (st_top10, be_top10) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(pct) = top10_pct {
            if pct > 95.0 {
                warn!(top10_pct = pct, "DEFERRED FILTER: top-10 holders > 95%");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("deferred_top10={:.1}% > 95%", pct)),
                    filter_name: Some("deferred_top10".to_string()),
                };
            }
        }
    }

    // ── Filter 8: dev holding > 50% ──
    if let Some(st) = &enrichment.solana_tracker {
        if let Some(pct) = st.dev_pct {
            if pct > 50.0 {
                warn!(dev_pct = pct, "DEFERRED FILTER: dev holding > 50%");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("deferred_dev_pct={:.1}% > 50%", pct)),
                    filter_name: Some("deferred_dev_holding".to_string()),
                };
            }
        }
    }

    // ── Filter 9: holders < 25 ──
    {
        let st_holders = enrichment.solana_tracker.as_ref().and_then(|st| st.holders);
        let gp_holders = enrichment.goplus.as_ref().and_then(|gp| gp.holder_count);
        let holders = match (st_holders, gp_holders) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(h) = holders {
            if h < 25 {
                warn!(holders = h, "DEFERRED FILTER: holder count < 25");
                return HardFilterResult {
                    passed: false,
                    rejection_reason: Some(format!("deferred_holders={} < 25", h)),
                    filter_name: Some("deferred_min_holders".to_string()),
                };
            }
        }
    }

    info!("All deferred filters passed — fast-track position validated");
    HardFilterResult {
        passed: true,
        rejection_reason: None,
        filter_name: None,
    }
}

/// Cross-source mint authority check.
/// If ANY source reports active mint authority, return true (= blocked).
/// Returns None if no source had data (caller must handle).
fn check_mint_authority_active(enrichment: &EnrichmentResult) -> Option<bool> {
    let mut any_source = false;

    // Source 1: On-chain raw bytes (most reliable)
    if let Some(m) = &enrichment.on_chain_mint {
        any_source = true;
        if !m.mint_authority_revoked {
            return Some(true);
        }
    }

    // Source 2: Birdeye security
    if let Some(bs) = &enrichment.birdeye_security {
        if let Some(is_mintable) = bs.is_mintable {
            any_source = true;
            if is_mintable {
                return Some(true);
            }
        }
    }

    // Source 3: Solana Tracker
    if let Some(st) = &enrichment.solana_tracker {
        if let Some(has_mint) = st.has_mint_authority {
            any_source = true;
            if has_mint {
                return Some(true);
            }
        }
    }

    if any_source { Some(false) } else { None }
}

/// Cross-source freeze authority check.
/// If ANY source reports active freeze authority, return true (= blocked).
/// Returns None if no source had data (caller must handle).
fn check_freeze_authority_active(enrichment: &EnrichmentResult) -> Option<bool> {
    let mut any_source = false;

    // Source 1: On-chain raw bytes (most reliable)
    if let Some(m) = &enrichment.on_chain_mint {
        any_source = true;
        if !m.freeze_authority_revoked {
            return Some(true);
        }
    }

    // Source 2: Solana Tracker
    if let Some(st) = &enrichment.solana_tracker {
        if let Some(has_freeze) = st.has_freeze_authority {
            any_source = true;
            if has_freeze {
                return Some(true);
            }
        }
    }

    if any_source { Some(false) } else { None }
}

/// Compute a safety score (0-100) for tokens in the concentration soft zone.
///
/// Used when top10_holders is 80-90% or bundlers_pct is 60-80%.
/// Checks compensating signals that indicate the token is organic despite
/// high concentration. All data comes from existing enrichment — no extra calls.
///
/// Score >= 60 → pass the soft zone. Score < 60 → reject.
///
/// Calibrated from overnight data (Apr 16-17):
///   "human" (162.5x, top10=84.9%): liq=85, dev=0%, bundlers=2.3%, LP burned,
///   54 buyers, mint+freeze revoked → would score ~95 → PASS
///   Scam tokens: dev>5%, LP not burned, <10 buyers → score ~20 → REJECT
fn compute_concentration_safety(
    enrichment: &EnrichmentResult,
    initial_liquidity_sol: f64,
) -> f64 {
    let mut score: f64 = 50.0;

    // ── Liquidity depth (0 to +15): high liq = hard to rug ──
    if initial_liquidity_sol >= 80.0 {
        score += 15.0;
    } else if initial_liquidity_sol >= 50.0 {
        score += 10.0;
    } else if initial_liquidity_sol >= 30.0 {
        score += 5.0;
    } else {
        score -= 10.0; // low liq + high concentration = very dangerous
    }

    if let Some(st) = &enrichment.solana_tracker {
        // ── Dev holding (0 to -20): dev with tokens can dump ──
        if let Some(dev_pct) = st.dev_pct {
            if dev_pct < 0.1 {
                score += 10.0; // dev holds nothing — bullish
            } else if dev_pct > 5.0 {
                score -= 20.0; // dev can dump hard
            } else if dev_pct > 2.0 {
                score -= 10.0;
            }
        }

        // ── Bundlers (0 to -10): cross-check with the bundler filter itself ──
        if let Some(bundlers_pct) = st.bundlers_pct {
            if bundlers_pct < 10.0 {
                score += 8.0; // very low bundlers despite high concentration = organic whales
            } else if bundlers_pct > 50.0 {
                score -= 10.0;
            }
        }

        // ── LP burn (0 to +10): burned LP = can't pull liquidity ──
        if let Some(lp) = st.lp_burn_pct {
            if lp >= 99.0 {
                score += 10.0;
            }
        }

        // ── Holder count from ST ──
        if let Some(holders) = st.holders {
            if holders >= 50 {
                score += 5.0;
            } else if holders < 10 {
                score -= 5.0;
            }
        }

        // ── Risk score ──
        if let Some(risk) = st.risk_score {
            if risk <= 5.0 {
                score += 5.0;
            } else if risk >= 50.0 {
                score -= 10.0;
            }
        }
    }

    // ── Mint/freeze authority (from on-chain) ──
    if let Some(m) = &enrichment.on_chain_mint {
        if m.mint_authority_revoked && m.freeze_authority_revoked {
            score += 5.0;
        }
    }

    // ── Smart wallet metrics ──
    if let Some(sw) = &enrichment.smart_wallets {
        if sw.suspicious_ratio > 0.3 {
            score -= 10.0;
        } else if sw.genuine_count >= 5 {
            score += 5.0;
        }
    }

    // ── Whale buy metrics ──
    if let Some(wb) = &enrichment.whale_buys {
        if wb.whale_buy_count >= 5 && wb.buy_sell_volume_ratio > 2.0 {
            score += 5.0; // smart money loading
        }
    }

    score.clamp(0.0, 100.0)
}

// ── Bonding curve pattern filter ─────────────────────────────────────────
// Pre-enrichment gate: rejects tokens based on bonding curve trading patterns.
// Data-driven from 1,249 BC signals analysis (2026-04-20):
//   - creator_rebuy graduates at 1.1% vs 6.0% without (5x worse)
//   - buy_sell_ratio Q4 (>2.3) graduates at 10.9% vs Q1 (<1.1) at 3.2%
//
// REMOVED (2026-04-20): bc_sell_count check. Threshold of 40 was calibrated
// on 50-SOL-signal-time data (median 20 sells), but filter runs at graduation
// time when median is 208 sells. Blocked 100% of BC-observed graduates,
// including PDM (22.7x), MLG (14.2x), AB (7.67x). buy_sell_ratio already
// captures sell pressure as a normalized ratio.

/// Check bonding curve trading patterns. Returns `None` if the token passes,
/// or `Some(reason)` if it should be rejected.
pub fn check_bc_pattern(
    token: &crate::detection::types::GraduatedToken,
    filters_cfg: &crate::config::FiltersConfig,
    bc_fast_track_score: Option<f64>,
) -> Option<String> {
    // 1. Creator rebuy — dev buying back is a manipulation signal, not bullish
    if filters_cfg.reject_creator_rebuy && token.creator_rebuy {
        return Some("creator_rebuy_detected".to_string());
    }

    // 2. Buy/sell ratio too low — weak buying pressure on the curve
    // Skip check for tokens from non-PumpFun sources with no data (ratio = 0).
    // If enabled, let strong BC fast-track candidates bypass this early reject
    // and continue into minimal enrichment.
    let fast_track_bypass = filters_cfg.allow_fast_track_buy_sell_ratio_bypass
        && bc_fast_track_score
            .map(|score| score >= filters_cfg.bc_fast_track_min_score)
            .unwrap_or(false);

    if token.buy_sell_ratio > 0.0
        && token.buy_sell_ratio < filters_cfg.min_buy_sell_ratio
        && !fast_track_bypass
    {
        return Some(format!(
            "buy_sell_ratio={:.2} < {:.1}",
            token.buy_sell_ratio, filters_cfg.min_buy_sell_ratio
        ));
    }

    None
}
