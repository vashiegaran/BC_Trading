# Bonding Curve Signals

## Status
📊 First-pass analysis complete (2026-04-19). Working hypothesis identified. Needs price/PnL follow-up before any live gate.

## Goal
Find patterns in PumpFun bonding-curve activity that predict graduation to Raydium/pump-AMM. Base graduation rate is 4–6% (we measured 5.79%). Even a 2–3x lift on graduation probability would be a significant edge for early entry.

## Data source

- **Table:** `bonding_curve_signals` (Supabase)
- **Writer:** [src/detection/pumpfun_ws.rs](../../src/detection/pumpfun_ws.rs) — `build_signal_payload()` + `write_bonding_curve_signal()`
- **Fields captured:** mint, creator_wallet, token_age_secs, total_volume_sol, buy_count, sell_count, unique_buyers, initial_buy_sol, trades (jsonb), signals jsonb (whale_buy, whale_buy_count, buy_velocity_30s, zero_sells, creator_rebuy, sell_then_buy_flip, fast_volume, max_single_trade_sol, avg_trade_sol, buy_sell_ratio, total_buy_sol, total_sell_sol)
- **Graduation marker:** `graduated` bool flipped when Raydium pool detected
- **Analysis script:** [scripts/analyze_bc_signals.py](../../scripts/analyze_bc_signals.py)

## Working hypotheses (original, 2026-04-19)

1. Unique-buyer velocity predicts graduation
2. Creator rebuy is a red flag
3. Buy/sell ratio + buyer growth compound
4. There's a time-to-graduate sweet spot
5. First-N-seconds dominance matters

## Findings — 2026-04-19 (n=2,504 mints, 145 graduated, 5.79% base rate)

### Univariate lift (★ = lift ≥ 1.5x, n ≥ 20)

| Feature                    | Bucket     |    n | grad |  rate   |  lift |
| -------------------------- | ---------- | ---: | ---: | ------: | ----: |
| **Buy/sell ratio**         | <1         |  344 |    8 |  2.33%  | 0.40x |
|                            | 1–2        | 1288 |   44 |  3.42%  | 0.59x |
|                            | **2–4**    |  685 |   63 |  9.20%  | **1.59x ★** |
|                            | **4–8**    |  135 |   22 | 16.30%  | **2.82x ★** |
|                            | **8+**     |   53 |    8 | 15.09%  | **2.61x ★** |
| **Creator rebuy=true**     | red flag   |  360 |    9 |  2.50%  | **0.43x** |
| Unique buyers              | 40–80      |  962 |   67 |  6.96%  | 1.20x |
| Buy count                  | <20        |  127 |   10 |  7.87%  | 1.36x |
| Token age at signal        | <30s       | 1333 |   83 |  6.23%  | 1.08x |
|                            | 30–60s     |  516 |   20 |  3.88%  | 0.67x |
| Max buys in 30s window     | 10–20      |  220 |   18 |  8.18%  | 1.41x |

### Combined filters

| Filter stack                                     |    n | grad |   rate | lift |
| ------------------------------------------------ | ---: | ---: | -----: | ---: |
| No rebuy + 20+ unique buyers                     | 1866 |  120 |  6.43% | 1.11x |
| **No rebuy + 40+ buyers + BSR ≥ 2**              |  451 |   60 | **13.30%** | **2.30x ★★** |
| Fast volume + No rebuy                           | 1975 |  126 |  6.38% | 1.10x |
| Whale (1+) + No rebuy + 20+ buyers               |  548 |   41 |  7.48% | 1.29x |
| Volume 40+ + 40+ buyers + No rebuy               | 1026 |   72 |  7.02% | 1.21x |
| First-60s + 20+ buyers + No rebuy                | 1352 |   84 |  6.21% | 1.07x |

### Conclusions

1. **Buy/sell ratio is the dominant univariate signal.** BSR ≥ 2 gives 1.59–2.82x lift across 873 rows. Below 2, graduation is actively suppressed.
2. **Creator rebuy is a hard red flag.** Rebuy=true drops rate to 2.50% (lift 0.43x). 14% of the sample rebuys, all grad at less than half the base rate.
3. **Best single stack:** `no_rebuy AND unique_buyers ≥ 40 AND BSR ≥ 2` → **13.30% grad rate on 451 rows, 2.30x lift.** This filter retains 18% of the population while capturing 41% of graduations (60/145).
4. **Weak/no signal:** whale_buy_count, buy_velocity_30s, fast_volume_flag, zero_sells, max_single_trade_sol, token_age_at_signal — all roughly flat against base rate.
5. **Early graduations share a profile.** 10 earliest graduates: age 1–4s at first signal, volume ~50–70 SOL, unique buyers 9–33, buy/sell ratio ≥ 1.8, velocity = buy count (pure-buy opening), no rebuy.

## Working candidate gate (NOT LIVE)

```
bsr >= 2 AND unique_buyers >= 40 AND creator_rebuy == false
```

Expected: ~13% graduation (vs 5.79% base), ~41% recall. Filter retains 18% of the top-of-funnel.

## Data-collection gaps

- No per-mint price history between signal fire and graduation — can't measure entry timing
- No exit-price distribution for graduates — grad rate ≠ PnL
- Non-graduates in the BSR≥2 bucket (87% of the filter): how fast do they die? Could be false positives that still dump profitably if held short-term.

## Open questions (for next session / separate codebase)

1. **Price/PnL follow-up.** Pull price curves for the 145 graduated mints. Where does the gate-entry point land relative to the ATH? How long do we have to exit?
2. **Time-to-graduation distribution.** Is the signal fired 0–30s after birth actionable, or do we need to wait for confirmation? What fraction of tokens that match the gate graduate within 10 min / 1h / 24h?
3. **False-positive dump study.** For non-graduates that matched the gate, reconstruct price action. If they still pumped 2–5x before dying, the strategy may be viable as a scalp regardless of graduation.
4. **Survivor bias check.** Are we capturing every PumpFun token, or only those that pass an upstream filter? Check how `bonding_curve_signals` is gated before insert.

## Proposed v2 design (for separate codebase)

```
service: bc_research_engine

inputs:
  - subscription to bonding_curve_signals INSERT stream
  - DexScreener price poller for each graduated mint (5min intervals for first 2h, hourly for next 24h)

outputs:
  - bc_gate_rollup table: (gate_id, date, n_matched, n_graduated, median_peak_mult, median_time_to_peak, p90_peak_mult)
  - bc_gate_recommendations table: (gate_id, active, confidence_score, last_reviewed_at)

rotation:
  - recompute gate lift weekly over trailing 14d window
  - promote gate to "active" when lift >= 2.0x AND n >= 100 AND median_peak_mult >= 1.5x
  - demote when lift < 1.2x over 7d
```

The live sniper bot would subscribe to `bc_gate_recommendations` and only open positions on tokens that match an active gate. No gate edits without the engine's sign-off.

## PnL backfill — 2026-04-22

Prior analysis proved the gate predicts **graduation**, not **PnL**. This session adds the missing half:

- **[migrations/017_bc_gate_backtest.sql](../../migrations/017_bc_gate_backtest.sql)** — new `bc_gate_backtest` table. Stores, per mint: gate match flag, graduated flag, price-at-signal, peak_multiplier, time_to_peak, max_drawdown, and realized PnL under 5 TP/SL rules (TP+30/SL-20, TP+50/SL-30, TP+100/SL-30, TP+100/SL-50, TP+200/SL-50).
- **[scripts/backfill_bc_gate_pnl.py](../../scripts/backfill_bc_gate_pnl.py)** — pulls Birdeye 1m candles (fallback 15m) from signal-time → +24h for every mint in `bonding_curve_signals`; computes metrics; simulates TP/SL exits; upserts idempotently.
- **[scripts/analyze_bc_gate_pnl.py](../../scripts/analyze_bc_gate_pnl.py)** — summary report. Compares gate/no-gate/graduated slices; ranks TP/SL rules by expectancy; tests 4 sub-gate variants (tighter BSR, tighter buyer count, volume≥40, age<30s).

### Operator runbook

1. Apply `migrations/017_bc_gate_backtest.sql` in Supabase SQL editor.
2. Smoke test: `python scripts/backfill_bc_gate_pnl.py --only-graduated --limit 5`.
3. Full backfill: `python scripts/backfill_bc_gate_pnl.py` (≈2,500 mints × ~0.1s ≈ 5 min, well under Birdeye Starter 15 RPS).
4. Report: `python scripts/analyze_bc_gate_pnl.py`.

### Decision rule the report answers

- If best TP/SL rule has **mean PnL > 0 on n ≥ 50 gate-matched rows**, gate is a viable long-only edge → proceed to feature-flagged "score boost" in the live bot (separate, reviewed change).
- If all rules yield mean ≤ 0 but peak_multiplier p90 is high, the edge lives in a shorter-hold exit; revisit TP/SL grid with tighter TPs (e.g. +10%/+20%) in a follow-up.
- If peak_multiplier median is ≈ 1.0, entry-at-signal is too early — graduation-prediction is real but not tradable at this timing. Look at later entry triggers (e.g. wait for pullback-and-reclaim).

## Change log

- **2026-04-19** — first-pass analysis, gate candidate identified.
- **2026-04-22** — PnL backfill infrastructure shipped (migration 017 + backfill + analyze scripts). Not yet run.
