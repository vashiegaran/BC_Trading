# Strategy Versions

Each version is stamped into every `positions` and `moonbag_positions` row via the
`strategy_version` column. Set the active version in `config.toml`:

```toml
strategy_version = "v14.1-fasttrack-only"
```

---

## v18.7.1 — Early-Buyer Rebuy Shadow (2026-05-07)

**Live strategy impact**: none. Active live behavior remains `v18.7-creator-rebuy-canary` unless [config.toml](config.toml) is changed separately.

### Why
`creator_rebuy` only checks whether the creator/dev wallet bought again. That can be too narrow. This shadow experiment tests a broader OR-candidate signal: whether any of the first five non-creator buyers returns and buys again before graduation.

### Changes
- Adds `early_buyer_rebuy_shadow_enabled` and first-N rebuy thresholds under `[detection]` in [config.toml](config.toml).
- Adds a separate Supabase table via [migrations/029_early_buyer_rebuy_shadow.sql](migrations/029_early_buyer_rebuy_shadow.sql).
- Computes the signal locally from the existing pump.fun/gRPC trade stream, so it adds no Helius, DAS, Solana RPC, or SolanaTracker calls.
- Records one shadow row per mint when any configured early buyer rebuys, including rebuy size, rebuyer count, early-seller count, creator-rebuy state, curve metrics, graduation state, and post-graduation price outcomes.
- Does not forward to live execution and does not change Standard/Fast-Track gates.

### Initial shadow rule
- First `5` unique non-creator buyers.
- Pass if at least `1` of those wallets buys again.
- Count any non-zero rebuy size; quality fields are logged for later filtering.

---

## v18.7 — Creator-Rebuy Canary (2026-05-07)

**strategy_version**: `v18.7-creator-rebuy-canary`

### Why
Live flow after the gRPC cutover showed the bot was healthy but over-gated: Standard lane stayed disabled by design, while many current graduations were creator-rebuy and therefore blocked before live execution. Shadow data showed some creator-rebuy winners, but the median outcome was still weak, so this is a tiny canary rather than a broad allowlist.

### Changes
- Keeps `reject_creator_rebuy = true` in [config.toml](config.toml).
- Adds a separate creator-rebuy live-test gate in [src/config.rs](src/config.rs) and [src/sniper/mod.rs](src/sniper/mod.rs): Fast-Track safety must pass, then the token must meet stricter BC profile thresholds for score, buy pressure, buy/sell ratio, unique buyers, sell count, and liquidity.
- Keeps creator-rebuy shadow logging for non-qualifying names.
- Adds lane-specific tiny sizing: `creator_rebuy_live_test_buy_amount_sol = 0.01` in [config.toml](config.toml).
- Adds an execution-time canary cap of one open creator-rebuy live-test position via [src/execution/state.rs](src/execution/state.rs).

### Rollback
Set `creator_rebuy_live_test_enabled = false` in [config.toml](config.toml) and restart PM2. Shadow tracking remains active.

---

## v18.6 — Data-Tuned Filters + Score Re-fit (2026-05-01)

**strategy_version**: `v18.6-data-tuned`

### Why
Full-corpus rahwn audit (n=282 closed positions, baseline +10.17 SOL, 16.0% ≥3x rate) under the explicit objective "maximize ≥3x hit-rate" revealed two big findings:

1. **Initial liquidity is the strongest pre-buy signal**, but in the *opposite* direction we'd assumed. Logistic regression on 7 BC features had standardized coefficient `liq_sol = -1.276` — by far the largest, and `-liq_sol` alone has AUC=0.754 against the ≥3x target.
2. **The existing `compute_bc_score` is essentially uncorrelated with ≥3x outcomes** (AUC=0.533). Runners and non-runners both averaged ~82 on the old score. The "more buyers = better" assumption was inverted: runners had *fewer* unique buyers (med 43, avg 41.7) than non-runners (med 43, avg 48.1).

### Changes
- **NEW filter `max_initial_liquidity_sol = 80.0`** in [config.toml](config.toml). Rejects pools with > 80 SOL on the SOL side at detection. Implemented in [src/filters/liquidity.rs](src/filters/liquidity.rs) as an early-return at the top of `LiquidityFilter::check`. Schema field added to [src/config.rs](src/config.rs); default 0.0 = disabled.
- **`compute_bc_score` re-fit** in [src/detection/types.rs](src/detection/types.rs#L59):
  - `unique_buyers` weights INVERTED (rewards low/medium counts).
  - `whale_buy` bumped +10 → +15 (strongest discrete signal).
  - `buy_count` band added (lower = bonus, higher = penalty).
  - `total_volume_sol` and `sell_count` dropped from the score (no signal in the fit; kept in the function signature for callsite compat).
  - `buy_sell_ratio` weights softened (data showed weaker stratification than expected).
- **`bc_fast_track_min_score: 65 → 50`** in [config.toml](config.toml). The new score has tighter distribution; 50 keeps ~76% of BC-eligible tokens on the fast-track and matches the old gate's pass-rate.

### Backtest (rahwn n=282)

| Config | Kept n | Kept pnl | ≥3x rate | WR | Dropped runners |
|---|---|---|---|---|---|
| Baseline (no filter) | 282 | +10.17 | 16.0% | 66.3% | 0 of 45 |
| `liq<=80` (new) | 145 | +5.70 | **26.2%** | 71.7% | 7 of 45 |
| v18.x slice baseline | 39 | +0.45 | 7.7% | — | — |
| v18.x slice + `liq<=80` | 27 | **+0.59** | **11.1%** | — | 0 dropped |

### Caveats
- Total kept_pnl drops 10.17 → 5.70 SOL on the full corpus, but most of that lost pnl came from the v12-v14 era under the old unrealistic-fills paper model. On the realistic-fills (v18.x) slice the new config *increases* total pnl (0.45 → 0.59). Live trading runs the realistic-fill model, so the v18.x slice is the truthful one.
- 7 of 45 historical ≥3x runners get filtered out by `liq<=80`. Six of those came from `bc-research-v1` and `v12-fast-track`, both pre-realistic-fills.
- `compute_bc_score` AUC improved 0.533 → 0.592 on the n=152 BC subset. Liquidity itself is a much stronger signal (AUC=0.754) but isn't a `compute_bc_score` input — it's gated separately by the new filter.

### Score formula (v18.6)
```
base 50
- 30 if creator_rebuy
+ 15 if buy_sell_ratio >= 4.0  | + 8 if >= 2.5  | + 3 if >= 1.5  | -15 if < 1.0
+  8 if unique_buyers <= 25     | 0 if <= 40    | -5 if <= 60    | -12 if > 60
+ 15 if whale_buy
+  5 if buy_count <= 30         | -8 if buy_count >= 60
clamp 0..100
```

---

## v18.6-prev — Max Liquidity Cap (REVERTED, 2026-05-01)

**Status:** Investigated and reverted. Code support kept (`max_liquidity_usd` field, default 0 = disabled), but `config.toml` keeps it off.

### Why reverted
Initial 2D `bc_score × liquidity` grid suggested the 80–150 SOL one-side band was dead capital. A full-population cumulative simulation across 283 closed rahwn positions showed every candidate cap is strictly worse than no cap:

| Cap (SOL one-side) | Kept | Total pnl | Dropped pnl |
|---|---|---|---|
| ≤ 70 ($42k) | 97 | +4.89 | **+5.28 lost** |
| ≤ 80 ($48k) | 146 | +5.70 | **+4.46 lost** |
| ≤ 90 ($54k) | 270 | +9.72 | **+0.45 lost** |
| no cap | 283 | **+10.17** | 0 |

The 2D grid was misleading because only ~50% of positions have `bc_score`; the full 80–150 SOL band actually earned +4.5 SOL net.

### Code state
- [src/config.rs](src/config.rs) keeps `max_liquidity_usd: u64` field (default 0 = disabled) for future experiments.
- [src/filters/liquidity.rs](src/filters/liquidity.rs) keeps the no-op cap check (only fires when value > 0).
- [config.toml](config.toml) sets `max_liquidity_usd = 0`. `strategy_version` reverted to `v18.5-bags-watchworthy-shadow`.

---

## v18.5 — Bags Watchworthy Shadow (2026-05-01)

**strategy_version**: `v18.5-bags-watchworthy-shadow`

### Why
The Bags creator monitor is now strong enough to produce a real research lane: detect fresh Bags launches, recover the creator-side funding wallet at birth, score which creators repeatedly attract real early demand, and shadow only the launches from those already-proven creators.

### Code changes
- [src/monitoring/bags.rs](src/monitoring/bags.rs) adds a background Bags monitor that:
  - polls the shared Bags authority for new launch transactions,
  - extracts the creator-side wallet from the launch transaction,
  - stores launch and demand metrics in `bags_launches` / `bags_creator_stats`, and
  - fires a new research-only shadow lane `bags_watchworthy_shadow` when a fresh launch belongs to a creator already marked `watchworthy`.
- [src/config.rs](src/config.rs) and [config.toml](config.toml) add Bags shadow-lane controls for max launch age, entry-price wait, poll cadence, and tracking duration.
- [migrations/027_bags_launch_monitor.sql](migrations/027_bags_launch_monitor.sql) remains the base launch-monitor schema; [migrations/028_bags_shadow_entries.sql](migrations/028_bags_shadow_entries.sql) adds a dedicated table for watchworthy Bags shadow entries and their outcome metrics.
- [scripts/bags_creator_report.py](scripts/bags_creator_report.py) ranks creators by demand rate and sample size, and summarizes recent watchworthy-shadow rows.

### Strategy effect
- No live execution behavior changed.
- The repo can now collect a dedicated observe-only corpus for fresh Bags launches from historically productive creator wallets, separate from Pump.fun lanes.

---

## v18.4 — Shadow Launch-Label Basket (2026-05-01)

**strategy_version**: `v18.4-shadow-launch-label`

### Why
Reverse engineering on `clukz.sol` pointed to an even earlier pattern than the existing label-flow lane: the wallet was buying within seconds of mint when a fresh simple label started repeating across multiple brand-new Pump.fun mints.

### Code changes
- [src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs) adds `launch_label_shadow`, a new observe-only `bc_paper_trades` trigger that fires on the first seconds of trading when a mint belongs to a very recent same-label, multi-creator launch cluster.
- [src/detection/types.rs](src/detection/types.rs) adds a one-shot watchlist flag so the mint-time shadow lane records only once per mint.
- [src/config.rs](src/config.rs) and [config.toml](config.toml) add launch-window thresholds: token age, max BC progress, prior mint count, prior creator count, and label recency.

### Strategy effect
- No live execution behavior changed.
- The bot can now collect outcome data for a mint-time same-label basket proxy, which is the strongest on-chain subset of the `clukz.sol` method that can be shadowed without access to the wallet's private off-chain discovery source.

---

## v18.3 — Shadow Label-Flow + Probe/Add (2026-05-01)

**strategy_version**: `v18.3-shadow-label-probe`

### Why
Wallet research kept pointing to two behaviors that the bot could not measure directly in shadow mode: repeated buying of the same simple label across distinct fresh mints, and staged pressing into the same mint only if the curve kept strengthening after an initial probe.

### Code changes
- [src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs) now keeps a recent normalized-label cache and records two new observe-only `bc_paper_trades` triggers:
  - `label_flow_shadow` — repeated same-label mint cluster plus healthy early flow
  - `probe_add_probe` / `probe_add_add` — would-be staged entry ladder for the same mint
- [src/detection/types.rs](src/detection/types.rs) adds transient watchlist state needed to track recent labels and probe-stage snapshots.
- [src/config.rs](src/config.rs) and [config.toml](config.toml) add shadow-lane toggles and thresholds.

### Strategy effect
- No live execution behavior changed.
- The bot now collects outcome data for both wallet-derived patterns using the existing `bc_paper_trades` shadow pipeline so they can be analyzed before any live rollout.

---

## v14.1 — Fast-Track-Only + Duplicate-Lane Prune (2026-04-27)

**Cargo version**: 0.3.3 | **strategy_version**: `v14.1-fasttrack-only` | **cutover**: `2026-04-27T03:15:00Z` (after restart)

### Why
First 26 hours of v14 data showed two clear, data-driven prunes:

| Lane | n | Total SOL | Avg/fire | Win % | Verdict |
|---|---|---|---|---|---|
| Fast-Track (real) | 26 | +1.66 | +0.0638 | 61.5% | **keep — only profitable lane** |
| Standard (real) | 16 | +0.014 | +0.0009 | 62.5% | **disabled in v14.1** |
| graduation_goplus (paper) | 148 | +1.54 | +0.0104 | 28.4% | **disabled — duplicate of `graduation_raw`** |
| progress_60pct / 75 / 90, graduation_raw | 130–148 each | +1.2 to +1.5 | +0.010 | ~28% | kept as research baseline |

Fast-Track shows **5.5x higher avg/fire and 2x higher win rate** vs the best paper lane, with a positive median (+0.089 SOL) where every paper lane has a negative median (-0.031 SOL).

### Code changes
- **Standard lane disabled** — [src/config.rs](src/config.rs) `standard_lane_enabled: bool` flag (default `true`); [src/sniper/mod.rs](src/sniper/mod.rs) early-rejects with `pipeline_latency` log when off.
- **graduation_goplus paper lane disabled** — [src/config.rs](src/config.rs) `graduation_goplus_enabled: bool` flag (default `true`); [src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs#L984) skips the duplicate fire when off. Saves one Helius+GoPlus call per graduation.
- **[config.toml](config.toml)**: `standard_lane_enabled = false`, `graduation_goplus_enabled = false` with data-justification comments.

### New tooling
- [scripts/v14_lane_comparison.py](scripts/v14_lane_comparison.py) — apples-to-apples ladder simulation across all lanes; clamps `price_1h/bc_price_usd` to `peak_multiplier` to handle late-detection reference-frame mismatch.
- [scripts/v14_weekly_health_check.py](scripts/v14_weekly_health_check.py) — 5-gate readiness check (FT n≥100, win≥55%, avg≥0.04, median>0, paper avg <50% of FT). Run weekly; when all 5 pass, prune `progress_75pct` + `progress_90pct` and keep `progress_60pct` + `graduation_raw` as the two-point baseline.

### Data bug found and contained
During lane comparison, one mint (`POTUS`, `D99rM7fEtxAfqLmC1mHrKTqqMAw9ktkShS5adcSGpump`) showed `price_1h / bc_price_usd = 67.79x` despite `peak_multiplier = 1.05x`. Root cause: late-detection (token detected post-graduation) means `bc_price_usd` and `peak_multiplier` live in different reference frames — not a price-feed corruption. Simulator now clamps so this can’t recur.

### Pruning gate (current)
At v14.1 baseline: 5/5 quality gates **pass**, only Fast-Track sample size is short (n=26, target=100). Re-run health check in ~7 days to evaluate further pruning.

---

## v14 — Multi-Lane BC Research + Data-Driven Moonbag Paths (2026-04-26)

**Cargo version**: 0.3.2 | **strategy_version**: `v14-multi-lane` | **cutover**: `2026-04-26T05:00:29Z` (first `progress_60pct` row)

### BC paper-trade lanes (5 entry triggers)
- `progress_60pct` — fires at BC ≥60%, no API check (earliest baseline)
- `progress_75pct` — fires at BC ≥75%, no API check (was mislabeled `progress_90pct` pre-cutover)
- `progress_90pct` — fires at BC ≥90%, **with GoPlus** (true 90% + safety gate)
- `graduation_raw` — fires at tokenComplete, no API check (latency baseline)
- `graduation_goplus` — fires at tokenComplete, **with GoPlus** (paired with raw)

### New feature columns (migration 022)
- `creator_sold_during_bc` — boolean, did creator dump during BC?
- `buy_pressure_at_entry_pct` — `buy_count / (buy_count + sell_count) * 100`
- `initial_liquidity_sol` — total BC volume at graduation (only on `graduation_*` lanes)

### Moonbag promotion paths (3 new, data-driven)
Fired as **fallback** when OpenAI score < gate AND not fast-runner. Order: C → B → D.

| `promotion_source` | Rule | Sample lift |
|---|---|---|
| `off_hours_low_vol` | `!is_us_hours && 0 < be_volume_24h_usd ≤ 25k` | 5.44x |
| `liquidity_floor` | `0 < be_liquidity_usd ≤ 10k` | 2.34x |
| `bc_score_80` | `bc_score ≥ 80` (fast-track only) | 2.13x |

### Bug fixes carried in
- `bc_price_usd` now stored in USD/token (was SOL/token, off ~150x). Backfilled 3,080 historical rows.

---

## v13 — Lane-B BC Trigger + 24h Shadow Log (2026-04-25)

**Cargo version**: 0.3.1 | **strategy_version**: `v12-fast-track` (unchanged — paper-only data collection, no exit-logic changes)

Adds **observability + experimentation infrastructure** to answer two open
questions before any live trade:

1. *"What is each token's TRUE 24h peak — independent of the bot's own
   exit decisions?"* → enables data-driven moonbag promotion design.
2. *"Does buying late on the bonding curve (at 90% progress) with a
   score+API filter beat the existing post-graduation entry?"* → enables
   A/B comparison of entry timing.

No live trading behavior changes. Bot still exits on the v12 ladder, paper
mode still on. This release is **purely about generating the data we lack**.

### 1. 24h Shadow-Log Window ([src/monitoring/mod.rs](src/monitoring/mod.rs), [config.toml](config.toml))

Extended price logging horizon from 1h to 24h with per-phase poll cadence:

- `shadow_log_duration_secs` raised from 3600 → **86400** (24h)
- Active phase (position still open): poll every **5s** (unchanged)
- Post-exit phase (after the bot closes): poll every **30s** (was 5s)
- Flush cadence: every 30s active, **every 5min post-exit** (avoids
  re-sending the growing snapshot array thousands of times over 24h)
- New `last_flush_secs` tracker replaces the brittle
  `elapsed_secs % 30 < 5` window check
- Jupiter load: ~1.6 RPS sustained across 50 concurrent post-exit tails

### 2. shadow_log Table ([migrations/020_shadow_log.sql](migrations/020_shadow_log.sql))

The `shadow_log` table the code already writes to was **never in any
migration** — it had been silently failing for the entire history of the
project. Migration 020 creates it:

- One row per `position_id`
- `snapshots` JSONB (compact `{ t, p, m, phase }` per tick)
- `shadow_peak_multiplier`, `shadow_low_usd`, `total_ticks`
- `exit_at_secs`, `exit_reason`, `duration_secs`, `completed_at`
- Indexed on `position_id`, `mint`, `completed_at`, `shadow_peak_multiplier`

Once populated, this is the ground-truth needed to backtest the 25/75 +
48h trailing moonbag strategy without depending on the bot's premature
exit decisions.

### 3. Live BC-Progress Capture ([src/detection/types.rs](src/detection/types.rs), [src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs))

Pre-v13: `bc_progress_pct` came from `https://frontend-api.pump.fun/coins/{mint}`
which is Cloudflare-protected. **All 743 historical `bc_paper_trades` rows
had `bc_progress_pct = NULL`.**

v13: extract reserves directly from every `tokenTrade` WS event:

- `WatchlistEntry` gains `last_v_sol_reserves`, `last_v_token_reserves`,
  `last_market_cap_sol`
- `handle_token_trade()` snapshots `vSolInBondingCurve`,
  `vTokensInBondingCurve`, `marketCapSol` on each trade
- `build_signal_payload()` computes
  `bc_progress_pct = ((vSol − 30) / 85) × 100` (clamped 0-100)
- WS values are now the **primary** source in `write_bc_paper_trade`;
  the pump.fun REST call is best-effort enrichment only

### 4. Lane-B Trigger ([src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs), [migrations/021_lane_b_progress_trigger.sql](migrations/021_lane_b_progress_trigger.sql))

A second BC signal trigger — independent of the existing 50-SOL volume
signal (Lane-A) — fires once per token when `bc_progress_pct ≥ 90%`.

- New `progress_signal_recorded` flag on `WatchlistEntry` (independent
  of `signal_recorded`, so a single mint can produce both rows)
- `bc_paper_trades` gains `entry_trigger`, `entry_score`, `entry_api_checks`
- Lane-A rows: `entry_trigger = 'volume_50sol'` (≈23% progress)
- Lane-B rows: `entry_trigger = 'progress_90pct'` (≈90% progress)
- Same `compute_bc_score(...)` recorded as `entry_score` on both rows
  for direct A/B filter-threshold comparison
- Both rows get auto-updated on graduation (peak prices, multipliers, etc.)

### 5. Async API-Check on Lane-B ([src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs))

Tests the hypothesis: *"filter at 90% with score + safety API checks → does
it beat Lane-A's profitability?"*

- New `run_lane_b_api_check()` runs **after** the row is written so it
  doesn't delay the trigger
- Currently calls `GoPlusFilter::check()` (~250-500ms; honeypot,
  mintable, transfer-pausable, blacklist, reclaim flags)
- PATCHes result to `entry_api_checks` JSONB column:
  `{ succeeded, goplus_passed, goplus_rejection_reason, ms_total }`
- `write_bc_paper_trade()` refactored to return `Option<i64>` row id
  (uses `Prefer: return=representation`) so the API result can be
  PATCHed onto the same row
- Easy to extend with mint-authority / freeze-authority RPC checks later

### 6. Migrations to Run on Supabase

Both must be applied manually in the Supabase SQL Editor before the bot
restart picks up the new binary:

1. [migrations/020_shadow_log.sql](migrations/020_shadow_log.sql) —
   creates `shadow_log` table (was missing entirely)
2. [migrations/021_lane_b_progress_trigger.sql](migrations/021_lane_b_progress_trigger.sql)
   — adds `entry_trigger`, `entry_score`, `entry_api_checks` to
   `bc_paper_trades`

### Unchanged from v12

- BC fast-track pipeline (score ≥ 65 → 250ms entry, deferred verification)
- All entry filters and scoring thresholds (`min_sniper_score = 60.0`)
- Exit ladder: TP1=1.8x/25%, TP2=4.0x/50%, post-TP1 trailing=22%,
  post-TP2 moonbag trailing=45-55%
- Stop loss = 35%, never_profitable = 20%, max open = 8
- Narrative path remains disabled (Option B backtest: r=−0.13, no signal)
- Reentry watcher remains disabled (`shadow_mode = true`)
- Paper trade mode active, 0.05 SOL per trade

### Expected Data After 7 Days

- ~150-300 closed positions with full 24h shadow tails
- ~200-500 Lane-B (`progress_90pct`) rows alongside their Lane-A
  counterparts for the same mints
- Enough sample to:
  - Backtest the 25/75 + 48h moonbag strategy on real 24h peaks
  - Compare Lane-A vs Lane-B grad-rate, peak, and PnL by score band
  - Test whether GoPlus-pass at 90% improves Lane-B PnL

---

## v12 — BC Fast-Track Pipeline (2026-04-24)

**Cargo version**: 0.3.0 | **strategy_version**: `v12-fast-track`

Adds a fast-track entry pipeline for tokens that scored highly during their
bonding curve phase. Instead of waiting 2s for full enrichment, pre-validated
tokens buy with only mint+GoPlus safety checks (~250ms), then run deferred
verification post-buy. Goal: buy as early as possible after graduation.

### Architecture: Dual-Pipeline Entry

The sniper pipeline now has two entry paths:

```
Token graduates → BC pattern gate → Cache lookup
  ├─ BC score ≥ 65: ⚡ FAST-TRACK (~250ms → safety filters → BUY → deferred verification 3s later)
  └─ BC score < 65 or no cache: Normal pipeline (2s enrichment → all 9 filters → sniper score → BUY)
```

### BC Score Cache ([src/detection/types.rs](src/detection/types.rs), [src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs))

- New `BcScoreEntry` struct cached in-memory when BC signals are recorded
  (at 30+ SOL cumulative volume, ~60% to graduation)
- `compute_bc_score()` function (0-100) based on:
  - Creator rebuy (-30 — strong manipulation signal)
  - Buy/sell ratio (+5 to +20 — demand pressure)
  - Unique buyers (+3 to +15 — organic interest)
  - Whale buys (+10 — conviction signal)
  - Volume momentum (+5)
- Thread-safe `Arc<Mutex<HashMap>>` shared between detection and sniper
- Auto-prune at 5,000 entries (oldest half evicted)
- Data basis: score ≥ 65 tokens that graduate → median peak 1.97x, 49% hit 2x+.
  Score < 35 → median peak 1.00x, 18% hit 2x+.

### Fast-Track Enrichment ([src/sniper/enrichment.rs](src/sniper/enrichment.rs))

- New `enrich_token_fast()` — only 2 parallel calls: on-chain mint data + GoPlus
- 1.5s timeout (typical ~250-500ms vs normal 2s budget with 11 calls)
- Returns minimal `EnrichmentResult` with only `on_chain_mint` + `goplus` populated

### Fast-Track Filters ([src/sniper/filters.rs](src/sniper/filters.rs))

- New `apply_fast_track_filters()` — checks 4 critical safety filters:
  1. Mint authority revoked (filter 1)
  2. Freeze authority revoked (filter 2)
  3. GoPlus honeypot (filter 5)
  4. GoPlus critical flags: mintable, transfer_pausable, blacklist, reclaim (filter 7)
- Skips: bundlers, liquidity, top10, dev holding, min holders (deferred)

### Deferred Verification ([src/sniper/mod.rs](src/sniper/mod.rs), [src/sniper/filters.rs](src/sniper/filters.rs))

- `run_deferred_verification()` — spawns 3s after fast-track buy
- Runs full enrichment (all 11 API calls with 2s budget)
- `apply_deferred_filters()` checks the skipped filters:
  - Bundlers > 90% → reject
  - Top-10 holders > 95% → reject
  - Dev holding > 50% → reject
  - Holders < 25 → reject
- On failure: updates `sniper_candidates` row with `action = "deferred_rejected"`
  and rejection reason for monitoring to trigger emergency exit

### Pipeline Modification ([src/sniper/mod.rs](src/sniper/mod.rs))

- `start()` now accepts `BcScoreCache` parameter
- After BC pattern gate, looks up mint in cache
- Fast-track tokens tagged with `entry_tier = "fast_track"` in sniper_features
- Logged to `sniper_candidates` with `action = "fast_track_passed"`
- Normal pipeline unchanged for tokens without cache entries or low BC scores

### Config ([src/config.rs](src/config.rs), [config.toml](config.toml))

- `bc_fast_track_enabled = true` — toggle the fast-track pipeline
- `bc_fast_track_min_score = 65.0` — minimum BC score for fast-track eligibility

### Detection Plumbing ([src/detection/mod.rs](src/detection/mod.rs), [src/main.rs](src/main.rs))

- `detection::start()` now returns `(Receiver<GraduatedToken>, BcScoreCache)`
- Cache threaded through `pumpfun_ws::run()` → `handle_token_trade()` → signal recording

### Time Savings

| Path | Enrichment | Filters | Total (typ) |
|---|---|---|---|
| Normal | 11 calls, 2s budget | 9 hard filters | 600-1000ms |
| Fast-track | 2 calls, 1.5s budget | 4 safety filters | 250-500ms |
| **Saved** | | | **~0.4-0.8s** |

### Unchanged from v11
- All exit params: TP1=1.8x/25%, TP2=4.0x/50%, trailing=30%
- Stop loss = 35%, never_profitable = 20%
- Normal pipeline filters and scoring (unchanged for non-fast-track tokens)
- Pool owner verification (v10), price cache safety (v11)
- Paper trade mode active, 0.05 SOL per trade

---

## v11 — Graduated Token Price Fix + Cache Safety (2026-04-22)

**Cargo version**: 0.2.0 | **BOT_TAG**: `v0.2.0-price-fix`

Fixes the critical root cause of zero moonbags for 2+ days: monitoring loop
price was permanently frozen for ALL graduated tokens. Every position's
`peak_multiplier`, shadow log prices, and exit decisions used stale data.
Also audits and hardens all price caches against memory leaks and stale
fallback loops.

### Root Cause: Frozen Monitoring Price (CRITICAL)

For graduated tokens (every token the bot trades), `get_monitoring_price()`
returned a stale `last_known` value forever:

1. Helius WS cache removes graduated tokens on migration
2. `last_known` HashMap returns the first-ever Jupiter price (non-zero)
3. `get_price()` (Jupiter) is only called when price == 0
4. Since `last_known` is non-zero after the first fetch, Jupiter is never
   called again → monitoring sees a FROZEN price for the entire position

**Impact (43 trades audited)**:
- ALL positions had `peak_db ≈ shadow_multiplier ≈ exit_multiplier` (frozen)
- 7 missed winners: monitor saw ~1.0x while real was 1.14x–1.47x → killed
  by `momentum_kill` at 93s
- 6+ losers bled 25–60% while monitor thought they were near-even
- Net PnL: -0.1256 SOL (18W / 25L)

| Position | Token | Monitor Mult | Real Mult | Outcome |
|---|---|---|---|---|
| 137 | AI | 0.96x | 1.47x | Winner killed as loser |
| 134 | POOKIE | 1.01x | 1.14x | Winner killed at breakeven |
| 127 | MCAT | 0.89x | 0.36x | Loss ran 2x deeper than detected |
| 124 | GROKKY | 0.98x | 1.43x | Winner killed as flat |

### Fix 1: Periodic Jupiter Refresh ([src/monitoring/mod.rs](src/monitoring/mod.rs))

Added `JUPITER_REFRESH_TICKS = 3` with a `price_poll_counter`. Every 3rd
monitoring tick (~6s), checks if the token has WS cache data via
`helius_cache().get(mint).is_some()`. For graduated tokens (no WS data),
calls `get_price()` (Jupiter HTTP) instead of relying on the stale
`last_known` fallback.

### Fix 2: Shadow Log Post-Exit Price ([src/monitoring/mod.rs](src/monitoring/mod.rs))

Shadow log continuation phase after exit now uses `get_price()` (Jupiter)
instead of the stale WS cache. Previously, post-exit shadow prices were
frozen at the last WS value before graduation.

### Fix 3: Bought Position Tracker Spawn ([src/monitoring/mod.rs](src/monitoring/mod.rs))

After monitoring exits, spawns `spawn_rejected_tracker()` for tokens that
had been bought. Previously, bought positions were never tracked post-exit,
so potential moonbag continuations were invisible.

### Fix 4: Cache Safety — `last_known` with TTL + Eviction ([src/monitoring/price.rs](src/monitoring/price.rs))

- Changed `last_known` from `HashMap<String, f64>` to
  `HashMap<String, (f64, Instant)>` with timestamps
- **TTL**: `LAST_KNOWN_MAX_AGE = 1800s` (30 min) — entries older than this
  return `None` instead of stale prices
- **Eviction**: when map exceeds `LAST_KNOWN_EVICT_THRESHOLD = 200` entries,
  evicts all entries older than the TTL
- **Cleanup**: new `remove_mint()` method called from `mod.rs` when positions
  close (both normal and inject/recovery paths), clearing both `last_known`
  and `failure_count`

### Fix 5: Cache Safety — validate_price + failure_count ([src/monitoring/price.rs](src/monitoring/price.rs))

- `validate_price` no longer re-caches stale fallback values. Callers only
  update `last_known` when the new price passes validation (preventing
  permanent price freeze on legitimate large price moves)
- `failure_count` now resets on ANY successful API response, not just when
  validation also passes. Prevents false "token dead" declaration after
  6 calls where API succeeds but validation rejects (large move scenario)
- `increment_failure_count` prunes entries for dead tokens (count ≥ max)
  to prevent unbounded memory growth

### Unchanged from v10
- All entry filters and scoring thresholds
- `min_sniper_score = 60.0`
- TP targets: TP1=1.8x/25%, TP2=4.0x/50%, trailing=30%
- Stop loss = 35%, never_profitable = 20%
- Max open positions = 8
- Pool owner verification (v10 fix preserved)
- PumpSwap vault resolution (v9/v8 fixes preserved)
- LP grace period = 45s

---

## v10 — Pool Owner Verification (Tick Monitoring Fix) (2026-04-21)

Fixes the REAL root cause of the momentum-kill plague: pool addresses stored
for v8/v9 positions were bonding-curve accounts, not PumpSwap pools. 9 of 10
recent positions had this bug, producing `ticks=0, momentum_ratio=0.5`.

### The Pool-Address Bug (CRITICAL)

The pump.fun `tokenComplete` WebSocket event's `pool`/`poolAddress` field
often returns the **bonding-curve account pubkey** (owned by PumpFun program
`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`, 151-byte data) rather than
the actual PumpSwap AMM pool (owned by `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj`,
429-byte data). Since that pubkey parses validly as a Pubkey, the migration-tx
and DexScreener fallback paths never ran. Downstream,
`resolve_pool_vaults()` saw the wrong owner, returned `None`, fell back to
`derive_ata()` on the BC account — resulting in zero tick notifications and
every position exiting via blind `momentum_kill`.

**Fix** (`src/detection/pumpfun_ws.rs`):
- Added `verify_pool_owner()` helper that calls `getAccountInfo` and checks
  owner against `PUMP_AMM_PROGRAM` / `RAYDIUM_AMM_V4`.
- After extracting pool from event, if owner is not a supported AMM the pubkey
  is discarded, forcing fallback to migration-tx parsing then DexScreener.
- Validated via RPC: 9/10 recent positions had owner=`6EF8…` (BC), 1 had
  owner=`LanMV…` (real pool).

### Filters Intentionally NOT Tightened

Initial analysis of 94 recent closed trades suggested tightening several
filters (score ≥65, bundlers 30-50% death zone). Cross-checking against
historical pre-DB-clear data (53 trades, **+2.63 SOL, 65% WR**) completely
flipped those conclusions:

| Signal | Recent (94, post-bug) | Historical (53, pre-bug) |
|---|---|---|
| Bundlers 30-50% | 0% WR, -0.087 SOL | **100% WR, +0.298 SOL** |
| Detection >8000ms | 34% WR, -0.300 SOL | **71% WR, +0.618 SOL** |
| Overall | 35% WR, -0.46 SOL | **65% WR, +2.63 SOL** |

**Conclusion**: recent data's "bad" signals are artifacts of the
momentum-kill bug polluting every trade's PnL, NOT true feature signals.
Historical (profitable) era had looser filters. The problem was never filter
looseness — it's the tick monitoring bug. v10 ships the pool fix alone and
leaves filters at v9 thresholds until tick data is healthy.

### Unchanged from v9
- `min_sniper_score = 60.0` (not bumped)
- 9 hard filters (no bundlers 30-50% reject added)
- All v9 structural fixes (WSOL vault offsets, PumpSwap AMM program ID)

---

## v9 — PumpSwap Program ID Fix + Data-Driven Filter Tightening (2026-04-21)

Two critical fixes: PumpSwap vault resolution was using the wrong program ID
and data offsets (making the v8 momentum fix ineffective), plus 3 new
data-driven filters to cut losing trades without losing moonbag potential.

**PumpSwap Program ID & Layout Fix** ([src/monitoring/helius_ws.rs](src/monitoring/helius_ws.rs), [src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs)):
- **Root cause**: `resolve_pool_vaults()` used program ID
  `PSwapMdSai8tjrEXcz51jHXJ9SqeShTSGrHUpFNvFJf` — the real PumpSwap AMM
  is `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj`. Every pool was
  rejected as "not PumpSwap" → fell back to broken ATA derivation →
  zero ticks on ALL positions even after v8.
- **Data layout fix**: Pool account struct has additional fields. Correct
  offsets: base_mint at 205 (was 43), quote_mint at 237 (was 75),
  token vault at 269 (was 139), WSOL vault at 301 (was 171).
  Minimum data length: 333 bytes (was 203).
- Verified against live pool `9fiLDm5scPW4qoUGt8mD...` — vaults resolve
  correctly and point to real SPL token accounts.
- Also fixed `PUMP_AMM_PROGRAM` constant in `pumpfun_ws.rs` for migration
  tx pool resolution.

**Filter 8: Dev Holding % Gate** ([src/sniper/filters.rs](src/sniper/filters.rs)):
- Hard reject if dev holds > 50% of supply.
- Data: dev_pct ≥ 48% produced ONLY losers (-0.048 to -0.049 SOL each).
  Winners median dev_pct = 2.36%, losers median = 4.74%.
- Backtested: blocks 9 trades, saves -0.101 SOL in losses.

**Filter 9: Minimum Holder Count** ([src/sniper/filters.rs](src/sniper/filters.rs)):
- Hard reject if holder count < 25 (cross-sourced: ST holders + GoPlus).
- Data: 0-10 holders = serial losers with no real community.
  Winners median 143 holders, losers median 121.
- Backtested: blocks 19 trades, saves -0.097 SOL.

**Minimum Sniper Score Gate** ([src/sniper/mod.rs](src/sniper/mod.rs), [src/config.rs](src/config.rs)):
- Reject candidates with `sniper_score < min_sniper_score` (config, default 60).
- Data: score ≥ 65 → +0.056 SOL profit, score < 65 → -0.500 SOL loss.
  Using 60 as conservative threshold.
- Backtested: blocks 51 trades, saves -0.329 SOL in losses.
- New config field: `min_sniper_score = 60.0` in `[filters]`.

**Pre-existing Bug Fix** ([src/execution/mod.rs](src/execution/mod.rs)):
- `FilteredToken` doesn't have `name`/`symbol` directly — fixed to use
  `token.event.symbol` and `token.event.name` for shadow dip watchlist.

**Backtested Impact (92 historical trades)**:
- Trades: 92 → 35 (much more selective)
- Net PnL: -0.411 → -0.122 SOL (+0.290 SOL improvement)
- Blocked 29 losers (-0.485 SOL avoided), sacrificed 16 small winners (+0.204 SOL)
- Top winner (id=47, +0.107 SOL, score=70.7) still passes all filters
- Previously-rejected 2.0x and 2.2x moonbags would now pass (st_risk_score removed in v8)

---

## v8 — PumpSwap Momentum Fix + Safety Tuning (2026-04-21)

Critical bug fix: momentum monitoring was completely blind on all positions,
causing every trade to exit via blind ~93s timeout. Root cause: PumpSwap pool
vault addresses were incorrectly derived, silently disabling the tick stream.
Also fixes LP false exits on fresh graduates and rugcheck false positives.

**P0: PumpSwap Vault Resolution** ([src/monitoring/helius_ws.rs](src/monitoring/helius_ws.rs), [src/monitoring/mod.rs](src/monitoring/mod.rs)):
- **Root cause**: Code assumed Raydium-style pools where token vault = ATA of
  pool address and SOL vault = the pool itself (raw SOL). PumpSwap pools
  (used by ALL PumpFun graduates) store tokens in separate vault accounts
  recorded in the pool's on-chain data, with WSOL instead of raw SOL.
- **Result**: `get_token_account_balance()` failed on the non-existent derived
  ATA → tick stream silently disabled → `momentum_ratio` stuck at 0.5
  (default) → every `momentum_kill` was a blind 93s timeout, not a real
  momentum decision.
- **Fix**: New `resolve_pool_vaults()` function fetches PumpSwap pool account
  from RPC and extracts actual vault Pubkeys from Borsh-serialized struct
  (token vault at offset 139, WSOL vault at offset 171). Falls back to
  legacy ATA derivation for non-PumpSwap (Raydium) pools.
- New `PoolVaults` struct, `PUMP_AMM_PROGRAM_ID` constant.
- `watch_pool_trades()` / `subscribe_pool_vaults()` now accept
  `sol_vault_is_token_account` flag — WSOL vaults subscribe with
  `jsonParsed` encoding and parse via `parse_token_balance()` instead of
  `base64` / `parse_lamports()`.
- Initial SOL balance fetch uses `get_token_account_balance()` for PumpSwap
  WSOL vaults vs `get_balance()` for Raydium.

**P1: LP Removal Grace Period** ([src/monitoring/mod.rs](src/monitoring/mod.rs), [src/config.rs](src/config.rs)):
- Fresh PumpFun graduates shuffle LP during PumpFun→PumpSwap migration.
  APXdsrtBf8 was killed at 17s via `liquidity_removed` — went to 13.3x.
- LP watcher now sleeps `lp_grace_period_secs` (default: **45s**) before
  activating. Checks shutdown signal after grace period.
- New config field: `lp_grace_period_secs` in `[strategy.monitoring]`.

**P2: Rugcheck Critical Threshold** ([src/filters/post_buy.rs](src/filters/post_buy.rs)):
- Post-buy rugcheck critical score threshold: **10,000 → 15,000**.
- NiPjkeGPo1 had rugcheck=14,395 but was legit ($1M mcap, 10.1x peak).
  Fresh tokens get inflated scores before RugCheck fully indexes them.

**BC Price Tracking Fix** ([src/detection/pumpfun_ws.rs](src/detection/pumpfun_ws.rs)):
- `spawn_bc_price_tracker()` was `.await`'d inline, blocking the parent
  spawn for 1+ hour. Changed to `tokio::spawn()`.
- `fetch_bc_price()` called immediately at graduation but DexScreener
  hasn't indexed yet → baseline=0 → all price deltas zero. Added 15s
  initial delay + 3 retry attempts with 10s spacing.

**Filter Removals** (deployed mid-session, still v7 tag):
- Removed `bc_sell_count` hard filter — was calibrated on 50-SOL-signal
  data but ran at graduation time where median was 208 sells. Blocking
  100% of BC-observed graduates.
- Removed `st_risk_score` filter — insufficient data, blocking tokens
  with 2.2x–8.5x peaks.

**Impact**:
- Momentum monitoring now receives real trade data for PumpSwap pools.
  Exit decisions will be data-driven instead of blind timeouts.
- LP false exits eliminated for first 45s (migration window).
- Rugcheck false positives reduced (scores 10K–15K no longer trigger exit).

**Unchanged from v7:**
- All entry filters, scoring, thresholds
- TP targets, trailing stops, dip state machine
- Moonbag promotion (min_score = 40)
- Stop loss = 35%, never_profitable = 20%
- Max open positions = 8
- Chainstack gRPC filters, dual-submit

---

## v7 — Chainstack Full Utilization + Dual-Submit (2026-04-19)

Maximizes the $98/mo Chainstack investment by using all available gRPC filter
slots and adding dual-submit redundancy for transaction landing. No strategy
parameter changes — pure infrastructure optimization.

**Yellowstone gRPC Extended Filters** ([src/monitoring/yellowstone_grpc.rs](src/monitoring/yellowstone_grpc.rs)):
- **Accounts filter #2: Dev wallet ATA watcher** — subscribes to developer
  token accounts for real-time rug-dump detection via gRPC instead of RPC
  polling. Detects balance drops at validator-memory speed. ATA bytes cached
  for O(1) per-tick comparison (no per-slot base58 decoding).
- **Transactions filter #2: Bot wallet tx confirmation** — subscribes to all
  non-failed transactions involving the bot wallet. Enables instant gRPC-based
  confirmation instead of `getSignatureStatuses` polling. Bot wallet pubkey
  derived from `WALLET_PRIVATE_KEY` at startup.
- **Graduation event pipeline** — `handle_transaction_update` now builds
  `GraduatedToken` events with `DetectionSource::Geyser` and sends through
  `graduation_tx` channel. Provides observability into Raydium graduation
  latency vs pump.fun WS detection.
- **`WatchDevWallet` command** — new `MuxCommand` variant + handler. Base58
  validation on receipt with explicit `warn!` on invalid addresses.
- **gRPC utilization**: 2/5 accounts filters, 2/5 transactions filters active
  (was 1/5 each in v6).

**Dual-Submit Buy/Sell** ([src/execution/mod.rs](src/execution/mod.rs), [src/exit/mod.rs](src/exit/mod.rs)):
- When using Chainstack Warp TX, the same signed transaction is simultaneously
  fire-and-forget submitted to the backup RPC for redundancy.
- Same signature = same transaction on Solana (network deduplicates). No risk
  of double execution.
- Non-critical backup failures logged at `debug` level. Does not block the
  primary Warp TX submission path.

**Audit Fixes**:
- Silent graduation event failures now logged with `warn!` (invalid mint).
- Bot wallet derivation failure logged with `warn!` instead of silently
  disabling gRPC tx confirmation.
- Dev ATA base58 validation added in `WatchDevWallet` handler — invalid
  addresses rejected with warning instead of silently breaking rug detection.

**Unchanged from v6:**
- All entry filters, scoring, thresholds
- TP targets, trailing stops, dip state machine
- Moonbag promotion (min_score = 40)
- MomentumKill trigger (40s / 1.3x)
- Stop loss = 35%, never_profitable = 20%
- Max open positions = 8

---

## v6 — Helius WS Price Stream + Exit Hardening (2026-04-18)

Replaced Jupiter polling for price ticks with real-time Helius Enhanced WS
(`accountSubscribe` on bonding curve PDAs). Major exit engine reliability
improvements.

**Helius WS Price Stream** ([src/monitoring/helius_price_ws.rs](src/monitoring/helius_price_ws.rs) — new):
- Multiplexed single-WS connection subscribing to pump.fun bonding curve
  accounts for all active positions.
- `HeliusPriceCache`: thread-safe, per-mint price cache with hit/miss/stale
  tracking and 60s metrics flushing to Supabase.
- `PriceStreamBackend` enum abstracts Helius vs Yellowstone (later replaced
  by Yellowstone gRPC in v6.1).
- Falls back to Jupiter automatically for graduated/Raydium tokens.
- Config: `enable_helius_price_ws = true` in `[monitoring]`.

**Note**: Helius Developer plan was later found to silently drop
`accountNotifications` for pump.fun PDAs (0% cache hit rate). v6.1 replaced
this with Chainstack Yellowstone gRPC.

**Exit Engine Hardening** ([src/exit/mod.rs](src/exit/mod.rs), [src/exit/dedup.rs](src/exit/dedup.rs) — new, [src/exit/error.rs](src/exit/error.rs) — new):
- **Exit deduplication** (`dedup.rs`): `DedupRegistry` prevents concurrent
  exit attempts for the same position. Guard-based RAII pattern.
- **Structured error handling** (`error.rs`): `ExitError` enum with
  categorized errors (slippage, insufficient balance, TOKEN_NOT_TRADABLE,
  etc.) and retry/permanent classification.
- **577-line exit rewrite**: improved retry logic with per-error-type
  handling, slippage re-quoting, and proper partial-fill detection.

**Jupiter Improvements** ([src/execution/jupiter.rs](src/execution/jupiter.rs)):
- 189 LoC of improvements to quote/swap error handling.

**Config Changes** ([config.toml](config.toml)):
- `moonbag_promotion_min_score`: 50 → **40** (only 8/206 v5 positions
  cleared 50; narrative scores cluster 45-55).

### v6.1 — Chainstack Migration (2026-04-18)

Migrated from Helius to Chainstack ($98/mo: Growth $49 + Yellowstone gRPC $49)
after proving Helius WS broken for pump.fun PDAs.

**Yellowstone gRPC Integration** ([src/monitoring/yellowstone_grpc.rs](src/monitoring/yellowstone_grpc.rs) — new):
- Single long-lived gRPC stream with server-side filtering (Chainstack
  Yellowstone, Jito ShredStream enabled).
- Accounts filter: bonding curve PDAs (up to 50 accounts).
- Transactions filter: Raydium AMM V4 `initialize2` for graduation detection.
- Exponential backoff reconnect with state preservation.
- `PriceStreamBackend::Yellowstone` variant replaces Helius WS.

**Chainstack Warp TX** ([src/execution/helius_sender.rs](src/execution/helius_sender.rs)):
- Removed: Helius tip accounts, `inject_tip_instruction`, tip logic (saves
  0.0005 SOL/trade).
- `send_transaction` made generic (works with any RPC URL).
- `get_priority_fee_estimate` rewritten to use standard Solana RPC
  `getRecentPrioritizationFees` with p75 calculation (was Helius-specific
  `getPriorityFeeEstimate`).

**Safety Watchers Migrated** ([src/monitoring/mod.rs](src/monitoring/mod.rs)):
- Dev dump, LP drain, tick stream watchers switched from `helius_ws_url`
  to `solana_ws_url` (Chainstack WS).

**Bug Fixes:**
- Exit `getPriorityFeeEstimate` was routing to Chainstack (doesn't support
  Helius-specific method) → fixed to standard `getRecentPrioritizationFees`.
- gRPC backoff never reset after successful connect → fixed.
- Raydium graduation dedup: `seen_pools` set prevents double-logging.

---

## v5 — Data-Driven Filter Loosening (2026-04-17)

Audit of 69 Supabase positions (v3=54, v4=15) + 275 sniper_candidates showed
v4 was **underperforming v3**: net −0.14 SOL vs +0.30 SOL, zero 2x peaks in v4.
Hard filters were rejecting runners, not rugs. **Tier A** (filter loosening) +
**Tier B** (trailing engagement) applied.

**Missed moonshots under v4 (rejected by sniper gate, real post-rejection peaks):**
- `human` — 162.5x — `top10_holders=84.9% > 80%` (soft zone, safety<60)
- `$WW3` — 33.9x — `bundlers_pct=81.4% > 80%` (1.4% over hard floor)
- `Stimmy` — 26.3x — `initial_liquidity=5.5 SOL < 20`
- `ASSDAQ` — 8.6x, `EPHYRA` — 7.1x, `CLI` — 3.1x — all bundlers 60-80% soft zone rejects

**Hard Filter Changes** ([src/sniper/filters.rs](src/sniper/filters.rs)):
- **Bundlers**: hard >80% → **>90%**; soft-zone safety floor 60 → **50**
- **Top-10**: hard >90% → **>95%**; soft-zone safety floor 60 → **50**
- **Liquidity**: hard <20 SOL → **<10 SOL**; new 10-20 soft zone requires safety ≥ 50
- **Bundlers data-bug sanitizer**: values > 100% (seen: 118, 166, 173) are treated as missing,
  not as fail. Prevents false rejects like `pikachu` (4.8x, bundlers=166%).

**Fast Gate Config Changes** ([config.toml](config.toml)):
- `min_buy_pressure_pct` 58 → **54** (10+ tokens died at 55-57.9% marginal)
- `max_token_age_seconds` 600 → **900** (10 rejections in 600-900s bucket)

**Exit Engine Changes** ([config.toml](config.toml)):
- `trailing_stop_min_multiplier` 1.3 → **1.15** — 5 stop-losses had peak 1.2-1.5x
  where trailing never engaged. Let trailing protect small winners instead of
  waiting for -35% stop-loss.

**Expected Impact:**
- Accept ~20-30% more tokens (safer soft zones widen, age/buy_pressure loosen)
- Convert marginal winners (peak 1.2-1.5x) from stop-loss bleed to trailing exits
- One missed `human`-style 162x winner is worth ~+8 SOL @ 0.05 SOL size —
  outweighs many small losses

**Still deferred (Tier C — zero-API-cost):**
- Singleton RugCheck/GoPlus filters (3x cache misses across enrichment/precheck/post_buy)
- Wire `insiders_pct` / `jupiter_verified` / `sniper_count` into `compute_concentration_safety()`
- Use RugCheck `top_holders[11..50]` sum in safety score

### v5 post-deploy additions (2026-04-17)

No strategy change — pure correctness + observability improvements. Still tagged v5.

**Exit engine bug fix** ([src/exit/mod.rs](src/exit/mod.rs)):
- Partial-fill handler now uses on-chain balance as truth instead of requested amount.
- Dust threshold = max(1.0, 0.5% of original token_amount). Removed speculative retry loop.
- Fixes the ASTEROID (id=62) incident where `stop_loss` logged 100% sold but ~28%
  of tokens were stranded on-chain. `exit_reason` now gets `+stranded_dust` suffix
  when leftover exceeds dust threshold.

**DB hygiene** (one-shot):
- id=62 patched with real PnL (+2.819 SOL / +5638%) from user's manual USDC sale.
- 8 stranded-dust positions batch-patched: `token_amount=0`, exit_reason tagged.

**Dip sub-reason logging** ([src/monitoring/types.rs](src/monitoring/types.rs), [src/monitoring/mod.rs](src/monitoring/mod.rs), [src/exit/mod.rs](src/exit/mod.rs)):
- `ExitSignal` gained `sub_reason: Option<String>`.
- `dip_death` exits now write `exit_reason = "dip_death:<dip_reason>"`
  (e.g. `dip_death:whale_sell_during_dip`) for post-hoc bucket analysis.

**Enrichment sampler — 3-tier passive data collection**
([src/monitoring/api_limiter.rs](src/monitoring/api_limiter.rs),
[src/monitoring/enrichment_sampler.rs](src/monitoring/enrichment_sampler.rs),
[migrations/010_position_enrichment_snapshots.sql](migrations/010_position_enrichment_snapshots.sql)):
- Writes to new `position_enrichment_snapshots` table (40+ cols, JSONB raw payloads).
- **Schedule**: T+30s, 2m, 5m, 10m, 20m, 30m, 60m per open position.
- **Tier 2**: ad-hoc snapshot fired right before every `dip_death` exit (trigger `pre_dip_death:<reason>`).
- **Tier 3**: 1h after exit, if price > 3× entry, log a `post_exit_1h` snapshot to quantify missed moonbags.
- **Rate-limit guardrails**: per-API semaphore + circuit breaker (3 failures in 60s → 5min cooldown).
  Guards: helius_rpc (8/150ms), helius_das (2/250ms), birdeye (2/700ms),
  dexscreener (3/250ms), solana_tracker (2/700ms), jupiter (3/250ms).
- Flags in [config.toml](config.toml) `[monitoring]`:
  `enrichment_sampler_enabled = true`, `enrichment_post_exit_check_enabled = true`.
- **Purpose**: accumulate the per-position holder/volume/social/smart-wallet/whale trails
  the bot currently lacks during hold phase, so v6 can make data-driven decisions
  about which `dip_death` exits were premature and which exits missed moonbags.

---

## v1 — Baseline (2026-04-15)

First 111 paper-trade positions. Baseline data before any filter tuning.

**Exit config:**
- stop_loss_pct = 35%, never_profitable_stop_loss_pct = 25%
- trailing_stop_pct = 30%
- TP targets: 1.8x / 4.0x / 8.0x
- dip_threshold_pct = 15%, dip_grace_period_secs = 25, min_hold_before_dip_death = 30

**Results:**
- 111 positions total
- 24.3% dip_death (27 positions) — biggest drag
- 10.8% stop_loss (12 positions) — most damaging per-trade
- 37.8% trailing_stop (42 positions) — main profit engine
- 0% rug_pull / whale_dump (never triggered)

---

## v2 — Dip Death Reduction + Opportunity Capture (2026-04-15)

Targeted fixes for the 24.3% dip_death and 10.8% stop_loss exit rates from v1,
plus filter relaxation to capture the 56 sustained runners missed by v1.

**Exit / Trigger Changes:**
- **Early Momentum Kill Gate** (new exit trigger):
  - `momentum_kill_secs = 40` — check at 40s after entry
  - `momentum_kill_min_multiplier = 1.3` — must be 1.3x by then or exit
  - New `ExitReason::MomentumKill` — catches tokens with no traction before
    they bleed into dip_death (85% of v1 dip_death positions never hit 1.3x)
- **Tighter never_profitable stop loss**: 25% → 20%
  - Faster cut on tokens that never show signs of life
- **Raised min buy pressure**: 52% → 58%
  - Weak buy pressure at graduation strongly correlated with dip_death exits

**Filter Relaxation Changes:**
- **Bundler threshold**: 40% → 60% (`src/sniper/filters.rs`)
  - Sensitivity analysis: 29 sustained >2x winners in 40-60% band (avg 137x peak),
    57 losers. Asymmetric upside justifies relaxation.
- **Top-10 holder concentration**: 65% → 80% (`src/sniper/filters.rs`)
  - 75-80% bucket: 10 sustained winners (22% win rate), 9 went 3x+.
    Above 80%, manipulation probability spikes.
- **Max open positions**: 5 → 8 (`config.toml`)
  - 74% of v1 passed tokens never traded due to full slots.

**Results (28 v2 trades):**
- ROI: +61.9%, Win rate: 75.0%
- Dip death: 0% (down from 24.3%)
- MomentumKill: 39.3% of exits (avg peak 1.42x — correctly killing duds)

**Unchanged from v1:**
- TP targets: 1.8x / 4.0x / 8.0x
- trailing_stop_pct = 30%
- dip state machine params unchanged
- Low liquidity threshold unchanged

---

## v3 — Moonbag Survival + Promotion Fix (2026-04-16)

Fixes for moonbag system based on v2 data analysis: 2 moonbags created but both
died in <10 seconds due to trailing stop firing during post-spike pullback.

**Changes:**
- **Fast-runner moonbag grace period**: 45 seconds (`src/monitoring/moonbag.rs`)
  - Trailing stop is disabled for first 45s after fast-runner promotion.
  - Lets the natural post-spike pullback settle before trailing activates.
  - Floor check (1.2x entry) still active during grace — protects against total collapse.
  - v2 moonbags died at 6s and 9s; shadow log showed both tokens continued to 10-13x.
- **Moonbag promotion threshold lowered**: 60 → 50 (`config.toml`)
  - Two 8x+ tokens scored 52 and 55 — just below the old 60 threshold.
  - Both exited via TP3 with strong profits. Lowering to 50 captures these.
- **Moonbag promotion bug fix**: (`src/monitoring/mod.rs`)
  - `moonbag_promoted`, `exit_reason`, and `status` now persisted to positions table
    when a moonbag promotion occurs. Previously only logged to system_events.

**Unchanged from v2:**
- All entry filters (bundler 60%, top10 80%, liq 20 SOL)
- MomentumKill trigger
- TP targets, trailing stops, dip state machine
- Max open positions = 8

---

## v4 — Concentration Scoring + Jupiter Rate Limiter + API Cost Reduction (2026-04-17)

Data-driven changes from the overnight run (Apr 16 21:35 → Apr 17 09:04 UTC):
49 positions, +0.2716 SOL net, 46.7% WR. Key findings: 22/27 execution failures
were Jupiter 429s, and 12 missed opportunities included "human" (162.5x, rejected
by top10=84.9% > 80%).

### Entry Filter Changes

- **Top-10 holder concentration** (`src/sniper/filters.rs`):
  - Hard reject raised: 80% → 90%
  - New soft zone 80-90%: pass if `compute_concentration_safety() >= 60`
  - Data: "human" had top10=84.9% but liq=85 SOL, dev=0%, bundlers=2.3%,
    LP 100% burned, 54 buyers → would score ~95 → PASS. Went 162.5x.
  - Pure scams are all >95%: dev>5%, LP not burned, <10 buyers.

- **Bundlers threshold** (`src/sniper/filters.rs`):
  - Hard reject raised: 60% → 80%
  - New soft zone 60-80%: pass if `compute_concentration_safety() >= 60`
  - Data: EPHYRA (62.8% bundlers, liq=62 SOL, dev=0%, 165 buyers) went 7.1x.

- **New scoring function: `compute_concentration_safety()`** (`src/sniper/filters.rs`):
  - 0-100 score using 9 existing enrichment signals (no extra API calls):
    - Liquidity depth: +15 (≥80 SOL), +10 (≥50), +5 (≥30), -10 (<30)
    - Dev holding: +10 (<0.1%), -10 (>2%), -20 (>5%)
    - Bundlers cross-check: +8 (<10%), -10 (>50%)
    - LP burn: +10 (≥99%)
    - Holder count: +5 (≥50), -5 (<10)
    - Risk score: +5 (≤5), -10 (≥50)
    - Mint+freeze revoked: +5
    - Smart wallets: +5 (≥5 genuine), -10 (>30% suspicious)
    - Whale buys: +5 (≥5 whale buys + 2x buy/sell ratio)
  - Threshold: score ≥ 60 → pass soft zone. < 60 → reject.
  - Backtested: 5 safe tokens would pass (+2.59 SOL estimated gain),
    7 risky tokens correctly rejected.

### Jupiter Rate Limiter (fixes 429s without paid API)

- **Concurrency semaphore** (`src/execution/jupiter.rs`):
  - `tokio::sync::Semaphore` with 2 permits (`JUPITER_MAX_CONCURRENT = 2`)
  - `get_quote()` and `get_swap_transaction()` acquire permit before retry loop
  - Shared via `Arc` across all buy/exit tasks
  - Prevents burst 429s when multiple positions try to exit simultaneously
  - Overnight run had 22/27 execution failures from Jupiter 429s

- **Reduced retry counts** (cuts max Jupiter calls per exit from ~60 to ~27):
  - Buy quote retries: 5 → 3 (`src/execution/mod.rs`)
  - Buy retry delay: 1s → 2s
  - Exit route retries: 5 → 3 (`src/exit/mod.rs`)
  - Exit slippage retries: 4 → 3

### Twitter/X API Cost Reduction

- **Deferred search** (`src/narrative/mod.rs`):
  - `fetch_twitter_search` moved after dead-token check — saves ~40% of search calls
  - Dead tokens were consuming full 3-tier X API searches before being detected

- **Reduced narrative checks**: 3 → 2 (`config.toml`):
  - `narrative_check_intervals_secs = [120, 300]` (was [120, 300, 420])
  - Most positions exit by T+4m. Third check at T+7m rarely fires.

- **Moonbag narrative re-checks disabled** (`src/monitoring/moonbag.rs`):
  - `is_narrative_recheck_due()` returns `false` always
  - On-chain data (DexScreener, Birdeye) already captures the same signals

### Exit Engine Fixes

- **TOKEN_NOT_TRADABLE immediate bail** (`src/execution/jupiter.rs`):
  - No longer retries forever on tokens with drained LP
  - New `permanent: bool` field on `ExitResult`
  - Monitoring loops break on permanent failure instead of retrying

### Database Fix

- **Migration 009**: `exit_attempts` column added to positions table
  - Without this, PostgREST returns HTTP 400 on exit PATCH, silently dropping
    the entire update (status, pnl_sol, exit_price, etc.)
  - Root cause of positions stuck as "open" when already closed on-chain

### Filter Engine Fixes (from earlier in session)

- **Bonding volume sanity** (`src/filters/sanity.rs`):
  - Changed `< 0.5` to `> 0.0 && < 0.5` — 0.0 means "not measured" (ST/Raydium),
    not garbage data. Was killing 22 tokens per session.

- **Lowered min_liquidity_usd**: 10000 → 6000 (`config.toml`)
  - Pump-AMM graduated tokens launch at ~$6-10K.

- **Lowered min_unique_buyers**: 12 → 8 (`config.toml`)
  - Tokens with 8-11 buyers were being rejected, some went 2-5x.

**Unchanged from v3:**
- TP targets: 1.8x / 4.0x / 8.0x
- Trailing stops, dip state machine
- Max open positions = 8
- Moonbag promotion threshold = 50
- MomentumKill trigger (40s / 1.3x)
- Stop loss = 35%, never_profitable = 20%
