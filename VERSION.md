# Strategy Versions

Each version is stamped into every `positions` and `moonbag_positions` row via the
`strategy_version` column. Set the active version in `config.toml`:

```toml
strategy_version = "v10"
```

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
