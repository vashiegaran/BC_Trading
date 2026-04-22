# BC Research Bot — Setup & Cleanup Instructions

> **Context:** This repo is a copy of the `Trading-MemeCoins` sniper bot. The goal is to strip it
> down to **only the bonding-curve pipeline**, run it in **paper-trade mode**, and use the
> simulated trades as research data to validate the BC gate before ever risking real SOL.

---

## What this bot does (simplified)

```
PumpPortal WebSocket (free)
    → detects new pump.fun token launches
    → records bonding-curve trade patterns to `bonding_curve_signals` table
    → on graduation (token migrates to Raydium):
        → enrichment (parallel API calls, ~2s)
        → hard filters (liquidity, holders, honeypot, rugcheck, etc.)
        → sniper scoring
        → paper-trade buy (simulated, no real SOL)
        → price monitoring (exit triggers: TP1/TP2/TP3, stop-loss, trailing)
        → paper-trade sell
        → results logged to `positions` + `trades` tables in Supabase
```

The **BC gate** (BSR ≥ 2, unique_buyers ≥ 40, no creator rebuy) is already partially implemented
as hard filters in `config.toml` and `src/sniper/filters.rs`. The paper-trade data will tell us
whether these filters produce positive PnL, not just predict graduation.

---

## Services & API keys

### Shared (paid — reuse from original repo)
| Service | Env var | Plan | What it does |
|---|---|---|---|
| **Helius RPC** | `SOLANA_RPC_URL`, `SOLANA_WS_URL`, `HELIUS_WS_URL`, `HELIUS_API_KEY` | Developer ($49/mo) | Solana RPC, WebSocket price streams, DAS asset lookups, dev-wallet monitoring |
| **Helius Sender** | `HELIUS_SENDER_URL` | Same plan | Priority TX submission (not used in paper mode, but code references it) |
| **Birdeye** | `BIRDEYE_API_KEY` | Starter (free tier ~15 RPS) | Price data for enrichment + research scripts |

### New (free — create fresh accounts)
| Service | Env var | What to do |
|---|---|---|
| **Supabase** | `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` | Create new project at supabase.com. Run migrations (see below). |
| **PumpPortal WS** | None (hardcoded `wss://pumpportal.fun/api/data`) | Free public WebSocket, no account needed. |
| **pump.fun REST** | None (hardcoded `https://frontend-api.pump.fun/...`) | Free public API, no key. |
| **DexScreener** | None (hardcoded) | Free public API, no key. |
| **GoPlus** | None | Free security API, no key. |
| **RugCheck** | None | Free API, no key. |
| **Jupiter** | None | Free swap API (paper mode simulates quotes, still calls for pricing). |

### Optional (can disable)
| Service | Env var | Notes |
|---|---|---|
| **OpenAI** | `OPENAI_API_KEY` | Narrative moonbag scoring. Set `narrative_check_enabled = false` in config.toml to skip. |
| **Solana Tracker** | `SOLANA_TRACKER_API_KEY` | Backup detection + trade stream. Leave unset to disable. |
| **Telegram** | `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | Alert notifications. Leave unset to disable. |
| **Yellowstone gRPC** | `YELLOWSTONE_GRPC_*` | Alternative price stream. Leave unset to disable. |

---

## Step 1 — Copy files to new repo

Copy **everything** from the original repo into the new one. Then proceed with cleanup below.

---

## Step 2 — Create `.env` file

```env
# ── Network ──
SOLANA_NETWORK=mainnet

# ── RPC (Helius — shared from original) ──
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY
SOLANA_WS_URL=wss://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY
SOLANA_RPC_BACKUP_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY
HELIUS_API_KEY=YOUR_HELIUS_KEY
HELIUS_WS_URL=wss://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY

# ── Wallet (paper-trade wallet — no real SOL needed beyond gas) ──
WALLET_PRIVATE_KEY=YOUR_PAPER_WALLET_PRIVATE_KEY

# ── Supabase (NEW project) ──
SUPABASE_URL=https://YOUR_NEW_PROJECT.supabase.co
SUPABASE_SERVICE_KEY=YOUR_NEW_SERVICE_ROLE_KEY

# ── Detection ──
DETECTION_METHOD=pumpfun_ws
POLL_RAYDIUM=false

# ── Execution ──
USE_JITO=false
JITO_BLOCK_ENGINE_URL=
USE_HELIUS_SENDER=false

# ── Paper trade mode (CRITICAL — must be true) ──
PAPER_TRADE=true

# ── Birdeye (shared from original) ──
BIRDEYE_API_KEY=YOUR_BIRDEYE_KEY

# ── Logging ──
LOG_LEVEL=info

# ── Optional (leave blank to disable) ──
# OPENAI_API_KEY=
# SOLANA_TRACKER_API_KEY=
# TELEGRAM_BOT_TOKEN=
# TELEGRAM_CHAT_ID=
```

**Critical:** `PAPER_TRADE=true` — this makes the bot simulate all buys/sells without sending real transactions. It still calls Jupiter for price quotes but never submits transactions.

---

## Step 3 — Set up new Supabase

Create a new Supabase project. In the SQL editor, run these migrations **in order**:

### Required migrations (core pipeline)
```
migrations/001_sniper_tables.sql     — sniper_candidates, creator_reputation
migrations/002_pipeline_latency.sql  — pipeline_latency
migrations/003_trade_costs.sql       — trade_costs
migrations/005_narrative_moonbag.sql — moonbag_positions, narrative_checks
migrations/008_strategy_version.sql  — adds strategy_version column
migrations/009_exit_attempts_column.sql
migrations/010_position_enrichment_snapshots.sql
migrations/011_trade_latency.sql     — trade_latency
migrations/012_sniper_candidates_score.sql
migrations/014_narrative_peak_price.sql
```

### Required for BC research
```
migrations/013_shadow_strategy_tables.sql  — bonding_curve_signals + shadow tables
migrations/017_bc_gate_backtest.sql        — PnL backtest results table
```

### Skippable (shadow features being removed)
```
migrations/004_st_trade_snapshots.sql     — SolanaTracker snapshots (not needed)
migrations/006_narrative_result_column.sql — can skip if moonbag disabled
migrations/007_fast_runner_moonbag.sql     — can skip if moonbag disabled
migrations/015_tracked_wallets.sql         — copy-trader wallets (removing)
migrations/016_archive_smart_wallet_signals.sql — copy-trader archive (removing)
```

Also create the `positions` and `trades` tables — these are created by the bot's Supabase logger
on first insert, but if you want them upfront, check the bot's insert payloads in
`src/execution/mod.rs` and `src/exit/mod.rs`.

---

## Step 4 — Code cleanup (remove shadow modules)

The shadow modules are observe-only data collectors for strategies we're NOT running in this repo.
Remove them to keep the codebase focused on BC only.

### 4a. Delete the `src/shadow/` directory entirely

All 8 files:
- `src/shadow/mod.rs`
- `src/shadow/copy_trader.rs`
- `src/shadow/cto_watcher.rs`
- `src/shadow/dip_watcher.rs`
- `src/shadow/meta_tracker.rs`
- `src/shadow/price_updater.rs`
- `src/shadow/raydium_direct.rs`
- `src/shadow/volume_scanner.rs`

### 4b. Remove shadow references from `src/main.rs`

1. Remove `mod shadow;` declaration (near the top with the other `mod` lines).
2. Remove the `shadow::start(...)` call (around step 6c in main, before detection starts).

### 4c. Remove shadow call-sites from `src/sniper/mod.rs`

There are 3 shadow calls to remove (search for `crate::shadow`):
1. `crate::shadow::meta_tracker::log_token_category(...)` — remove the entire `tokio::spawn` block.
2. `crate::shadow::raydium_direct::log_direct_launch(...)` — remove the entire `tokio::spawn` block.
3. `crate::shadow::cto_watcher::maybe_add_to_watchlist(...)` — remove the entire `tokio::spawn` block.

Each call is inside a `// ── Shadow: ... ──` marked block. Remove the block, keep surrounding code.

### 4d. Remove shadow call-sites from `src/execution/mod.rs`

There are 2 identical calls to remove (search for `crate::shadow`):
1. Paper-trade success branch: `crate::shadow::dip_watcher::add_bought_token(...)` — remove the `tokio::spawn` block.
2. Real-trade success branch: same call — remove the `tokio::spawn` block.

Both are inside `// ── Shadow: add bought token to dip watchlist ──` markers.

### 4e. Delete scripts (optional — they don't affect the bot)

The `scripts/` directory contains Python analysis scripts from the research repo. You can keep
them for reference or delete them. They are never called by the Rust bot.

Files safe to delete:
- All `scripts/analyze_*.py`
- All `scripts/discover_*.py`, `scripts/reverse_*.py`, `scripts/rotate_*.py`
- `scripts/seed_tracked_wallets_from_legacy.py`
- `scripts/simulate_copytrade_exits.py`
- `export_supabase.py`

**Keep these** (useful for BC research):
- `scripts/analyze_bc_signals.py` — BC signal analysis
- `scripts/backfill_bc_gate_pnl.py` — pulls Birdeye price history and simulates TP/SL
- `scripts/analyze_bc_gate_pnl.py` — PnL report from backfill data
- `scripts/_audit_recent.py` — quick audit of recent positions

### 4f. Delete docs not relevant to BC research

Safe to delete:
- `SHADOW_OBSERVATION.md`
- `PHASE3_DATA_ANALYSIS.md`
- `SNIPER_PLAN.md`
- `docs/strategies/copy_trader.md`
- `docs/strategies/cto_watchlist.md`
- `docs/strategies/dip_watchlist.md`
- `docs/strategies/narrative.md`
- `docs/strategies/raydium_direct.md`
- `docs/strategies/volume_spike.md`
- `docs/next-codebase-plan.md`
- `data/` directory

**Keep:**
- `ARCHITECTURE.md` — explains the pipeline
- `VERSION.md` — version history
- `docs/strategies/bonding_curve.md` — the core research doc
- `docs/strategies/README.md` — index (update after deleting others)

---

## Step 5 — config.toml tuning for BC-focused paper trading

Key changes to make in `config.toml`:

```toml
# Bump the version to mark this as the BC research fork
strategy_version = "bc-research-v1"

[detection]
method = "pumpfun_ws"
poll_raydium = false              # Only care about PumpFun graduations
bonding_curve_signals_enabled = true
bc_signal_volume_threshold = 50.0

[filters]
# ── BC Gate (the hypothesis we're testing) ──
reject_creator_rebuy = true       # KEEP — 0.43x lift when rebuy=true
min_buy_sell_ratio = 2.0          # TIGHTEN from 1.2 → 2.0 (the gate threshold)
max_bc_sell_count = 40            # KEEP
min_unique_buyers = 40            # TIGHTEN from 8 → 40 (the gate threshold)

# Everything else can stay at current values — they're safety filters,
# not part of the BC gate hypothesis. Don't change them.

[execution]
buy_amount_sol = 0.05             # Paper-trade size (doesn't spend real SOL)
paper_slippage_bps = 250          # Simulate realistic slippage
paper_exit_delay_ms = 2000        # Simulate execution delay on exits

[monitoring]
# Disable narrative moonbag if you don't have an OpenAI key
narrative_check_enabled = false
```

**Important filter changes explained:**
- `min_buy_sell_ratio = 2.0` — this is the core BC gate finding. BSR ≥ 2 showed a 2.30x lift
  on graduation rate (13.3% vs 5.79% base). Original config was 1.2.
- `min_unique_buyers = 40` — the gate requires ≥ 40 unique buyers. Original was 8.
- `reject_creator_rebuy = true` — already set, creator rebuy drops grad rate to 2.5%.

With these three filter changes, the bot will only paper-trade tokens that match the BC gate.
Everything else gets rejected and logged to `sniper_candidates` for analysis.

---

## Step 6 — Build and run

### Prerequisites
- Rust toolchain (stable, recent — the bot uses async/await heavily)
- Windows: the `vendor/protobuf-src-stub/` handles protobuf build issues

### Build
```powershell
cargo build --release
```

### Run
```powershell
# Make sure .env is in the project root
cargo run --release
```

Or on a server:
```bash
# Build
cargo build --release

# Run (with .env in same directory as the binary)
./target/release/solana-memecoin-bot
```

### What to expect

On startup you'll see:
```
[INFO] Config loaded: strategy_version = "bc-research-v1"
[INFO] Paper-trade mode: ON
[INFO] Connecting to PumpFun WebSocket...
[INFO] Connected to PumpFun WebSocket
[INFO] Subscribed to PumpFun newToken + migration
```

Then a stream of tokens being detected, enriched, filtered:
```
[INFO] NEW_TOKEN: SYMBOL (mint: Abc123...) — subscribing to trades
[INFO] GRADUATION: SYMBOL migrated to Raydium
[INFO] ENRICHMENT: SYMBOL — 12 features collected in 1.8s
[INFO] REJECTED: SYMBOL — buy_sell_ratio=1.3 < 2.0  ← didn't pass BC gate
[INFO] BOUGHT (paper): SYMBOL at $0.00012 — position_id=42
[INFO] EXIT (paper): SYMBOL — tp1 hit at 1.82x, sold 25%
```

---

## Step 7 — Analyze results

After running for 24–72 hours, you'll have paper-trade data in your new Supabase.

### Quick audit
```powershell
python scripts/_audit_recent.py
```
(Update the Supabase URL/key in the script to point to the new project.)

### Key tables to query

```sql
-- How many tokens passed the BC gate?
SELECT COUNT(*) FROM sniper_candidates WHERE action = 'bought';

-- How many were rejected and why?
SELECT rejection_reason, COUNT(*)
FROM sniper_candidates
WHERE action = 'rejected'
GROUP BY rejection_reason
ORDER BY count DESC
LIMIT 20;

-- Paper-trade PnL summary
SELECT
  COUNT(*) as trades,
  AVG(pnl_pct) as avg_pnl,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pnl_pct) as median_pnl,
  SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as win_rate,
  SUM(pnl_sol) as total_pnl_sol
FROM positions
WHERE strategy_version = 'bc-research-v1';

-- Exit reason distribution
SELECT exit_reason, COUNT(*), AVG(pnl_pct)
FROM positions
WHERE strategy_version = 'bc-research-v1'
GROUP BY exit_reason
ORDER BY count DESC;

-- Best and worst trades
SELECT symbol, mint, pnl_pct, peak_multiplier, exit_reason, hold_duration_secs
FROM positions
WHERE strategy_version = 'bc-research-v1'
ORDER BY pnl_pct DESC
LIMIT 10;
```

### BC gate backfill (from historical data)

If you also want to backtest the gate against existing `bonding_curve_signals` data:

```powershell
# Update SUPA_URL and SUPA_KEY in the script to your NEW Supabase
python scripts/backfill_bc_gate_pnl.py --only-graduated --limit 10  # smoke test
python scripts/backfill_bc_gate_pnl.py                              # full run
python scripts/analyze_bc_gate_pnl.py                               # report
```

---

## File map — what's essential

```
src/
  main.rs               ← startup (MODIFY: remove shadow references)
  config.rs              ← config structs
  detection/
    mod.rs               ← detection orchestrator
    pumpfun_ws.rs        ← PRIMARY: PumpFun WebSocket + BC signal recording
    raydium_poller.rs    ← secondary (disabled via POLL_RAYDIUM=false)
    st_search_poller.rs  ← backup (disabled if no ST API key)
    types.rs             ← GraduatedToken, DetectionSource
  sniper/
    mod.rs               ← enrichment pipeline (MODIFY: remove shadow calls)
    enrichment.rs        ← parallel API enrichment
    features.rs          ← feature extraction
    filters.rs           ← hard filters (BC gate lives here)
    scoring.rs           ← sniper score
    types.rs             ← pipeline types
    birdeye.rs           ← Birdeye client
    solana_tracker.rs    ← ST client
    tracker.rs           ← rejected-token tracker
    post_trade.rs        ← post-trade snapshots
  filters/
    mod.rs + 15 files    ← concurrent filter engine (DO NOT MODIFY)
  execution/
    mod.rs               ← buy engine, paper + live (MODIFY: remove shadow calls)
    jupiter.rs           ← Jupiter swap client
    jito_client.rs       ← Jito bundles (unused in paper mode)
    helius_sender.rs     ← Helius sender (unused in paper mode)
    state.rs             ← in-memory position state
    types.rs             ← trade types
    wallet.rs            ← wallet management
  monitoring/
    mod.rs + 10 files    ← price monitoring, exit triggers, moonbag
  exit/
    mod.rs               ← exit engine (sell execution)
    types.rs, error.rs, dedup.rs
  logger/
    mod.rs, types.rs     ← Supabase REST client
  narrative/
    mod.rs               ← OpenAI narrative scoring (optional)
  shadow/                ← DELETE ENTIRE DIRECTORY
config.toml              ← strategy params (MODIFY: tighten BC gate filters)
Cargo.toml               ← build config (no changes needed)
.env                     ← CREATE NEW (see Step 2)
migrations/              ← SQL migrations (run in new Supabase)
scripts/
  analyze_bc_signals.py       ← KEEP
  backfill_bc_gate_pnl.py     ← KEEP
  analyze_bc_gate_pnl.py      ← KEEP
  _audit_recent.py            ← KEEP
  (everything else)           ← DELETE or ignore
```

---

## Success criteria

After 48–72h of paper trading with the BC gate filters:

| Metric | Target | Meaning |
|---|---|---|
| **Win rate** | > 30% | Better than the 23% blind-sniper baseline |
| **Mean PnL per trade** | > 0% | Gate produces positive expectancy |
| **Median peak multiplier** | > 1.5x | Tokens pump enough for TP1 to hit |
| **TP1 hit rate** | > 25% | At least 1 in 4 trades reaches +80% |
| **Trades per day** | 3–15 | Gate isn't too tight (0 trades) or too loose (50+) |

If these hold, the gate is validated for real-money trading with small positions.

---

## Important safety notes

1. **PAPER_TRADE=true is non-negotiable** until the data proves the gate works.
2. The wallet private key still matters — the bot uses it for signing price-quote
   requests. Use a wallet with minimal SOL (0.01 is enough for paper mode).
3. Helius RPC calls are shared with the original bot. Monitor your usage at
   dashboard.helius.dev — the Developer plan allows 50 RPS standard.
4. `bonding_curve_signals` will accumulate in both repos (original + this one) since
   both connect to the same PumpFun WS. That's fine — they write to different Supabase projects.
5. **Never change `PAPER_TRADE` to `false`** in this repo without explicit review. This is a
   research instance, not a trading instance.
