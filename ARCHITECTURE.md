# Solana Meme Coin Trading Bot — Full Architecture

> End-to-end automated trading system for Solana meme coins. Covers token discovery through bonding curve graduation, parallel enrichment, multi-stage filtering, dynamic execution, real-time monitoring, and narrative-driven moonbag exits.

---

## Table of Contents

1. [High-Level Pipeline](#1-high-level-pipeline)
2. [Token Discovery](#2-token-discovery)
3. [Pre-Check Cache (Phase 0)](#3-pre-check-cache-phase-0)
4. [Sniper Enrichment](#4-sniper-enrichment)
5. [Hard Filters (Sniper Gate)](#5-hard-filters-sniper-gate)
6. [Filter Engine (Fast Gate)](#6-filter-engine-fast-gate)
7. [Pre-Execution Safety Checks](#7-pre-execution-safety-checks)
8. [Execution (Buy)](#8-execution-buy)
9. [Post-Buy Verification (Slow Gate)](#9-post-buy-verification-slow-gate)
10. [Position Monitoring](#10-position-monitoring)
11. [Exit Strategy](#11-exit-strategy)
12. [Narrative Moonbag System](#12-narrative-moonbag-system)
13. [Exit Execution (Sell)](#13-exit-execution-sell)
14. [Safety Rules & Protections](#14-safety-rules--protections)
15. [APIs & Services Summary](#15-apis--services-summary)
16. [Database (Supabase)](#16-database-supabase)
17. [Configuration Reference](#17-configuration-reference)

---

## 1. High-Level Pipeline

```
Token Discovery (3 sources)
    │
    ▼
Pre-Check Cache (bonding curve phase)
    │
    ▼
Graduation Event (token migrates to Raydium/pump-AMM)
    │
    ▼
Sniper Enrichment (9 parallel API calls, 2s budget)
    │
    ▼
Hard Filters (5 critical checks — reject unsafe tokens)
    │
    ▼
Fast Gate (5 instant checks — reject weak tokens)
    │
    ▼
Pre-Execution Safety (wallet balance, exposure, daily loss)
    │
    ▼
Execution (Jupiter swap via Helius Sender or Jito)
    │
    ▼
Post-Buy Verification (background slow gate — emergency exit if danger found)
    │
    ├──▶ Position Monitoring (price polling, dev wallet watch, dip state machine)
    │        │
    │        ├── StopLoss / TrailingStop / TimeStop → Exit 100%
    │        ├── TP1 (1.8x, sell 25%) → check narrative → moonbag promotion
    │        ├── TP2 (4.0x, sell 50% of remaining)
    │        └── TP3 (8.0x, sell 100%)
    │
    └──▶ Narrative Moonbag Tracker (independent long-hold with age-based trailing)
             │
             └── Age-based trailing decay → eventual exit
```

---

## 2. Token Discovery

Three independent detection sources run simultaneously. A token must **graduate** from the bonding curve to enter the pipeline.

### 2a. PumpFun WebSocket (Primary)

| Detail | Value |
|--------|-------|
| **Endpoint** | `wss://pumpportal.fun/api/data` |
| **Cost** | Free |
| **Latency** | Real-time (sub-second) |

**How it works:**

1. Subscribes to `newToken` events — token creation on the bonding curve (no sale yet).
2. Subscribes to `tokenTrade` per mint — every buy/sell on the bonding curve.
3. Subscribes to `migration` (also called `tokenComplete`) — token graduates to Raydium/pump-AMM.

**In-memory watchlist** tracks up to 5,000 tokens simultaneously:
- Mint address, creator wallet, detection timestamp
- Total volume (SOL), buy count, sell count, unique buyers
- All trade timestamps (for wash-trade detection later)
- Buy pressure: `buy_count / (buy_count + sell_count) × 100`

**On graduation**, the system:
1. Extracts pool address from the graduation payload.
2. If pool not in payload, resolves it from the migration transaction via RPC (reads accounts array, matches Raydium AMM v4 or pump-AMM program IDs).
3. If that fails, falls back to DexScreener API to find the pool.
4. If no pool found at all, proceeds anyway with estimated liquidity (85 SOL default from bonding curve).
5. Fetches initial pool SOL balance via RPC. If rent-only (< 1 SOL), uses 85 SOL fallback.
6. Fetches historical trades from pump.fun API: `https://frontend-api.pump.fun/trades/all/{mint}?limit=200`
7. Merges historical trades with WebSocket-observed trades for complete picture.

Emits a `GraduatedToken` event containing: mint, pool address, creator wallet, bonding curve volume, buy pressure %, time to graduate, unique buyer count, buy/sell counts, trade timestamps, name, symbol, initial liquidity estimate, and pipeline timing data.

### 2b. Raydium logsSubscribe (Secondary)

| Detail | Value |
|--------|-------|
| **Endpoint** | Solana RPC WebSocket (your configured `SOLANA_WS_URL`) |
| **Method** | `logsSubscribe` |
| **Program** | Raydium AMM v4: `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` |
| **Cost** | Free (uses your RPC plan) |

Watches for `initialize2` / `InitializeInstruction2` log messages indicating a new pool being created. Extracts pool address from logs. This catches direct Raydium launches that bypass pump.fun entirely.

### 2c. SolanaTracker Search Polling (Tertiary)

| Detail | Value |
|--------|-------|
| **Endpoint** | SolanaTracker `/search` API (private, key-based) |
| **Poll interval** | Every 20 seconds |
| **Cost** | ~130k requests/month |

Polls for recently graduated tokens matching: market cap < $5,000, top holder < 65%, dev hold < 30%. Maintains a 5,000-entry seen-set to avoid re-processing. Acts as a safety net for tokens missed by the other two sources.

---

## 3. Pre-Check Cache (Phase 0)

While a token is still on the bonding curve (before graduation), the system can optionally pre-fetch safety data so the fast gate doesn't have to wait for it.

**Activation thresholds** (all must be met):
- Unique buyers ≥ 3
- Buy pressure ≥ 50%
- Total bonding volume ≥ 0.5 SOL

**Pre-checks run:**
- **Token Safety** — checks mint authority and freeze authority via RPC
- **GoPlus** — honeypot, mintable, transfer pausable checks
- **RugCheck** — risk score and bundled launch detection

Results are cached for 5 minutes (max 500 entries). When the token graduates, these cached results are used immediately instead of re-fetching.

---

## 4. Sniper Enrichment

Once a token graduates, 9 API calls fire **in parallel** with a **2-second deadline**. Any call that doesn't complete in time is abandoned — the system proceeds with partial data.

| # | API Call | Endpoint | What It Returns |
|---|---------|----------|------------------|
| 1 | Birdeye Token Meta | `https://api.birdeye.so/v1/token/{mint}` | Decimals, image, metadata |
| 2 | Birdeye Token Security | `https://api.birdeye.so/v1/token/{mint}/security` | isMintable, isFrozen, canTransfer |
| 3 | Helius DAS `getAsset` | Helius RPC (JSON-RPC) | Mint/freeze authority status |
| 4 | RugCheck Report | `https://api.rugcheck.xyz/v1/tokens/{mint}/report` | Risk score, top holders, bundled flag, LP locked |
| 5 | GoPlus Security | `https://api.gopluslabs.io/api/v1/solana/token_security/{mint}` | Honeypot, mintable, pausable, blacklist, reclaim ownership |
| 6 | SolanaTracker Market | SolanaTracker `/token/{mint}/market` | Holder concentration, real-time price, volume |
| 7 | SolanaTracker Trades | SolanaTracker `/token/{mint}` | Recent buys/sells for wash-trade detection |
| 8 | Jupiter SOL Price | `https://price.jup.ag/v4/price?ids=SOL` | SOL/USD price (fallback: Coinbase, then DexScreener) |
| 9 | DexScreener | `https://api.dexscreener.com/latest/dex/tokens/{mint}` | priceUsd, FDV, liquidity, 24h volume |

**Output:** An `Enrichment` struct containing liquidity, price, market cap, holder concentration, dev hold %, mint/freeze authority status, honeypot flag, mintable flag, bundled flag, RugCheck score, and per-source timing.

---

## 5. Hard Filters (Sniper Gate)

5 critical checks — **all must pass** or the token is rejected. These run on the enrichment data (no additional API calls).

| # | Check | Reject If |
|---|-------|-----------|
| 1 | Mint Authority | Not revoked (owner can mint new tokens) |
| 2 | Freeze Authority | Not revoked (owner can freeze wallets) |
| 3 | Honeypot | Detected by RugCheck, Birdeye, or GoPlus |
| 4 | Supply Integrity | Token is mintable (supply not fixed) |
| 5 | Bundled Launch | Detected AND `reject_bundled = true` |

Tokens that pass are logged to the `sniper_candidates` Supabase table with full enrichment data as JSONB. Rejected tokens are also logged with the rejection reason.

---

## 6. Filter Engine (Fast Gate)

5 checks run on graduated tokens. These are the main quality/momentum filters.

### 6a. Sanity Check
- Rejects if bonding curve volume < 0.5 SOL (structurally broken data).

### 6b. Age Filter
- Rejects if token age > 600 seconds (10 min) since first detection.
- Rejects if time to graduate > 7,200 seconds (2 hours).
- Reasoning: Old tokens have usually already peaked or died.

### 6c. Buy Pressure
- Rejects if buy pressure < 52%.
- Rejects if bonding volume < 5 SOL.
- Rejects if unique buyers < 12.
- **Wash-trade detection:** Rejects if > 10 new wallets buy within the same 3-second window (`coordinated_window_ms = 3000`, `coordinated_buy_threshold = 10`).

### 6d. Liquidity Filter
- Fetches SOL/USD price from Jupiter (cached 10 seconds), fallback to Coinbase API.
- Calculates: `liquidity_usd = pool_reserve_sol × 2 × sol_price_usd`
- Rejects if liquidity < $10,000.
- For pump-AMM pools where on-chain data isn't available yet, uses the estimated liquidity from detection.

### 6e. Price Impact Filter
- Estimates: `impact_pct = (buy_amount_sol / pool_reserve_sol) × 100`
- Rejects if impact > 5%.

**All pass** → forward to execution.
**Any fail** → log to `filter_results` table + spawn a rejected token price tracker (tracks price at 1m, 5m, 15m, 1h for counterfactual analysis).

---

## 7. Pre-Execution Safety Checks

Before placing a buy, these in-memory checks must all pass:

| Check | Threshold | Source |
|-------|-----------|--------|
| Open position count | < 5 | In-memory TradingState |
| Portfolio exposure | Current deployed + buy amount ≤ 0.6 SOL | In-memory |
| Daily P&L | Today's P&L > -1.0 SOL | In-memory |
| Wallet SOL balance | ≥ 0.2 SOL | Live RPC call |
| Duplicate mint | Only 1 position per mint | In-memory dedup |
| Dev blacklist | Creator wallet not blacklisted | In-memory (blacklisted if previously dumped) |

### Dynamic Position Sizing

Buy size scales with pool liquidity:
- ≤ 30 SOL liquidity → buy 0.04 SOL (minimum)
- ≥ 80 SOL liquidity → buy 0.10 SOL (maximum)
- Between → linear interpolation

### Anti-Chase Check

Compares current price to price at filter time:
- If price moved > 50% in either direction → skip trade.
- Prevents chasing tokens that already spiked during the enrichment/filter pipeline.

---

## 8. Execution (Buy)

### Paper Trade Mode

1. Fetch current price from DexScreener (no Jupiter swap needed).
2. Estimate tokens received: `tokens = (buy_sol × sol_usd_price) / token_price`
3. Apply simulated slippage penalty: `entry_price *= (1 + 250 bps / 10000)` — 2.5% worse entry.
4. Write position to Supabase with `status: "paper"`.

### Real Trade Mode

**Step 1 — Jupiter Quote:**
- Endpoint: `https://api.jup.ag/swap/v1/quote`
- Parameters: input = SOL, output = token mint, amount in lamports, slippage = 1500 bps (15%)
- Retries up to 5 times with 1-second delays if `NO_ROUTES_FOUND` (route may not exist immediately post-graduation).

**Step 2 — Jupiter Swap Transaction:**
- Endpoint: `https://api.jup.ag/swap/v1/swap`
- Generates a serialized transaction with dynamic compute unit limits.
- Priority fee: Jupiter's built-in `prioritizationFeeLamports` at level `veryHigh` (capped at 500,000 lamports = 0.0005 SOL).

**Step 3 — Transaction Submission (one of):**

| Method | When | How |
|--------|------|-----|
| **Helius Sender** | `USE_HELIUS_SENDER=true` | Injects tip transfer into the swap tx, sends to `https://sender.helius-rpc.com/fast`. Tip: 0.0005 SOL. |
| **Jito Bundle** | `USE_JITO=true` | Fetches tip floor from Jito API, multiplies by 3x, caps at 0.02 SOL. Bundles [tip_tx, swap_tx] and submits to Jito block engine. |
| **Standard RPC** | Neither enabled | Sends signed tx to configured Solana RPC. |

**Step 4 — Confirmation:**
- Polls RPC for signature status every 200ms.
- Timeout: 25 seconds.
- Commitment level: `confirmed`.

**Step 5 — Record Position:**
- Writes to Supabase `positions` table with: mint, entry price, SOL spent, tokens received, pool address, dev wallet, detection source, detection latency, entry hour UTC, concurrent position count.
- Returns `PositionOpened` event → sent to monitoring engine.

---

## 9. Post-Buy Verification (Slow Gate)

Runs **in the background** immediately after buy execution. No time pressure — these are thorough checks. If any finds critical danger, it triggers an **emergency exit** (sell 100% immediately).

| Check | API | Critical Danger Condition |
|-------|-----|--------------------------|
| RugCheck | `https://api.rugcheck.xyz/v1/tokens/{mint}/report` | Score > 10,000 OR authority not revoked |
| GoPlus | `https://api.gopluslabs.io/api/v1/solana/token_security/{mint}` | Honeypot, mintable, pausable, blacklist, or reclaim ownership |
| Holders | RPC `getTokenLargestAccounts` | Top 10 wallets hold > 65% combined |
| Market Cap | Token supply × DexScreener price | Outside $5k–$150k range |
| Narrative | OpenAI Responses API (see Section 12) | Not a danger check — stores initial narrative state |

If critical danger is found, a `PostBuyAlert` is sent to the monitoring engine which triggers an immediate 100% exit.

---

## 10. Position Monitoring

Each open position gets its own monitoring loop running every 2 seconds.

### 10a. Price Fetching

Three sources tried in priority order:

| Priority | Source | Endpoint | Timeout |
|----------|--------|----------|---------|
| 1 | Birdeye | `https://api.birdeye.so/v1/defi/price` | 3s |
| 2 | Jupiter | `https://price.jup.ag/v4/price?ids={mint}` | 5s cache |
| 3 | DexScreener | `https://api.dexscreener.com/latest/dex/tokens/{mint}` | last resort |

**Sanity checks on every price tick:**
- Must be > 0 and < $1,000,000
- Single-tick change ratio must be < 10x (reject as invalid data)
- 120+ consecutive zero prices (60 seconds worth) → emergency exit

### 10b. Dev Wallet Monitoring & CTO Detection

Runs in parallel with price monitoring when `HELIUS_WS_URL` is configured.

**Two streams watched:**

1. **Dev token account balance** — checked every 10 seconds via RPC.
   - **Partial dump (balance drops ≥ 15% but not to zero):** Emergency exit + blacklist dev wallet.
   - **100% sell (balance drops to exactly 0):** Treated as a **CTO (Community Takeover)** — a bullish signal. See 10e below.

2. **Pool SOL vault balance** — monitors the pool's SOL reserve.
   - If SOL drops ≥ 15% → emergency exit (LP being drained).

### 10c. Dip State Machine (Tick Stream)

Monitors pool vault token/SOL balance changes in a rolling 60-second window.

**States:**
- **Normal** — no unusual activity.
- **DIP_WATCH** — price dropped 15% from peak. Grace period: 25 seconds.
- **DIP_DEATH** — no recovery after 30 seconds in DIP_WATCH, or single whale sell > 3× average trade size with no buy follow-up.

**Recovery condition** (cancels dip_death): buy volume ratio ≥ 55% within the 60-second window with minimum 0.05 SOL volume.

### 10d. SolanaTracker Trade Snapshots

Every 15 seconds per active position, polls SolanaTracker for recent trades and logs a snapshot to the `st_trade_snapshots` table:
- Buy count, sell count, total buy/sell SOL
- Average and max trade sizes for buys and sells
- Unique buyers/sellers
- Detected patterns (whale buys, coordinated sells, etc.)

### 10e. CTO (Community Takeover) Detection

When the dev wallet balance hits exactly 0 tokens, the system evaluates whether this is a genuine Community Takeover (bullish) or a noisy non-event.

**CTO qualification gate:**
- Dev must have held ≥ 3% of circulating supply (from enrichment data: SolanaTracker `st_dev_pct` or Birdeye `be_owner_balance_pct`).
- If dev held < 3% and sold to 0, it’s noise — ignored silently (no exit, no blacklist).
- Dev wallet is **NOT blacklisted** regardless of outcome.

**Staged evaluation (15s / 45s / 90s checkpoints):**

Memecoins react fast, so the system checks at three escalating intervals instead of a single wait:

| Stage | Time | Purpose |
|-------|------|-------|
| 1 | 15s | Detect immediate panic or fast absorption |
| 2 | 45s | Check sustained recovery or continued bleed |
| 3 | 90s | Final verdict — grade the CTO |

At each checkpoint, the system logs: recovery %, momentum ratio, and stage number.

**Fail-fast rule:** If price drops below 60% of pre-CTO level at **any** point during the evaluation (not just at checkpoints), the system immediately exits. This catches violent collapses without waiting for the next stage.

**Tiered grading (at final 90s stage):**

| Grade | Recovery % | Narrative Boost | Effect |
|-------|-----------|----------------|--------|
| Strong CTO | ≥ 85% | `RunnerConfirmed` | +30 narrative bonus, widest moonbag trail |
| Moderate CTO | 70–85% | `ExpandingAttention` | +20 narrative bonus |
| Failed CTO | < 70% | None | Emergency exit |

**Data used at each stage:**
- Price recovery: `current_price / cto_pre_price × 100`
- Momentum ratio: from tick stream (`last_momentum.momentum_ratio`)

All CTO events are logged to the `system_events` Supabase table: `cto_detected`, `cto_stage_1/2/3`, `cto_strong`, `cto_moderate`, `cto_failed`.

**Narrative prompt integration:** The OpenAI narrative prompt explicitly searches for "{name} CTO" and "{name} community takeover" terms, and is instructed to score CTO tokens at 51+ (ExpandingAttention or higher).

---

## 11. Exit Strategy

Exit triggers are evaluated every 2-second poll. **First match wins** — evaluated in this priority order:

### 11a. TimeStop (Highest Priority)
- **Trigger:** Position held ≥ 900 seconds (15 min).
- **Action:** Sell 100%.
- **Reasoning:** Sniped tokens that don't move in 15 minutes are dead.

### 11b. Post-Fill Sanity (First 10 Seconds)
- **Trigger:** Current price < 50% of entry price.
- **Action:** Sell 100%.
- **Reasoning:** Bad fill or token already rugging.

### 11c. Stop Loss (Two-Tier)

| Tier | Condition | Threshold | Grace Period |
|------|-----------|-----------|--------------|
| Never Profitable | Peak never exceeded entry price | -25% from entry | 20 seconds |
| Normal | Standard drawdown | -35% from entry | 5 seconds |

### 11d. Trailing Stop (Drawdown from Peak)

Drawdown calculation: `(peak_price - current_price) / peak_price × 100`

| Condition | Trailing % | Detail |
|-----------|-----------|--------|
| Base | 30% | Default drawdown tolerance from peak |
| Adaptive (peak ≥ 3x) | 18% | Tightens to lock gains on runners |
| Post-TP1 | 22% | After first profit take, with entry floor enforcement |
| Post-TP2 | 30% | Wider to let moonbag portion ride |

### 11e. Take Profit Levels

| Level | Entry Multiple | Sell Percentage | Remaining After |
|-------|---------------|-----------------|-----------------|
| TP1 | 1.8× | 25% | 75% |
| TP2 | 4.0× (300% gain) | 50% of remaining | 37.5% of original |
| TP3 | 8.0× (700% gain) | 100% of remaining | 0% |

**After TP1:** If combined score (on-chain strength + narrative bonus) ≥ 60, the remaining 75% is promoted to the moonbag tracker. The position slot is **freed** so new trades can open.

### 11f. Low-Liquidity Overrides

When initial pool liquidity < 50 SOL, tighter rules apply:
- Trailing stop: 22% (instead of 30%)
- Max hold: 600 seconds (instead of 900)
- Stop loss: 30% (instead of 35%)

---

## 12. Narrative Moonbag System

The moonbag system uses a **3-tier approach** (KOL match → OpenAI without web search → OpenAI with web search) to detect tokens with social/viral momentum and hold them beyond the normal 15-minute window. DexScreener and Birdeye data are always fetched first (free / already paid).

### 12a. Narrative Checks During Monitoring

Narrative checks run at scheduled intervals during the monitoring phase:
- T+120 seconds (2 minutes)
- T+240 seconds (4 minutes)
- T+360 seconds (6 minutes)

**Cost-reduction gates (checked BEFORE each scheduled check):**
1. **Stop after strong answer:** If narrative state already reached `ExpandingAttention` or higher, no further checks are scheduled. The score we have is sufficient for promotion.
2. **Price gate:** Current price must be ≥ 1.2× entry. A token that's flat or down at check time won't promote.
3. **Momentum gate:** Momentum ratio must be ≥ 0.5 (more buying than selling). Dumping tokens are skipped.

If either gate fails, the check is skipped (no OpenAI call), the check index advances, and the system logs `Narrative check SKIPPED — price/momentum gate`.

Each check has a 35-second timeout and runs as a background task (doesn't block price monitoring).

### 12b. How a Narrative Check Works — 3-Tier System

**Step 1 — Fetch DexScreener + Birdeye in parallel (free / already paid):**
- DexScreener: `https://api.dexscreener.com/latest/dex/tokens/{mint}` — extracts 24h volume, transactions, FDV, liquidity, price changes, boosts, **website URLs, social URLs**.
- Birdeye: `https://public-api.birdeye.so/defi/token_overview?address={mint}` — extracts social links (Twitter, Telegram, website from token metadata), unique wallets 24h, holder count, buy/sell 24h counts.

**Step 2 — Dead token pre-filter (DexScreener + Birdeye):**
If DexScreener shows zero life (volume=0, makers=0, no boosts, no social links) AND Birdeye is dead (no social links, <5 unique wallets, <10 holders), return `NoSignal` with score 0 immediately. **No OpenAI call made.** This saves $0.025 per dead token.

**Step 3 — Extract Twitter usernames and determine tier:**
All social URLs from DexScreener (`info.websites`, `info.socials`) and Birdeye (`twitter_url`) are parsed to extract Twitter/X usernames. These are matched against a configurable KOL list (`config.toml` → `[[monitoring.kol_wallets]]`).

| Tier | Condition | Action | Cost |
|------|-----------|--------|------|
| **1 — KOL Match** | Twitter username matches a KOL in the list | Instant score from `min_score` config. Optional oEmbed verification for display name. **No OpenAI call.** | **$0.00** |
| **2 — Social Links, No KOL** | DexScreener/Birdeye has social links but no KOL username match | Call OpenAI **without** `web_search_preview` tool. All social context provided in prompt. | **~$0.001** |
| **3 — No Social Links** | No social URLs from DexScreener or Birdeye | Call OpenAI **with** `web_search_preview` tool (current behavior). | **~$0.025** |

**KOL List (in `config.toml`):**
- **mega** tier (min_score 75–80): nikitabier, blknoiz06, MustStopMurad, crashiusclay69, ansaboriqua, ansem
- **major** tier (min_score 70–75): CryptoGodJohn, Tradermayne, DegenSpartan, notthreadguy, soljakey, 0xSun_crypto
- **notable** tier (min_score 65): Rewkang, cryptowizardd, eugenefinance

**Tier 1 KOL match uses Twitter oEmbed** (`https://publish.twitter.com/oembed?url=...`) — free, no API key — to verify the author display name. This is logged but doesn't affect the score.

**Step 4 — Build prompt (Tiers 2 & 3):**
- Includes on-chain metrics: buy/sell counts, buy/sell volume (SOL), momentum ratio, peak multiplier, hold duration.
- Includes all DexScreener market data: 24h volume, transactions, FDV, liquidity, price changes, boosts.
- **Includes Birdeye data** (when available): unique wallets 24h, holder count, 24h buy/sell counts, verified social links.
- **Tier 2 adds:** All social links as context in the prompt, instructs OpenAI to evaluate link quality without searching the web.
- **Tier 3 adds:** Web search instructions telling OpenAI to search for the mint address, name, symbol, and CTO status.
- **The prompt asks for a single holistic score (0–100) that weighs on-chain flow AND social/narrative strength together.**

**Step 5 — Call OpenAI Responses API (Tiers 2 & 3 only):**
- Endpoint: `https://api.openai.com/v1/responses`
- Model: `gpt-4o-mini`
- Tools: Empty array (Tier 2) or `[{ "type": "web_search_preview" }]` (Tier 3)

**Step 6 — Parse response:**
- **Score:** 0–100
- **Narrative State:**
  - `NoSignal` (0–25): No social or narrative presence.
  - `EarlyAttention` (26–50): Some early mentions, minor CT activity.
  - `ExpandingAttention` (51–75): Growing social presence, multiple sources.
  - `RunnerConfirmed` (76–100): Viral narrative, strong multi-platform presence.
- **Narrative Strength:** none / weak / moderate / strong / viral
- **Market Strength:** dying / weak / moderate / strong / explosive
- **Reasons:** Array of supporting evidence strings.
- **Risk Flags:** Array of concerns found.
- **Web Sources Found:** Count of web results the model found.

**Cost impact estimate:** ~90% reduction from $2.27/day to ~$0.05–0.15/day. Most checks will be Tier 1 (free KOL match) or dead-token pre-filter. Remaining checks use Tier 2 ($0.001) unless no social links exist.

**Data persistence:** Every completed narrative check PATCHes the `positions` row with:
- `narrative_state` — latest state string
- `narrative_score` — latest 0–100 score
- `narrative_result` — full output as JSONB (score, state, narrative_strength, market_strength, reasons, risk_flags, web_sources_found)

During moonbag tracking, each narrative re-check UPDATEs `moonbag_positions.narrative_result` with the latest output.

### 12c. Moonbag Promotion (OpenAI Holistic Score)

A position is promoted to a moonbag when **both conditions** are met:
1. TP1 has been triggered (price reached 1.8× entry).
2. **OpenAI holistic score ≥ 60** (configurable `moonbag_promotion_min_score`).

**How it works:** The latest OpenAI narrative check score (from the scheduled checks at T+120, T+240, T+360) is used directly as the promotion score. No manual on-chain calculation is needed — OpenAI already receives all on-chain metrics (buy/sell counts, buy/sell volume SOL, momentum ratio, peak multiplier) alongside DexScreener market data, Birdeye social data, and web search results, and makes a single holistic decision.

**Scoring principles baked into the prompt:**

| Scenario | Expected Score Range |
|----------|---------------------|
| Both on-chain and social are weak | 0–25 (NO_SIGNAL) |
| Some social mentions OR decent flow, not both | 26–50 (EARLY_ATTENTION) |
| Strong social buzz with weak on-chain (hype hasn't translated to buys *yet*) | 40–65 |
| Strong on-chain with no social (organic demand, market voting with real money) | 50–75 |
| Multiple social sources + solid flow, OR one side very strong | 51–75 (EXPANDING_ATTENTION) |
| CTO tokens with active communities | 55+ |
| Both strong: viral narrative + explosive flow | 76–100 (RUNNER_CONFIRMED) |

**Why OpenAI instead of manual scoring:** A token with 2 buys and 0.3 SOL volume gets 0/70 in manual on-chain scoring — but if Crypto Twitter is going nuts about it, the social momentum could translate to explosive buying within minutes. The manual system is blind to this context. OpenAI cross-references on-chain data against social signals and makes a judgment a threshold ladder cannot.

**No new API call at TP1 time.** The system uses the latest cached score from the scheduled narrative checks, so there's zero additional latency at the critical promotion moment.

At promotion:
- **25%** was already sold at TP1.
- The remaining **75%** is handed to the moonbag tracker.
- The **position slot is freed** in TradingState (so new trades can open).
- A row is INSERTed into `moonbag_positions` with: position_id, mint, token info, entry/peak pricing, trailing config, narrative_state, and `narrative_result` JSONB (full OpenAI output at promotion time).
- Moonbag runs independently with its own trailing stop logic.

### 12d. Moonbag Trailing Stop (Age-Based Decay)

The trailing stop starts wide and **tightens over time** as the probability of further upside decreases.

**Initial trailing stop** (first 30 minutes) depends on narrative state at promotion:

| State | Initial Trail |
|-------|--------------|
| EarlyAttention | 45% |
| ExpandingAttention | 55% |
| RunnerConfirmed | 55% |

**Age-based decay schedule** (only activates once profit gate is reached):

| Age | Trailing Stop |
|-----|--------------|
| 0–30 minutes | Initial (45% or 55%) |
| 30 min – 2 hours | 45% |
| 2 – 6 hours | 35% |
| 6 – 12 hours | 25% |
| 12 – 24 hours | 20% |
| 24+ hours | 15% |

**Profit gate:** The age-based decay only kicks in once `peak_price ≥ 2.0 × entry_price`. Before that threshold, the initial wide trail is kept to avoid chopping out marginal tokens.

**Floor protection:** Moonbag never exits below `1.2 × entry_price` regardless of trailing stop math.

### 12e. Moonbag Max Hold Caps

Each narrative state has a maximum hold duration:

| State | Max Hold | Extendable? |
|-------|----------|-------------|
| EarlyAttention | 12 hours | No |
| ExpandingAttention | 24 hours | No |
| RunnerConfirmed | 24 hours (initially) | Yes — extended to 48 hours if narrative re-check at T+24h scores ≥ 76 |

### 12f. Moonbag Price Polling (Decaying Frequency)

| Age | Poll Interval |
|-----|--------------|
| 0–5 minutes | Every 30 seconds |
| 5–15 minutes | Every 60 seconds |
| 15–60 minutes | Every 2 minutes |
| 1+ hours | Every 5 minutes |

### 12g. Moonbag Narrative Re-Checks (Upgrade & Downgrade)

The moonbag tracker periodically re-checks the narrative to see if social attention is growing or fading:

| Re-check # | Interval |
|------------|----------|
| 1 | 5 minutes post-promotion |
| 2 | 10 minutes later |
| 3 | 30 minutes later |
| 3+ | No more re-checks — rely on price action / trailing stop |

**Upgrade:** If the re-check returns a higher state, immediately ratchet up. Trailing widens, hold cap extends.

**Downgrade:** If the re-check returns a state **below** the current state for **2 consecutive checks** (configurable `moonbag_downgrade_consecutive`), the state steps down one level:
- RunnerConfirmed → ExpandingAttention
- ExpandingAttention → EarlyAttention
- EarlyAttention → NoSignal

On downgrade:
- Trailing stop immediately tightens to match the lower state.
- Max hold cap shortens to match the lower state.
- If a moonbag has been downgraded to NoSignal with a short hold cap, the max-hold timer will expire it faster.

**Same state:** Resets the consecutive counter to 0 (token is still performing at its level).

This prevents the old "ratchet-only" problem where a token that was briefly `RunnerConfirmed` during a spike would keep the generous 55% trail and 48h hold even after all attention faded.

### 12h. Moonbag Exit Reasons

| Reason | Description |
|--------|-------------|
| `trailing_stop` | Price dropped below trailing threshold from peak |
| `max_hold` | Held for maximum allowed duration per narrative state |
| `floor_breach` | Price dropped below 1.2× entry price |

---

## 13. Exit Execution (Sell)

### Paper Trade Exit
1. Sleep for simulated delay (2,000 ms).
2. Re-fetch price from DexScreener.
3. Apply 2.5% slippage penalty (receive less SOL than market price implies).
4. Update position in Supabase.

### Real Trade Exit

**Step 1 — Resolve token amount:**
- If amount unknown, polls on-chain SPL token balance (up to 5 seconds with 500ms retries).

**Step 2 — Jupiter sell quote:**
- Endpoint: `https://api.jup.ag/swap/v1/quote`
- Input = token mint, output = SOL, slippage = 2000 bps (20% — wider than buy because exit liquidity is often worse).
- Dynamic slippage range: 5000–10000 bps for Jupiter to optimize on-chain.

**Step 3 — TP sanity check:**
- For take-profit sells (not stop-loss): calculate `sol_received / sol_spent_for_chunk`.
- If return ratio < 1.0 → reject the sell (slippage too high, pool dead). Don't sell at a loss on what's supposed to be a profit take.

**Step 4 — Submit via Jito or Helius Sender** (same as buy).

**Step 5 — Confirm and update Supabase:**
- Update position with: exit price, exit reason, P&L %, P&L SOL, SOL received, hold duration, which TP levels triggered.
- Log cost breakdown: network fee, priority fee, Jito/Helius tip, slippage.

---

## 14. Safety Rules & Protections

| # | Rule | How It Works |
|---|------|--------------|
| 1 | Daily loss limit | Stop all new buys if today's realized P&L exceeds -1.0 SOL |
| 2 | Portfolio exposure cap | Current SOL deployed + new buy ≤ 0.6 SOL total |
| 3 | Min SOL balance | RPC balance check: must have ≥ 0.2 SOL for gas |
| 4 | Max open positions | Hard cap at 5 concurrent positions |
| 5 | Dev dump detection | Partial dump (≥15%, not 100%) → emergency exit + blacklist. Dev sells 100% with ≥3% of supply → staged CTO evaluation (15s/45s/90s), tiered grading |
| 6 | LP drain detection | Pool SOL vault balance drop ≥ 15% → emergency exit |
| 7 | Post-buy emergency | RugCheck/GoPlus/Holders find critical danger after buy → immediate 100% exit |
| 8 | Startup recovery | On boot, queries Supabase for stuck "open" positions — resumes monitoring or force-closes if token balance is 0 |
| 9 | Stale position cleanup | Force-close any position still in TradingState after 1,200 seconds (20 min) |
| 10 | Graceful shutdown | Ctrl+C handler → logs event, clears TradingState, waits up to 10 seconds for cleanup |
| 11 | Anti-chase | Rejects buy if price moved > 50% between filter time and execution time |
| 12 | Wash-trade detection | Rejects tokens where > 10 new wallets bought in the same 3-second window |

---

## 15. APIs & Services Summary

| Service | Base URL | Purpose | Auth | Rate Limit |
|---------|----------|---------|------|------------|
| **PumpFun Portal** | `wss://pumpportal.fun/api/data` | Token discovery (bonding curve events) | None | Unlimited (free WS) |
| **Pump.fun Frontend** | `https://frontend-api.pump.fun` | Historical bonding curve trades | None | Unknown |
| **Solana RPC (Helius)** | `https://mainnet.helius-rpc.com/?api-key=…` | On-chain reads, tx submission | API key in URL | 50 RPS (Developer plan) |
| **Helius DAS** | Same as RPC | `getAsset` for mint/freeze authority | API key in URL | 5 DAS RPS |
| **Helius WebSocket** | `wss://mainnet.helius-rpc.com/?api-key=…` | Dev wallet + LP vault monitoring | API key in URL | 3 Enhanced WS connections |
| **Helius Sender** | `https://sender.helius-rpc.com/fast` | Priority tx submission | API key embedded | Uses priority txn credits (1M/mo) |
| **Jupiter Swap** | `https://api.jup.ag/swap/v1` | Quote + swap transaction building | API key | ~50 RPS |
| **Jupiter Price** | `https://price.jup.ag/v4/price` | SOL/USD price | None | High |
| **Birdeye** | `https://api.birdeye.so/v1` | Token metadata, security, prices | API key header | 50 RPS (Pro) |
| **DexScreener** | `https://api.dexscreener.com` | Price, volume, liquidity, FDV | None | Unlimited (CDN) |
| **RugCheck** | `https://api.rugcheck.xyz/v1` | Risk scoring, holder analysis, bundled detection | None | Unknown (429 retry with 2s backoff) |
| **GoPlus** | `https://api.gopluslabs.io/api/v1` | Token security (honeypot, mintable, etc.) | None | Unknown |
| **SolanaTracker** | Private (key-based) | Token search, market data, trade history | API key header | ~130k req/month |
| **OpenAI Responses** | `https://api.openai.com/v1/responses` | Narrative detection + web search | API key header | Pay-per-use (~$0.025/call) |
| **Jito Block Engine** | Configurable | Bundle submission with tips | None | Unknown |
| **Supabase** | `https://{project}.supabase.co/rest/v1` | All data persistence | Service key header | Unlimited (self-hosted DB) |

---

## 16. Database (Supabase)

All data is stored in a Supabase PostgreSQL database via REST API.

| Table | Purpose |
|-------|---------|
| `tokens_seen` | Every detected token (mint, source, liquidity, name, symbol) |
| `filter_results` | Fast-gate pass/fail per token with rejection reasons |
| `precheck_log` | Phase 0 pre-check results from bonding curve phase |
| `sniper_candidates` | Enrichment results + hard filter pass/fail + counterfactual price tracking |
| `positions` | Open/closed/paper positions with full entry/exit data + narrative result JSONB |
| `pipeline_latency` | End-to-end timing per token (detection → filter → execution) |
| `trade_costs` | Buy/sell fee breakdown per transaction |
| `st_trade_snapshots` | Per-position SolanaTracker trade data (every 15s) |
| `shadow_log` | Post-close price tracking for 1 hour (counterfactual analysis) |
| `system_events` | Startup, shutdown, alerts, exceptions, moonbag events |
| `moonbag_positions` | Full moonbag lifecycle (promotion → exit with all metrics + narrative result JSONB) |
| `narrative_checks` | Every OpenAI narrative check result with scores and evidence |

---

## 17. Configuration Reference

### Detection
| Parameter | Value | Description |
|-----------|-------|-------------|
| `method` | `pumpfun_ws` | Primary detection source |
| `poll_raydium` | `true` | Also watch Raydium for direct launches |
| `poll_interval_seconds` | `5` | Raydium poll interval |

### Filters
| Parameter | Value | Description |
|-----------|-------|-------------|
| `min_liquidity_usd` | `10,000` | Minimum pool liquidity |
| `max_top_holder_pct` | `65%` | Max top-10 holder concentration |
| `max_dev_hold_pct` | `5%` | Max developer holding |
| `max_token_age_seconds` | `600` | Reject tokens older than 10 min |
| `max_graduation_time_seconds` | `7,200` | Reject slow graduations (> 2 hrs) |
| `min/max_market_cap_usd` | `$5k / $150k` | Market cap range |
| `max_price_impact_pct` | `5%` | Max buy impact on pool |
| `max_rugcheck_score` | `3,500` | RugCheck risk score ceiling |
| `reject_bundled` | `true` | Reject bundled launches |
| `min_buy_pressure_pct` | `52%` | Minimum buy-side momentum |
| `min_bonding_volume_sol` | `5` | Minimum bonding curve volume |
| `min_unique_buyers` | `12` | Minimum distinct buyer wallets |
| `max_single_holder_pct` | `25%` | Max any single wallet can hold |
| `min_holder_count` | `8` | Minimum distinct holders |

### Execution
| Parameter | Value | Description |
|-----------|-------|-------------|
| `buy_amount_sol` | `0.10` | Max buy size |
| `min_buy_sol / max_buy_sol` | `0.04 / 0.10` | Dynamic sizing range |
| `slippage_bps` | `1,500` (15%) | Buy slippage tolerance |
| `max_open_positions` | `5` | Concurrent position limit |
| `priority_level` | `veryHigh` | Jupiter priority fee level |
| `max_entry_price_move_pct` | `50%` | Anti-chase threshold |
| `paper_slippage_bps` | `250` (2.5%) | Simulated slippage for paper trades |

### Exit Rules
| Parameter | Value | Description |
|-----------|-------|-------------|
| `max_hold_seconds` | `900` (15 min) | Time stop |
| `tp1_multiplier / sell_pct` | `1.8× / 25%` | First take profit |
| `tp2_multiplier / sell_pct` | `4.0× / 50%` | Second take profit |
| `tp3_multiplier` | `8.0×` | Moonshot (sell all) |
| `stop_loss_pct` | `35%` | Standard stop loss |
| `never_profitable_stop_loss_pct` | `25%` | Tighter stop if never green |
| `trailing_stop_pct` | `30%` | Base trailing stop |
| `trailing_stop_adaptive_pct` | `18%` | Tighten above 3× peak |
| `trailing_stop_post_tp1_pct` | `22%` | After TP1 with entry floor |
| `trailing_stop_post_tp2_pct` | `30%` | After TP2 moonbag trail |

### Risk Limits
| Parameter | Value | Description |
|-----------|-------|-------------|
| `daily_loss_limit_sol` | `1.0` | Stop trading for the day |
| `max_portfolio_exposure_sol` | `0.6` | Total SOL at risk |
| `min_sol_balance` | `0.2` | Reserve for gas |

### Monitoring
| Parameter | Value | Description |
|-----------|-------|-------------|
| `monitor_interval_ms` | `2,000` | Price poll frequency |
| `dev_dump_threshold_pct` | `15%` | Dev wallet drop → exit |
| `lp_drop_threshold_pct` | `15%` | Pool SOL drop → exit |
| `dip_threshold_pct` | `15%` | Peak drawdown to enter DIP_WATCH |
| `shadow_log_duration_secs` | `3,600` | Post-close price tracking (1 hr) |

### Narrative Moonbag
| Parameter | Value | Description |
|-----------|-------|-------------|
| `narrative_check_intervals_secs` | `[120, 240, 360]` | Check schedule during monitoring (T+0 removed) |
| `moonbag_max_concurrent` | `20` | Max simultaneous moonbags |
| `moonbag_max_hold_early_hours` | `12` | EarlyAttention hold cap |
| `moonbag_max_hold_expanding_hours` | `24` | ExpandingAttention hold cap |
| `moonbag_max_hold_confirmed_hours` | `48` | RunnerConfirmed hold cap |
| `moonbag_floor_multiplier` | `1.2` | Never exit below 1.2× entry |
| `moonbag_trailing_early` | `45%` | Initial trail for EarlyAttention |
| `moonbag_trailing_expanding` | `55%` | Initial trail for ExpandingAttention |
| `moonbag_trailing_confirmed` | `55%` | Initial trail for RunnerConfirmed |
| `moonbag_profit_gate_multiplier` | `2.0` | Peak must reach 2× before decay activates |
| `moonbag_promotion_min_score` | `60.0` | OpenAI holistic score threshold for moonbag promotion |
| `moonbag_downgrade_consecutive` | `2` | Consecutive weak re-checks before state downgrades |

### CTO Detection
| Parameter | Value | Description |
|-----------|-------|-------------|
| `cto_min_dev_hold_pct` | `3%` | Minimum dev holding to qualify for CTO path |
| `cto_stage_secs` | `[15, 45, 90]` | Staged evaluation checkpoints (seconds) |
| `cto_strong_recovery_pct` | `85%` | Price recovery for Strong CTO → RunnerConfirmed |
| `cto_moderate_recovery_pct` | `70%` | Price recovery for Moderate CTO → ExpandingAttention |
