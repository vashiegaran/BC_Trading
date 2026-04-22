# Strategy Research — Index

Per-strategy observation docs. Each file captures hypothesis, findings, data-collection gaps, and the proposed design that a **separate future codebase** will implement.

**Hard rule:** Nothing in this folder drives live trading. These are research notes only. See the repository-level [SHADOW_OBSERVATION.md](../../SHADOW_OBSERVATION.md) for the safety rules that govern the current bot.

## Why separate docs?

- Current repo's sniper bot must stay stable and minimal. These observation features are **data collectors** whose analysis should not pollute the core codebase.
- Findings are meant to be **portable seed material** for a separate services project (`meme-strategy-engine` or similar) that runs continuous research, backtests, and rotates which strategies the live bot should respect.
- Each strategy gets its own file so the future codebase can pick them up independently (one service per strategy).

## Status table

| Strategy | File | Status | Top finding | Next action |
|----------|------|--------|-------------|-------------|
| Bonding Curve | [bonding_curve.md](bonding_curve.md) | 📊 analyzed | BSR ≥ 2 + unique_buyers ≥ 40 + no-rebuy → 2.30x lift, 13.30% grad rate | Price/PnL follow-up on 145 graduated mints |
| Narrative | [narrative.md](narrative.md) | ⚠️ data-starved | cat/elon +400–460% median 1h; ai −88% | Wait for n ≥ 100/category; verify peak_price populates |
| Copy Trader | [copy_trader.md](copy_trader.md) | ❌ list broken | Blind copy = −8.5% mean 1h. 48/100 silent. | Build v2 list (Track A & B) in separate repo |
| CTO Watchlist | [cto_watchlist.md](cto_watchlist.md) | 🔧 keyword fix applied | Previously empty; keyword match fixed this session | Re-check after 7d of collection |
| Dip Watchlist | [dip_watchlist.md](dip_watchlist.md) | 🔧 wired this session | `add_bought_token()` now called from both paper + real branches | Re-check after 7d |
| Volume Spike | [volume_spike.md](volume_spike.md) | 📊 prior-session | $100K–$500K MC +25% avg | Re-analyze with recent data |
| Raydium Direct | [raydium_direct.md](raydium_direct.md) | 🔧 source check fixed | Correct code, low volume — few non-PumpFun tokens | Extend collection window |
| Meta Tracker | — merged into narrative.md | — | Same data source | — |

## Future codebase blueprint

See [../next-codebase-plan.md](../next-codebase-plan.md) for the proposed architecture of the separate research/rotation service.
