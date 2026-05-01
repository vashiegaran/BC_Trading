# 14-Day Narrative Winner Memo (4x+ Cutoff)

Date: 2026-05-01

## Scope

- Window used: 2026-04-17T00:00:00Z through the 2026-05-01 extraction run.
- Internal sources used: `positions`, `bc_paper_trades`, `sniper_candidates`.
- External sources used: pump.fun metadata links, direct X post fetches, Bath article, Bittime article, Gate article.
- Inclusion rule: unique mints with `peak_multiplier >= 4.0`.
- Classification:
  - Traded = mint exists in `positions`.
  - Missed = mint exists in `bc_paper_trades` but not in the traded set.

## Dataset Size

- Traded unique 4x+ tokens in the window: 25
- Missed unique 4x+ paper-trade tokens in the window: 148

## Main Miss Pattern

Among the 148 unique missed 4x+ names, the dominant rejection reasons were:

- `buy_sell_ratio < 2.0`: 62
- `sniper_score < 60`: 30
- `creator_rebuy_detected`: 12
- `mint_authority_no_data`: 6
- No explicit recorded rejection reason: 38

This means the biggest leak on the missed side was not a lack of narrative names. It was mostly hard filtering on buy/sell ratio and, secondarily, sniper score.

## Best Evidence-Backed Cases

These are the names where I could connect the token to a specific external post, article, or ongoing public narrative with enough confidence to be useful.

### 1. AMERICA IS BACK cluster

Why it matters:
This is the cleanest political-slogan cluster in the dataset. The same slogan appeared on both the traded and missed sides.

Internal rows:

- Traded: `GgieLnjHjwYwSvscVNc2xB2qN1K3YhjYPngzY9c2bonk` reached 9.42x, entered 2026-04-23 22:10Z, exited by `tp3`.
- Missed: `98Mrhop3Fd6pCuSWe3Hbb8FuhKBWnh1op9P9iZQQpump` with symbol `BACK` reached 14.36x, entered 2026-04-23 22:06Z, rejected for `buy_sell_ratio=1.08 < 2.0`.
- Missed: `EjPkXfv9A9h84Dx6Wb4UFZaQzCocLKem5qdVBjCMpump` with symbol `AIB` reached 12.97x, entered 2026-04-23 22:07Z, rejected for `buy_sell_ratio=1.51 < 2.0`.

External evidence:

- The pump.fun metadata for both the traded AIB mint and the traded `BACK` mint linked directly to the same RT post.
- The fetched RT post at 2026-04-24 06:06Z read: `AMERICA IS BACK` and described Trump using it as a new political slogan.
- A 2026-04-24 Bittime article on AIB said the token was drawing attention because its name resembled a popular political slogan.
- A Gate article described AIB as a Solana meme coin centered on American revival and political or patriotic branding.

Read:

- Narrative mapping is strong.
- Timing is suggestive, not fully proven. The exact fetched RT post came after the bot's traded entry time, so the slogan likely existed in circulation before the fetched post, but I did not independently fetch an earlier source.

Confidence: Medium

### 2. Conspiracist

Why it matters:
This is one of the clearest examples of a token whose own metadata and linked post explicitly explain the narrative.

Internal row:

- Missed: `5wqbNRtu5eEuGLP1houqRMZdMqq2bvHZuVtY1yLcpump` reached 4.29x, entered 2026-04-26 11:58Z, rejected for `buy_sell_ratio=1.45 < 2.0`.

External evidence:

- The token website linked to a University of Bath article titled `Memes spread conspiracy theories by uniting online groups, shows new research`.
- The fetched linked X post at 2026-04-26 19:58Z explicitly said:
  - a `shooting` happened at Trump's memecoin conference the day before,
  - the author found an old article about memes spreading conspiracy theories,
  - `Conspiracist` was the perfect narrative for the current meta,
  - people were claiming the event was staged and turning it into meme-driven conspiracy content.

Read:

- The external post directly explains why the token existed and why it matched the moment.
- I do not have exact peak timestamp validation, so I cannot prove the later tweet came before the exact top. But the narrative linkage itself is unusually explicit.

Confidence: Medium-High

### 3. The Chosen Bee

Why it matters:
This is a clean social-buzz example where the token maps to a very specific image meme rather than a vague ticker theme.

Internal row:

- Missed: `DTfN4DotNupzDbqpg6dX4jfXekB5vVkePxjuyivJpump` reached 19.32x, entered 2026-04-28 16:17Z, rejected for `buy_sell_ratio=1.27 < 2.0`.

External evidence:

- The token metadata linked to an X post from 2026-04-29 00:16Z saying an image of Trump holding a bee was getting spam-posted, that people were calling it `the chosen bee`, and that bigger accounts were starting to push it.
- That same post linked to an Autism Capital post from 2026-04-29 00:10Z reading: `You have chosen wisely, bee...` which had 57K views in the fetched page.

Read:

- The meme cluster is real and externally visible.
- The precise catalyst-to-peak sequence is not fully pinned down from the available tables, but this is a strong example of a politically adjacent meme image turning into a token narrative.

Confidence: Medium

### 4. Solana AI Companion

Why it matters:
This is a missed AI-theme name with both decent internal score and an external social post that matches the exact idea.

Internal row:

- Missed: `2iB7DnDBLFKRTVShXdcyX9jS8S5gWRicEbu3e2Uypump` with symbol `ZUMI` reached 4.32x, entered 2026-04-28 16:29Z, had `entry_score=83`, and was still rejected for `buy_sell_ratio=1.30 < 2.0`.

External evidence:

- The token metadata linked to a Tapestry post from 2026-04-29 00:22Z that simply said `ai companions on Solana`.

Read:

- This is a straightforward AI-on-Solana meme or product-narrative match.
- The evidence is lighter than AIB, Conspiracist, or Bee because I only fetched one directly matching post.

Confidence: Medium-Low

## Strong But More Tentative Cases

These names clearly moved hard, but the evidence was either weaker, post-hoc, or too incomplete to call them main-list confirmations.

### FOUR TWENTY / 420 cluster

- Traded: `8wiK5otr25uY5htJn9wNURnA5muh7yuoyHG34SAfpump` reached 56.80x.
- Missed: `D1WBckzyNPsi8UfWUfKn5jrnuzAnWcLaETzPKqnVpump` reached 20.56x and was rejected for `buy_sell_ratio=1.62 < 2.0`.
- The traded mint's metadata linked to a White House X post from 2026-04-21 04:40Z.
- Problem: the fetched White House post was generic app-promotion copy and did not clearly explain the `420` narrative by itself.

Confidence: Low

### ELON VS ALTMAN

- Traded: `EJYpcdDvRJAxe2J3JRgCbbUph24QJ94nbhQbCLmGpump` reached 4.25x, entered 2026-04-28 09:12Z, exited by `trailing_stop`, and was moonbag-promoted.
- The token description explicitly framed the trade around Elon Musk versus Sam Altman and the OpenAI conflict.
- I could not fetch an independent external news article for this exact token during the run because search results began hitting anti-bot challenges.

Confidence: Low-Medium

### Unt Sweeney

- Missed: `Eh9xdfR9uxCjoyY85m3caC2K3MEEuSGNBXaCYpoEpump` reached 4.29x and was rejected for `sniper_score=49 < 60`.
- The profile fetch showed it was a Sydney Sweeney parody account created in April 2026.
- Problem: the profile had only 125 followers and no posts visible, so this looked more like a small social parody than a broad catalyst-driven move.

Confidence: Low

## High Movers I Did Not Promote Into The Main Memo

I left these out of the main evidence-backed list even though some were very large winners:

- `World Collective Oil Reserve` at 276.67x
- `Global Military Arms Reserve` at 172.42x
- `boobcoin` at 38.72x
- `JAPANESE UNC` at 8.22x
- `UNCLE COWBOY` at 8.06x
- `FIRE` at 7.59x
- `Aukt` at 7.18x
- `paper guy` at 5.80x
- `Riverhold` at 4.33x

Reason:

- The name-to-narrative fit may be obvious in some cases, but I did not get enough accessible external evidence to distinguish `real catalyst trade` from `random memecoin that also ran`.
- DuckDuckGo started returning anti-bot challenges mid-run, which limited broader name-based discovery. For that reason, I prioritized direct metadata links and directly fetchable posts instead of looser web guesses.

## What This Suggests

1. Narrative or social-buzz tokens absolutely showed up in the last 14 days on both the traded and missed sides.
2. The missed side was much larger than the traded side at the 4x+ threshold.
3. The main structural leak was not `no narrative names present`; it was filtering, especially `buy_sell_ratio < 2.0`.
4. Political and Trump-adjacent slogan or image memes were the clearest repeat theme in this sample.
5. AI and culture-account narratives were present too, but the external proof quality was usually weaker than the political cluster.

## Limits

- I did not change repo code or strategy logic.
- I did not run a broader web crawler or build a permanent event database.
- Some `bc_paper_trades` short-horizon snapshot fields were not reliable enough for clean catalyst-to-peak timing validation on every candidate.
- This memo should be read as a high-signal research pass, not as an exhaustive catalog of every news-linked token in the window.