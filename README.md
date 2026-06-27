# INVESTOS — Personal Intelligence Platform
**@adejuwon_t · Toronto, Canada · Live at [tolulop3.github.io/investos](https://tolulop3.github.io/investos)**

> **FOR AI ASSISTANTS:** This README is the authoritative context document for every session.
> Read this fully before touching any file. The system is live, running daily.
> Do not break what works. Optimise only.
>
> Start every session by stating current system state before suggesting anything.
> Do not re-explain decisions listed in "Architecture Decisions — Do Not Revisit".
> Do not flag intentional design choices as bugs.

---

## WHAT THIS IS

A fully automated daily investment intelligence platform. Every weekday at 9:30am ET,
GitHub Actions runs `run_daily.py`, which screens 189 stocks, scores them with ML,
detects macro signals from 15 news feeds, sizes positions by volatility, generates ETF
picks, tracks NGX signals, and bakes everything into a live dashboard.

Personal tool for one user (Toronto). Not yet a product. Not public-facing.
Goal: open the dashboard, know what the market is doing, know what to do, trust it.

**Bigger goal: InvestOS is the IP engine. Every product below reads from it.**

---

## CURRENT SYSTEM STATE (as of June 27, 2026)

| Metric | Value | Status |
|--------|-------|--------|
| Sharpe (90d rolling) | -3.056 | 🔴 Guard engaged — recovering |
| Expected recovery | June 28-30 | ⏳ March picks rolling off |
| Win rate overall | 49.8% | ⚠️ Depressed — guard period |
| Win rate last 30d | 46.8% | ⚠️ Watch |
| Win rate last 90d | 54.6% | ✅ Holding |
| Current streak | 3 WIN | ✅ |
| Avg return/pick | +0.16% | ⚠️ Compressed |
| Unified regime | DEFENSIVE (12%) | 🔴 Guard at 0.25× |
| Macro regime | CAUTIOUS | ⚠️ War escalation signals |
| Breadth (50MA) | 65.1% | ✅ MODERATE |
| Breadth (200MA) | 68.3% | ✅ Holding |
| Robustness | 25/100 | 🔴 Guard period |
| Risk multiplier | 0.25× | 🔴 PCR conflict + convergence |
| PCR | 1.253 | ⚠️ BEARISH — rising |
| Neg alpha streak | 31 days | ⚠️ Clearing |
| Universe | 189 tickers | ✅ Scout adding 14 dynamic |
| OOS | Day 1 — v4.1 | ⏳ 0 resolved picks since Jun 26 |
| NGX phase | RESTRICTED Day 48 | ⏳ Tier 1 ≥80 only |
| NGX win rate | 0% (159 resolved) | ⚠️ Macro reversed — new signals |
| Runtime | ~65s | ✅ Fast |
| Last commit | June 27 2026 | |

**Score tier performance (1,850 resolved picks):**
| Tier | n | WR | Avg Return | PF | Notes |
|------|---|----|------------|-----|-------|
| 90-100 | 902 | 46.7% | -0.20% | 0.90 🔴 | Loses money — curve fix + ML gate active |
| 75-89 | 462 | 53.7% | +0.14% | 1.08 ⚠️ | Recovering |
| 60-74 | 341 | 56.9% | +1.14% | 1.90 ✅ | Real edge — best tier |
| below-60 | 145 | 40.7% | +0.11% | 1.05 ⚠️ | High PF but low WR |

**PF Baseline (locked June 25, 2026 — post-dedup + post-backlog):**
| Tier | Baseline PF | Current | Status |
|------|------------|---------|--------|
| 90-100 | 0.91 | 0.90 | ✅ stable |
| 75-89 | 1.07 | 1.08 | ✅ stable |
| 60-74 | 1.92 | 1.90 | ✅ stable |
| below-60 | 1.09 | 1.05 | ✅ stable |

> All PFs depressed due to Sharpe guard period. Compare to 90d baseline, not daily.

---

## THE PLATFORM VISION

InvestOS is the oil well. The products below are the refineries.

```
INVESTOS ENGINE (private IP — never expose)
              ↓
         BAKED_DATA
    (single source of truth)
              ↓
  ┌───────────┼───────────┬──────────────┐
  ↓           ↓           ↓              ↓
AllocOS    InvestOS    Regime Feed    Data API
(personal   Lens        (macro        (B2B
→ product)  (Chrome     signals)      signal
            extension)               feed)
```

---

## PRODUCT ROADMAP

### PRODUCT 1 — AllocOS
Portfolio allocation intelligence. Reads InvestOS signals + account balances →
outputs exact deployment plan per account (TFSA/FHSA/RRSP/NGX).
Telegram morning brief + web dashboard.
**Tiers:** Free (regime only) · Pro $15 CAD/mo · Elite $35 CAD/mo
**Status:** Design complete. Needs Telegram bot setup first.

### PRODUCT 2 — InvestOS Lens
Chrome extension. Hover over any ticker → InvestOS score ring popup.
**Status:** Design complete. Standalone build — any session.

### PRODUCT 3 — NGX Intelligence
Institutional macro signals for Nigerian Exchange.
**FTSE frontier reclassification September 2026 → $840M-$1B+ inflows.**
**Status:** Engine running. Waitlist page = next marketing step.

### PRODUCT 4 — Data API
Daily JSON signal feed for advisors, boutique PMs, fintech builders.
$200-500 USD/month. No extra build — API wrapper on BAKED_DATA.

### PRODUCT 5 — Newsletter
NFA educational brief via Telegram. Content engine already generates daily.

### PRODUCT 6 — Fund (Year 3+)
Requires 3yr verified track record + OSC exempt structure.

---

## AUTONOMY STACK (current state)

```
LAYER 1 — DATA PIPELINE ✅ fully autonomous
──────────────────────────────────────────────
GitHub Actions 9:30am ET weekdays
→ run_daily.py (12 steps, 189 tickers, 65s)
→ scout_agent.py (Sunday 6am — universe expansion)
→ ml_retrainer.py (auto-fires when coverage gate hits ~143 picks)
→ Commits outcomes, history, scores to repo

LAYER 2 — KNOWLEDGE LAYER ✅ live June 27
──────────────────────────────────────────────
history/obsidian/YYYY-MM-DD.md — daily notes (GitHub Actions writes)
history/obsidian/patterns.md   — weekly pattern summary
history/obsidian/watchlist.md  — signal watchlist
history/obsidian/tickers/*.md  — per-ticker notes (auto-created)
history/obsidian/research/*.md — daily strategist research notes
pattern_signals.json           — machine-readable score boosts

LAYER 3 — PATTERN FEEDBACK LOOP ✅ live June 27
──────────────────────────────────────────────
pattern_agent.py (9:45am ET):
→ reads last 7 daily snapshots + outcomes
→ detects streaks, velocity, regime drift, sector concentration
→ writes pattern_signals.json (score boosts/penalties)
→ stock_screener.py reads this → CONSIDER tickers get +3pts
→ AVOID tickers get -5pts
LOOP CLOSED: observation → pattern → score → picks → outcomes → repeat

LAYER 4 — STRATEGIST ✅ live June 27
──────────────────────────────────────────────
strategist_agent.py (10:00am ET):
→ reads brief + patterns + history + Obsidian notes
→ calls Claude API to reason about the data
→ writes research note to history/obsidian/research/YYYY-MM-DD.md
→ flags contradictions, regime drift, opportunities, watch items

LAYER 5 — ML IMPROVEMENT ⏳ on timers
──────────────────────────────────────────────
ML retrain: ~143 more featured picks needed (~3 weeks from Jun 26)
Walk-forward validation: August 2026
Portfolio optimizer: September 2026 (90d outcomes resolve)
```

---

## OBSIDIAN VAULT

**On Mac:** `~/Documents/investos-brain` (synced via Obsidian Git plugin)
**In repo:** `history/obsidian/` (written by GitHub Actions)

Both locations contain the same files. The Git plugin syncs within 10 minutes.

**Why notes may not appear on Mac yet:**
The Obsidian Git plugin needs to pull from the repo after GitHub Actions commits.
Check: Obsidian → Command palette → "Obsidian Git: Pull" to force sync.
Or wait for the auto-pull interval (set in plugin settings, default 5-10 min).

**Vault structure:**
```
history/obsidian/
├── YYYY-MM-DD.md      ← daily system notes (auto-written)
├── patterns.md        ← weekly pattern summary (pattern_agent)
├── watchlist.md       ← signal watchlist (pattern_agent)
├── tickers/           ← per-ticker notes (pattern_agent)
│   └── TICKER.md
└── research/          ← daily research notes (strategist_agent)
    └── YYYY-MM-DD.md
```

---

## FILE REGISTRY

| File | Lines | Purpose |
|------|-------|---------|
| `run_daily.py` | ~1982 | Master orchestrator — 12 steps + all agent calls |
| `stock_screener.py` | ~1210 | Screens 189 tickers, scores 0-100, reads pattern_signals |
| `ml_engine.py` | 1210 | XGBoost + vol-targeted sizing + Kelly + sector cap + ML gate |
| `ml_retrainer.py` | 443 | XGBoost retraining with coverage gate |
| `risk_engine.py` | ~1476 | Stress test, decay monitor, unified regime engine |
| `news_analyzer.py` | 625 | 15 RSS feeds, sector sentiment, macro regime |
| `intelligence_layers.py` | ~792 | RS ratings, score history, trend detection |
| `etf_engine.py` | ~408 | 30 ETFs, RRSP/TFSA/FHSA routing |
| `content_engine.py` | ~641 | Tweet + thread generation |
| `portfolio_engine.py` | ~1213 | Trades tracker, scorecard, open positions |
| `outcome_tracker.py` | 559 | Win rate tracking, score tier analysis |
| `factor_investigation.py` | 212 | Factor attribution — runs every daily run |
| `scout_agent.py` | ~410 | Sunday 6AM: universe expansion (30 ETF sources) |
| `pattern_agent.py` | 595 | 9:45am: detects patterns, writes signals, closes loop |
| `strategist_agent.py` | 356 | 10:00am: Claude reasons about data, writes research note |
| `history_analyzer.py` | 488 | Silent daily: generates history_analysis.json |
| `strategy_version.py` | — | OOS anchor — logs 41 rule params per run |
| `congressional_engine.py` | — | SEC EDGAR Form 4. HTTP 403 = DATA UNAVAILABLE |
| `ngx_screener.py` | ~360 | Nigerian Exchange macro-driven signals |
| `crypto_engine.py` | ~357 | BTC + SOL signals |
| `index.html` | ~7148 | Full dashboard — baked daily |
| `.github/workflows/daily_analysis.yml` | ~30 | 9:30am ET weekdays |
| `.github/workflows/pattern_agent.yml` | ~30 | 9:45am ET weekdays |
| `.github/workflows/strategist_agent.yml` | ~30 | 10:00am ET weekdays |
| `.github/workflows/weekly_scout.yml` | ~30 | Sunday 6AM ET |

**Persistent data files (never delete):**
| File | Contents |
|------|----------|
| `outcomes_log.json` | Every pick — 2,020 total (1,850 resolved) |
| `cooldown_flags.json` | Permanent exclusions + loss-streak cooldowns |
| `pf_baseline.json` | PF baseline locked June 25 |
| `pattern_signals.json` | Pattern agent score boosts — updated 9:45am |
| `history_analysis.json` | Weekly structured analysis |
| `strategy_version.json` | OOS audit trail — v4.1 from June 26 |
| `universe_current.json` | Current universe (static + dynamic scout) |
| `history/YYYY-MM-DD.json` | Daily snapshots — started June 6, 2026 |
| `history/obsidian/` | All Obsidian notes — synced to Mac vault |

---

## ARCHITECTURE

### The Daily Pipeline (12 steps + agents)

```
9:30am  [1]  NEWS & MACRO    → news_analyzer.py
        [2]  MARKET REGIME   → risk_engine.py
        [3]  STOCK SCREEN    → stock_screener.py (reads pattern_signals.json)
        [4]  APPLY NEWS      → run_daily.py
        [4c] CONGRESSIONAL   → congressional_engine.py
        [5]  ML ENGINE       → ml_engine.py
        [6]  INTELLIGENCE    → intelligence_layers.py
        [7]  X FEEDS         → run_daily.py
        [8]  CONVICTION      → run_daily.py
        [9]  FX & GOLD       → risk_engine.py
        [10] CRYPTO          → crypto_engine.py
        [11] RISK AUDIT      → risk_engine.py
        [ETF] ETF ENGINE     → etf_engine.py
        [12] CONTENT         → content_engine.py
        [NGX] NGX ENGINE     → ngx_screener.py
        [END] FACTOR REPORT  → factor_investigation.py
        [END] HISTORY ANALYZER → history_analyzer.py (silent)
        [END] STRATEGIST     → strategist_agent.py (if ANTHROPIC_API_KEY set)
        [END] OBSIDIAN BRIDGE → writes daily note

9:45am  pattern_agent.py (separate workflow)
        → reads snapshots + outcomes
        → writes pattern_signals.json + Obsidian notes

10:00am strategist_agent.py (separate workflow)
        → reads everything → calls Claude API
        → writes research note
```

### Score Compression (v4.1 — June 26 2026)
```python
# Above 85: each raw point = 0.4 calibrated points (was 0.6)
# Raw 100 → 91. Raw 94 → 88.6 (shifts to 75-89 tier)
adjusted = 85 + (excess * 0.4)
```

### Three-Layer Unified Regime Engine
```
market_score  = +1.0   (SPX vs 200MA)
macro_score   = -0.3   (news signals)
health_score  = -1.0   (Sharpe — currently negative)
unified_score = (0.40 × market) + (0.30 × macro) + (0.30 × health)
→ DEFENSIVE (12% exposure, 0.25× risk multiplier)
```

### Safety Gates (in order)
1. Permanent exclusions — `cooldown_flags.json`
2. Loss-streak cooldown — 2+ losses ≥1.5% in 10 picks → 7-day block
3. Sector diversity cap — max 2 picks per normalized sector
4. ML confidence gate — score≥90 AND ml_prob<0.20 → removed, replaced
5. Sharpe guard — Sharpe negative → 12% of normal sizing

### Pattern Signal Boost (new — June 27)
```python
# Applied last in score_stock() — after all caps
# CONSIDER (+3pts): 3+ day streak + rising velocity
# AVOID (-5pts): falling score + poor PF
# Capped at ±5 — nudges, never overrides fundamentals
```

---

## PERMANENT EXCLUSIONS (cooldown_flags.json)

Blocked to 2036 based on factor_investigation.py evidence:
F, WPM.TO, MDB, DXCM, AEM.TO, ABX.TO, NTR.TO, K.TO

**SCORE_CAP_74** (capped at 74, not excluded):
F, DXCM, WPM.TO, FM.TO, ABX.TO, AEM.TO, AGI.TO, MDB, MSFT

---

## KNOWN ISSUES (open)

| Priority | Issue | Detail | Fix |
|----------|-------|--------|-----|
| ⚠️ MEDIUM | ML retrain coverage 8% | Gate fires at ~143 more picks (~3 weeks) | Auto-fires |
| ⚠️ MEDIUM | Sharpe -3.056 | March picks clearing June 28-30 | Auto-recovers |
| ⚠️ MEDIUM | pattern_agent needs 30d data | Boosts are noise until patterns accumulate | Wait |
| ⚠️ MEDIUM | strategist needs ANTHROPIC_API_KEY | Add to GitHub Secrets | One-time setup |
| ⚠️ MEDIUM | Obsidian not syncing to Mac | Pull manually or check Git plugin interval | Check plugin |
| ℹ️ LOW | BoC feed 0 articles | No recent items in RSS | Not a code issue |
| ℹ️ LOW | Reuters/AP/Investopedia ❌ | GitHub Actions DNS — 12/15 stable ceiling | Architecture decision |
| ℹ️ INFO | Congressional 403 | S3 endpoints gone. api.congress.gov is clean path | Add CONGRESS_API_KEY |

---

## OPEN WATCH ITEMS

| Item | Status | Notes |
|------|--------|-------|
| Sharpe recovery | ⏳ June 28-30 | March picks rolling off — auto-recovers |
| ML retrain | ⏳ ~3 weeks | ~143 more featured picks needed |
| OOS validation | ⏳ Sept 2026 | 90 days needed for meaningful evidence |
| Walk-forward | ⏳ Aug 2026 | 60+ days history required |
| Pattern agent data | ⏳ 30 days | Boosts meaningful after July 27 |
| NGX FTSE catalyst | ⏳ Sept 2026 | $840M-$1B+ inflows — 3 months away |
| AGI.TO | ⚠️ Watch | 18 picks, 33% WR — approaching exclusion threshold |

---

## ARCHITECTURE DECISIONS — DO NOT REVISIT

1. **Regime engine: 3 inputs → 1 output.** market + macro + health. Correct as-is.
2. **9/15 news feeds ceiling.** GitHub Actions DNS. Not fixable. 12/15 stable.
3. **NGX 0% WR** — macro reversed during resolution. Thesis failed correctly. Not a bug.
4. **No DCA for NGX** — FX conversion cost. WAIT/WATCH/ENTER framework instead.
5. **No product launch yet** — engine must recover. Target: Sharpe > 1.0 sustained.
6. **Double bake** — intentional. First bake fast (pre-NGX), second includes NGX.
7. **Score ceiling → 0.4 curve** — NOT a hard ceiling at 89. Curve steepening ships.
   Raw 100 → 91. Raw 94 → 88.6. Ordinal ordering preserved. ML gate diagnostic intact.
8. **Pattern boost capped at ±5** — nudges, never overrides fundamentals.
9. **Strategist agent is read-only** — it observes and writes notes. Never touches scores.
10. **Obsidian notes live in repo** — `history/obsidian/`. Mac vault = same files via Git.
11. **No auto-execution** — system generates signals and research. Human decides.
12. **InvestOS never exposes raw engine logic** — BAKED_DATA is the interface layer.

---

## SECURITY

| Item | Status |
|------|--------|
| Hardcoded secrets | ✅ None — all via os.environ / GitHub Secrets |
| trades.csv | ✅ Gitignored |
| ANTHROPIC_API_KEY | ⚠️ Add to GitHub Secrets for strategist agent |
| Netlify function auth | ✅ x-investos-key header required |
| ML model cache (.pkl) | ✅ Gitignored |
| __pycache__ | ✅ Gitignored (fixed June 27) |

---

## ETF ENGINE

30 ETFs across 5 categories. Account routing:
- **RRSP** → US-listed ETFs (IRS treaty exemption on dividends)
- **TFSA** → .TO ETFs, sector/thematic OK
- **FHSA** → Conservative Canadian only. Excludes thematic + GLD, TLT, VWO, EEM, XLE, QQQ, VOO

---

## NGX ENGINE

Macro-driven scoring for 30 Nigerian Exchange stocks.
Inputs: Brent (primary), DXY, SPY, VIX, EEM. Resolution: 14 days.
Phase: RESTRICTED (Day 48) — Tier 1 stocks, score ≥80 only.
Entry: WAIT / WATCH / ENTER (not DCA — FX conversion cost).

**FTSE frontier reclassification September 2026 → institutional window opens now.**
GTCO(~₦135), MTNN(~₦820), SEPLAT(~₦11,363) — primary targets for passive inflows.

---

## ACCOUNT CONTEXT

| Account | Purpose | Strategy |
|---------|---------|----------|
| TFSA | Growth + income | Sector ETFs, dividend stocks, selective thematic |
| FHSA | Home purchase savings | Conservative Canadian — XEQT.TO, ZEB.TO, ZAG.TO |
| RRSP | Retirement | US-listed — XLE, VOO, QQQ, GLD, CIBR |
| NGX | Nigerian equity | Via Bamboo Invest — SEPLAT, MTNN, GTCO |

---

## PERFORMANCE HISTORY

| Date | Sharpe | WR 30d | Event |
|------|--------|--------|-------|
| Early 2026 | 1.07 | ~60% | BCE.TO losses begin |
| May 22 | 0.47 | 62.4% | ETF engine v1 launched |
| May 27 | -0.661 | 63.2% | Sharpe guard fires |
| Jun 6 | +0.715 | 63.6% | Guard OFF — all-time high WR |
| Jun 21 | 1.964 | 62.1% | Regime RISK_OFF (war escalation) |
| Jun 22 | 2.034 | 61.6% | Macro CAUTIOUS, recovering |
| Jun 26 | -3.058 | 47.6% | Guard re-engaged — DEFENSIVE 0.25× |
| Jun 27 | -3.056 | 46.8% | First Sharpe micro-recovery |

---

## IMMEDIATE NEXT BUILDS (in order)

```
1. Add ANTHROPIC_API_KEY to GitHub Secrets
   → unlocks strategist_agent.py daily research notes

2. UI — search overlay (⌘K)
   → ticker search → full research card
   → replaces Lookup tab
   → highest UI value remaining

3. Telegram bot setup (5 min via @BotFather)
   → needed for AllocOS Phase 1

4. AllocOS Phase 1 (personal build)
   → you are user zero
   → Telegram morning brief from BAKED_DATA

5. InvestOS Lens (Chrome extension)
   → standalone, any session

6. NGX waitlist page (Carrd.co)
   → email capture before FTSE catalyst
```

---

## HOW TO CONTINUE A SESSION

Start with:
> "I'm continuing InvestOS development. Read the README.md and the latest log,
> then tell me system state before we build anything."

The AI should:
1. Read this README fully
2. Read the latest GitHub Actions log if provided
3. State current system state (metrics table above)
4. Confirm what was last built
5. Not re-explain Architecture Decisions
6. Pick up from Immediate Next Builds — in order

---

## SESSION LOG

| Date | Change | File(s) | Notes |
|------|--------|---------|-------|
| Jun 22 | Dedup pass | outcomes_log.json | 2005→1881 entries |
| Jun 22 | SCORE_CAP_74: F, DXCM, WPM.TO | stock_screener.py | |
| Jun 22 | PF baseline locked | pf_baseline.json | |
| Jun 22 | Congressional engine | congressional_engine.py | HTTP 403 = DATA UNAVAILABLE |
| Jun 23 | SCORE_CAP_74: FM.TO added | stock_screener.py | -14.3% worst single loss |
| Jun 25 | PF baseline reset (post-backlog) | pf_baseline.json | Clean baseline |
| Jun 26 | Curve steepening 0.6→0.4 | stock_screener.py | v4.1 OOS Day 0 |
| Jun 26 | ML gate fix in conviction engine | run_daily.py | QCOM false positive fixed |
| Jun 26 | TIH.TO airline fix | ml_engine.py | Industrials→AIRLINES removed |
| Jun 26 | Obsidian vault created | investos-brain repo | ~/Documents/investos-brain |
| Jun 26 | Obsidian daily bridge fixed | run_daily.py | Variable scope: all from brief |
| Jun 26 | history_analyzer.py built | history_analyzer.py | 6 modules, silent daily |
| Jun 26 | strategy_version.py built | strategy_version.py | OOS anchor, 41 params |
| Jun 26 | __pycache__ gitignored | .gitignore | Stops binary conflicts |
| Jun 27 | pattern_agent.py built | pattern_agent.py | 595 lines, closes autonomy loop |
| Jun 27 | pattern_agent.yml workflow | .github/workflows/ | 9:45am ET weekdays |
| Jun 27 | scout_agent upgraded | scout_agent.py | 17→30 ETF sources, RSI 80→75 |
| Jun 27 | pattern_signals wired to screener | stock_screener.py | ±3-5pt boost/penalty |
| Jun 27 | strategist_agent.py built | strategist_agent.py | Claude API, daily research note |
| Jun 27 | strategist_agent.yml workflow | .github/workflows/ | 10:00am ET weekdays |
| Jun 27 | README updated | README.md | Full platform state captured |

---

## CONVERSATION CONTEXT

- "Uncle" = Claude
- OOS start: June 26 2026 (v4.1 — curve fix confirmed in code)
- $5,000 hero number = deployable capital at current 0.25× multiplier
- Score ceiling decision: curve 0.4 (NOT hard cap at 89) — ordinal ordering preserved
- Pattern agent: boosts are noise for ~30 days — meaningful after July 27
- Strategist agent: needs ANTHROPIC_API_KEY in GitHub Secrets
- Obsidian sync: check Git plugin pull interval if notes not appearing on Mac
- AllocOS: next product build after Telegram bot setup
- NGX FTSE: September 2026 catalyst — 3 months to institutional window

---

*InvestOS v4.2 — Built session by session. Every line has a reason.*
*NFA · Educational only · Personal use only*
