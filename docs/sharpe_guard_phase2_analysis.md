# Sharpe Guard — Phase 2 Analysis (wiring into real sizing)

**Status: ANALYSIS ONLY. No code from this document has been implemented.**
Phase 1 (shipped separately, commit `f14acd67`) fixed the false "auto-reduced"
log claim and added an explicit, accurate SIZING STACK log. This document
analyzes whether Sharpe Guard *should* become a real sizing multiplier, and
what happens if it does. It is a decision input, not a decision.

Data source: `run_manifest.json` + `risk_report.json` (`decay_monitor.rolling_sharpe`)
+ `latest_brief.json` (`sized_positions`, `dollar_amt` vs `dollar_amt_adj`) pulled from
one commit per calendar day, 2026-06-20 → 2026-07-19 (30 days), via `git show
<commit>:<file>` against the actual daily "Brief" commits — real production
history, not synthetic data. Trade outcomes from `outcomes_log.json`, filtered
to `resolved=true` and `signal_date >= 2026-06-26` (OOS_START_DATE per
`strategy_version.py`) — 271 resolved trades.

---

## 2A — Interaction modeling

The real, currently-applied regime multiplier is `_risk_multiplier`
(`run_daily.py:1197-1236`): CAPITAL_PRESERVATION/DEFENSIVE → 0.25×, NEUTRAL →
0.50×, RISK_ON with no convergence/PCR-conflict flags → 1.00× ("clean").
Historical convergence/PCR-conflict flags for past days were not
reconstructable from the daily snapshots pulled, so all RISK_ON days below are
modeled as "clean" (1.00×) — a simplification that does not affect the
NEUTRAL/DEFENSIVE rows, which are the ones that matter here.

If Sharpe Guard's factor (0.6× when `0.0 ≤ rolling_sharpe < 0.3`, 0.4× when
`rolling_sharpe < 0.0`) were multiplied directly onto `_risk_multiplier`:

| date | rolling_sharpe | regime | regime_mult | sharpe_mult | combined_mult | actual_deployed_% | hypothetical_deployed_% |
|---|---:|---|---:|---:|---:|---:|---:|
| 2026-07-19 | 0.213 | NEUTRAL | 0.50 | 0.60 | 0.300 | 50.0% | 30.0% ⚠️ |
| 2026-07-18 | 0.215 | NEUTRAL | 0.50 | 0.60 | 0.300 | 50.0% | 30.0% ⚠️ |
| 2026-07-17 | 0.218 | NEUTRAL | 0.50 | 0.60 | 0.300 | 50.0% | 30.0% ⚠️ |
| 2026-07-16 | 0.246 | NEUTRAL | 0.50 | 0.60 | 0.300 | 50.0% | 30.0% ⚠️ |
| 2026-07-15 | 0.229 | NEUTRAL | 0.50 | 0.60 | 0.300 | 50.0% | 30.0% ⚠️ |
| 2026-07-14 | 0.292 | NEUTRAL | 0.50 | 0.60 | 0.300 | 50.0% | 30.0% ⚠️ |
| 2026-07-13 | 0.329 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-12 | 0.346 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-11 | 0.346 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-10 | 0.343 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-09 | 0.375 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-08 | 0.399 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-07 | 0.375 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-06 | 0.409 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-05 | 0.412 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-04 | 0.416 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-03 | 0.421 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-02 | 0.422 | NEUTRAL | 0.50 | 1.00 | 0.500 | 50.0% | 50.0% |
| 2026-07-01 | -2.505 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-30 | -2.891 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-29 | -2.956 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-28 | -3.056 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-27 | -3.056 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-26 | -3.058 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-25 | -3.027 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-24 | -2.992 | DEFENSIVE | 0.25 | 0.40 | 0.100 | 25.0% | 10.0% ⚠️ |
| 2026-06-23 | 1.621 | RISK_ON | 1.00 | 1.00 | 1.000 | 100.0% | 100.0% |
| 2026-06-22 | 2.208 | RISK_ON | 1.00 | 1.00 | 1.000 | 100.0% | 100.0% |
| 2026-06-21 | 2.034 | RISK_ON | 1.00 | 1.00 | 1.000 | 100.0% | 100.0% |
| 2026-06-20 | 2.260 | RISK_ON | 1.00 | 1.00 | 1.000 | 100.0% | 100.0% |

**Double-penalty days (Sharpe Guard fires AND regime is already NEUTRAL/DEFENSIVE): 14/30 = 47%.**

- On NEUTRAL double-penalty days, combined deployment would drop from 50% → 30% of capital (a further 40% relative cut on top of the existing regime cut).
- On DEFENSIVE double-penalty days, combined deployment would drop from 25% → 10% of capital — at this level the system is close to a cash position; whether 10% deployment is "too conservative" or "appropriate" depends on risk tolerance, but it is a materially different posture than today's 25%, not a minor adjustment.
- The 4 RISK_ON days never double-penalize in this sample (Sharpe was 1.6–2.3, well clear of both thresholds) — but that's an artifact of this particular 30-day window, not a structural guarantee: a RISK_ON regime with a bad rolling Sharpe is possible and would double-penalize too.

**Why this happens structurally:** `_risk_multiplier`'s NEUTRAL/DEFENSIVE tiers are already *partly* informed by Sharpe — `unified_regime`'s `health_score` component (`run_daily.py:1050-1057`) is derived from `rolling_sharpe`. So a low Sharpe is already pulling `unified_regime` toward NEUTRAL/DEFENSIVE (and therefore `_risk_multiplier` down) *before* Sharpe Guard would apply a second, independent Sharpe-based cut. This is the double-counting risk named in 2D.

---

## 2B — Retroactive OOS impact

271 resolved trades since OOS_START_DATE (2026-06-26), matched to the 30-day
history above by `signal_date`.

**Actual (current system, no Sharpe Guard in sizing) — all 271 trades:**
win rate 55.7%, avg return per trade +0.013%.

**Split by whether Sharpe Guard would have fired that day:**

| group | n | win rate | avg return |
|---|---:|---:|---:|
| Guard-would-fire days | 136 | 57.4% | **+0.054%** |
| Guard-would-NOT-fire days | 135 | 54.1% | −0.029% |

This is the key, non-obvious result: **per-trade win rate and average % return
do not change** under Sharpe Guard — sizing doesn't affect which picks were
selected or how they performed individually. What changes is the *dollar
weight* behind each trade. And in this window, the days Sharpe Guard would
have cut exposure hardest (the DEFENSIVE stretch, Sharpe −2.5 to −3.1) turned
out, in hindsight, to have the **better**-performing trades, not worse.

Exposure-weighted impact (sum of `actual_return%` as a proxy for $ P&L
contribution at each day's exposure level, vs the same sum scaled by the
hypothetical Sharpe Guard multiplier):

- Guard-fired-day trades (n=136): actual exposure-weighted sum **+7.41**, hypothetical (guard wired in) **+2.96** → **delta −4.45**.
- **Explicit answer to the framing question: retroactively "fixing" this makes the track record look WORSE, not better.** Wiring Sharpe Guard into sizing would have cut real gains during this window, not avoided losses. This is reported as found — it is not a reason to adopt or reject the guard, and it should not be read as such; it is one 30-day, 271-trade window, and the DEFENSIVE-regime Sharpe readings (−2.5 to −3.1) are unusually extreme, which raises the possibility of mean reversion/small-sample noise rather than a repeatable pattern. A longer replay (ideally spanning multiple regime cycles) would be needed before treating this as evidence either way.

---

## 2C — Threshold sensitivity

Same trade set, three configs:

| config | days fired / 30 | trades affected / 271 | actual sum | hypothetical sum | delta |
|---|---:|---:|---:|---:|---:|
| **CURRENT** (0.3→0.6×, 0.0→0.4×) | 14 | 136 | +7.41 | +2.96 | −4.45 |
| **ALT-B** (threshold 0.2, single floor 0.50×) | 8 | 136 | +7.41 | +3.70 | −3.70 |
| **ALT-C** (threshold 0.4, single floor 0.25×) | 21 | 211 | +16.12 | +4.03 | **−12.09** |

Trade count affected is identical between CURRENT and ALT-B (136) because
every day CURRENT's 0.6× tier fires, its Sharpe was actually < 0.2 too in this
sample (0.213–0.292) — the two thresholds happen to select the same day set
here, they are not structurally equivalent. ALT-C's higher (more permissive)
threshold roughly triples the affected-trade count and the P&L impact swings
much further negative — the outcome is **highly sensitive** to where the
threshold and floor are set. A ±0.1 change in threshold and ±0.15 change in
floor here swings the hypothetical delta from −3.70 to −12.09, a >3x range.
**This sensitivity suggests the current 0.3/0.6×/0.4× values do not have a
demonstrated principled basis in this codebase (no backtest or optimization
artifact was found justifying them) — they read as plausible round numbers,
not tuned parameters.**

---

## 2D — Recommendation memo

**Is wiring Sharpe Guard into real sizing net positive, net negative, or unclear?**
**Unclear, leaning toward "not yet" on current evidence.** The one real
OOS window available shows a negative effect (cut gains, not losses), the
result is highly sensitive to threshold/floor choice, and the mechanism would
double-count Sharpe risk that already partially feeds `_risk_multiplier`
through `health_score`. None of that rules out the guard being a good idea —
it may earn its keep over a longer sample, or in a regime where a bad Sharpe
and a good `unified_regime` genuinely diverge — but nothing in the data
collected here supports shipping it now.

**Interaction risk with `_risk_multiplier`:** They should **not** simply
multiply. `_risk_multiplier`'s NEUTRAL/DEFENSIVE tiers are already partly
driven by the same Sharpe signal via `health_score`. Multiplying a second,
independently-thresholded Sharpe factor on top double-counts that signal —
exactly the double-penalty pattern in 47% of the sampled days. Two structurally
sounder options, in order of preference:
1. **Sharpe-Guard-only-when-regime-is-clean**: only apply the guard's
   reduction when `unified_regime == RISK_ON` (i.e., when the regime signal
   itself hasn't already reacted to a bad Sharpe) — this is the "avoid
   double-penalizing" framing the brief asked about, and it directly targets
   the gap where a good `unified_regime` masks a genuinely bad rolling Sharpe.
2. **Fold Sharpe into `health_score`'s weight, don't add a second gate**:
   increase `health_score`'s 0.30 weight in `unified_score` (`run_daily.py:1059`,
   `strategy_version.py:81`) so a bad Sharpe pulls `unified_regime` down harder
   through the existing single channel, rather than adding a second
   independent multiplier. Simpler, avoids the double-count structurally
   instead of by a conditional patch.

**Recommended threshold/floor if implementing:** insufficient evidence here to
recommend specific numbers — 2C shows the outcome is too sensitive to the
current guesses to extend confidence to alternatives without a longer,
multi-regime-cycle replay. Any implementation should ship with the
threshold/floor as clearly-labeled, easily-adjustable constants (as
`strategy_version.py` already does for other frozen rules) so they can be
revisited once more OOS data exists, and should log actual vs. hypothetical
deployment for a probation period before it actually starts reducing real
size, mirroring how the PROBATION_CAP mechanism already treats new tickers.

**Explicit call-out:** This changes future position sizes starting from
whatever date it ships. It does not require restating past OOS performance —
but past performance (win rate, avg return, everything in `win_rate.json` /
`risk_report.json` to date) was generated **without** this control active, and
2B shows that had it been active, realized $ P&L over the sampled window would
have been lower, not higher. Framing this as "closing a risk-control gap"
would be accurate for the *concept*; framing it as "would have improved
results" would not be, on the data collected here.
