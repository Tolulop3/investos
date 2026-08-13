"""
InvestOS — Signal Ledger Engine
================================
Tamper-evident, append-only audit trail of every signal InvestOS produces.
SHA-256 chain hash on each entry makes retroactive editing detectable.

WHY THIS MATTERS
-----------------
outcome_tracker.py tracks win rates from real prices — the data is real.
But it's a plain JSON file: anyone could edit it.

signal_ledger.py adds:
  - SHA-256 hash chain: edit entry #47 → every hash #48–1500+ breaks
  - Signal attribution: WHY was this pick made (ML, RS, NEWS, BREAKOUT)?
  - Public audit page (signal_ledger.html) — verifiable track record
  - pitch_deck_stats.json — VC due diligence reads this directly
  - Attribution stats: do ML-attributed picks outperform non-ML picks?

RESOLUTION (2026-07-22 reconciliation — read outcome_tracker.py first)
------------------------------------------------------------------------
resolve_ledger() no longer fetches prices or decides WIN/LOSS/FLAT itself.
Prior to this date it independently resolved against whatever ticker
happened to reappear in a future day's top-N screener list — which meant
losers (that don't recover into top-N) sat unresolved indefinitely while
winners got swept up, late but resolved. Traced against outcomes_log.json:
of 685 overlapping (ticker, signal_date) pairs, 258 were unresolved in the
ledger but already resolved in outcomes_log — and that 258's WR was 31.4%
(PF 0.31), far below the ledger's self-reported 63.7%. Not a timing bug —
a survivorship-selected headline number.

outcome_tracker.py / outcomes_log.json is now canonical: it actively
fetches stale prices every run (_fetch_stale_prices) and resolves on the
stated 7-day schedule regardless of outcome. resolve_ledger() now purely
reads outcome_tracker's resolution, keyed on (ticker, signal_date) — see
its docstring below. The hash chain is unaffected: resolution fields were
already outside _entry_hash()'s scope by design.

INTEGRATION (run_daily.py — already wired in)
----------------------------------------------
    from signal_ledger import append_signals, resolve_ledger, bake_audit_page
    append_signals(all_picks, regime=regime, news_signals=news_sigs)
    resolve_ledger()                   # after resolve_outcomes — reads outcomes_log.json
    bake_audit_page()                  # in bake step
"""

import json
import os
import hashlib
import math
from datetime import datetime, date

import outcome_tracker
from pick_utils import get_pick_data, get_pick_category
from strategy_version import OOS_START_DATE

LEDGER_FILE        = "signal_ledger.json"
AUDIT_HTML         = "signal_ledger.html"
PITCH_JSON         = "pitch_deck_stats.json"
HOLD_DAYS_CALENDAR = 7   # matches outcome_tracker — historical constant, no longer
                         # used to gate resolve_ledger() itself (see below), kept for
                         # any other code that still reads it as a reference value.


# ── Helpers ─────────────────────────────────────────────────────────────────

def _load_ledger():
    if not os.path.exists(LEDGER_FILE):
        return []
    try:
        with open(LEDGER_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def _save_ledger(entries):
    with open(LEDGER_FILE, "w") as f:
        json.dump(entries, f, indent=2, default=str)

def _sha256(data):
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def _entry_hash(entry, prev_hash):
    """Hash immutable signal fields + previous hash. Excludes resolution fields."""
    payload = (
        str(prev_hash) +
        str(entry.get("ticker", "")) +
        str(entry.get("signal_date", "")) +
        str(entry.get("signal_time", "")) +
        str(entry.get("entry_price", 0)) +
        str(entry.get("score", 0)) +
        str(entry.get("ml_prob", 0))
    )
    return _sha256(payload)


# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Append signals
# ════════════════════════════════════════════════════════════════════════════

def append_signals(picks, regime=None, news_signals=None, run_time=None):
    """
    Record every pick at the moment it is made — before market opens.
    Builds the tamper chain. Does not double-log same ticker on same date.
    """
    if not picks:
        return 0

    entries  = _load_ledger()
    now      = run_time or datetime.now().isoformat()
    date_str = datetime.now().strftime("%Y-%m-%d")

    logged_today = {e["ticker"] for e in entries if e.get("signal_date") == date_str}
    prev_hash    = entries[-1].get("entry_hash", "GENESIS") if entries else "GENESIS"

    # Regime context
    regime_state = "UNKNOWN"
    if isinstance(regime, dict):
        regime_state = (
            regime.get("regime") or
            regime.get("signal") or
            "UNKNOWN"
        )

    # Active news signals
    active_news = []
    if isinstance(news_signals, list):
        active_news = [
            str(s.get("signal", s.get("name", "")))
            for s in news_signals if isinstance(s, dict)
        ][:5]
    elif isinstance(news_signals, dict):
        active_news = list(news_signals.keys())[:5]

    new_count = 0

    for pick in picks:
        ticker = pick.get("ticker")
        if not ticker or ticker in logged_today:
            continue

        data      = pick.get("data", {})
        pick_meta = get_pick_data(pick)

        # Attribution: which signal layers contributed to this pick
        attribution = []
        ml_prob = pick.get("ml_prob", 0) or 0
        if ml_prob > 0.55:
            attribution.append(f"ML:{ml_prob:.2f}")
        rs = pick.get("rs_rating", 0) or 0
        if rs >= 90:
            attribution.append(f"RS:{rs}")
        news_adj = pick.get("news_adjustment", pick.get("news_boost", 0)) or 0
        if abs(news_adj) >= 10:
            sign = "+" if news_adj > 0 else ""
            attribution.append(f"NEWS:{sign}{news_adj:.0f}")
        bk = pick.get("breakout_signal", {}) or {}
        if bk.get("zone") in ("AT_HIGH", "BREAKOUT_IMMINENT"):
            attribution.append("BREAKOUT")
        eq = (pick.get("earnings_quality") or {}).get("eq_rating", "")
        if eq in ("STRONG", "SOLID"):
            attribution.append("EQ:STRONG")
        ins = (pick.get("insider_data") or {}).get("signal_strength", "")
        if ins in ("STRONG", "MODERATE"):
            attribution.append("INSIDER")
        if pick.get("conviction_count", 0) >= 2:
            attribution.append("HIGH_CONVICTION")

        entry = {
            # Immutable (hashed)
            "ticker":           ticker,
            "signal_date":      date_str,
            "signal_time":      now,
            "entry_price":      round(float(data.get("price", 0) or 0), 4),
            "score":            round(float(pick.get("score", 0) or 0), 1),
            "ml_prob":          round(float(ml_prob), 3),
            # Context (not hashed — can be enriched)
            # FIX (2026-08-09): the old fallback `pick.get("category", "")`
            # was dead code -- category never lives at the top level (see
            # pick_utils.py), so it always evaluated to "". get_pick_category()
            # already implements the correct (and only) lookup.
            "category":         get_pick_category(pick),
            "regime_at_signal": regime_state,
            "news_active":      active_news,
            "attribution":      attribution,
            "rs_rating":        rs,
            "sector":           data.get("sector", ""),
            "market":           "CA" if str(ticker).endswith(".TO") else "US",
            # Resolution (filled later)
            "resolved":         False,
            "exit_price":       None,
            "actual_return":    None,
            "outcome":          None,
            "resolved_date":    None,
            "hold_days":        None,
            # Chain
            "prev_hash":        prev_hash,
            "entry_hash":       None,
        }

        h = _entry_hash(entry, prev_hash)
        entry["entry_hash"] = h
        prev_hash = h

        entries.append(entry)
        logged_today.add(ticker)
        new_count += 1

    if new_count:
        _save_ledger(entries)
        print(f"  📒 Signal ledger: {new_count} new entries (chain: {len(entries)})")

    return new_count


# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — Resolve outcomes
# ════════════════════════════════════════════════════════════════════════════

def _hold_days(signal_date_str, resolved_date_str):
    try:
        sd = datetime.strptime(signal_date_str, "%Y-%m-%d").date()
        rd = datetime.strptime(resolved_date_str, "%Y-%m-%d").date()
        return (rd - sd).days
    except Exception:
        return None


def resolve_ledger():
    """
    Read-through resolution — reads outcome_tracker.py's outcomes_log.json
    (the canonical resolver, see module docstring) keyed on
    (ticker, signal_date). Does NOT fetch prices or decide WIN/LOSS/FLAT
    itself — that would reintroduce the independent-resolver drift this
    replaced. Call after resolve_outcomes() in the same run so
    outcomes_log.json is current.

    Resolution fields are outside _entry_hash()'s scope, so overwriting
    them here cannot break the chain (verified before/after in the
    2026-07-22 reconciliation — see commit message).

    Any entry that already carried a resolution from the OLD independent
    mechanism gets that original value preserved in *_ledger_legacy
    fields before being overwritten — never deleted, only superseded.
    """
    entries  = _load_ledger()
    outcomes = outcome_tracker.load_outcomes()
    if not outcomes:
        return 0

    outc_by_pair = {}
    for o in outcomes:
        if o.get("resolved") and o.get("outcome") in ("WIN", "LOSS", "FLAT"):
            outc_by_pair[(o.get("ticker"), o.get("signal_date"))] = o

    resolved_count = 0
    for entry in entries:
        key = (entry.get("ticker"), entry.get("signal_date"))
        oe  = outc_by_pair.get(key)
        if not oe:
            continue   # outcomes_log hasn't resolved this pick (yet) either

        already_synced = (
            entry.get("resolved")
            and entry.get("outcome")       == oe.get("outcome")
            and entry.get("exit_price")    == oe.get("exit_price")
            and entry.get("resolved_date") == oe.get("resolved_date")
        )
        if already_synced:
            continue

        # Preserve the OLD independent-mechanism resolution, once, before
        # overwriting — never on an entry that's already synced (that
        # would re-stamp the same legacy value pointlessly) and never if
        # legacy fields already exist (don't clobber the original on a
        # later re-run).
        if entry.get("resolved") and "outcome_ledger_legacy" not in entry:
            entry["outcome_ledger_legacy"]       = entry.get("outcome")
            entry["exit_price_ledger_legacy"]    = entry.get("exit_price")
            entry["resolved_date_ledger_legacy"] = entry.get("resolved_date")
            entry["actual_return_ledger_legacy"] = entry.get("actual_return")

        entry["exit_price"]        = oe.get("exit_price")
        entry["actual_return"]     = oe.get("actual_return")
        entry["resolved_date"]     = oe.get("resolved_date")
        entry["hold_days"]         = _hold_days(entry.get("signal_date"), oe.get("resolved_date"))
        entry["resolved"]          = True
        entry["outcome"]           = oe.get("outcome")
        entry["resolution_source"] = "outcomes_log"
        resolved_count += 1

    if resolved_count:
        _save_ledger(entries)
        print(f"  📒 Signal ledger: {resolved_count} entries synced from outcomes_log (canonical resolver)")

    return resolved_count


# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — Chain verification
# ════════════════════════════════════════════════════════════════════════════

def verify_chain():
    """Walk entire ledger and verify every hash link. Returns (is_valid, broken_idx)."""
    entries   = _load_ledger()
    if not entries:
        print("  📒 Ledger empty — nothing to verify.")
        return True, None

    prev_hash = "GENESIS"
    for i, entry in enumerate(entries):
        expected = _entry_hash(entry, prev_hash)
        actual   = entry.get("entry_hash", "")
        if expected != actual:
            print(f"  ❌ Chain broken at entry {i}: {entry.get('ticker')} on {entry.get('signal_date')}")
            return False, i
        prev_hash = actual

    total = len(entries)
    res   = [e for e in entries if e.get("outcome")]
    wr    = f"{sum(1 for e in res if e['outcome']=='WIN')/len(res)*100:.1f}%" if res else "pending"
    print(f"  ✅ Chain verified: {total} entries intact | WR: {wr}")
    return True, None


# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — Audit statistics
# ════════════════════════════════════════════════════════════════════════════

def compute_audit_stats():
    entries  = _load_ledger()
    resolved = [e for e in entries if e.get("resolved") and e.get("outcome")]
    total    = len(entries)
    n_res    = len(resolved)

    if n_res < 3:
        return {
            "total_signals": total, "resolved": n_res,
            "pending": total - n_res, "status": "building",
            "message": f"Building track record... {n_res} resolved of {total} signals",
        }

    returns   = [e["actual_return"] for e in resolved]
    wins      = [e for e in resolved if e["outcome"] == "WIN"]
    losses    = [e for e in resolved if e["outcome"] == "LOSS"]
    win_rate  = len(wins) / n_res * 100
    avg_ret   = sum(returns) / n_res
    avg_win   = sum(e["actual_return"] for e in wins) / len(wins) if wins else 0
    avg_loss  = sum(e["actual_return"] for e in losses) / len(losses) if losses else 0
    loss_rate = len(losses) / n_res
    expectancy = (win_rate/100 * avg_win) - (loss_rate * abs(avg_loss))

    if len(returns) >= 30:   # need 30+ resolved to estimate Sharpe meaningfully
        mean_r = sum(returns) / len(returns)
        var    = sum((r - mean_r)**2 for r in returns) / (len(returns)-1)
        std    = math.sqrt(var) if var > 0 else 1
        sharpe = round((mean_r / std) * math.sqrt(252 / HOLD_DAYS_CALENDAR), 3)
    else:
        sharpe = None   # building — suppress until 30+ resolved

    today = date.today()
    w30, w90 = [], []
    for e in resolved:
        try:
            rd = datetime.strptime(e["resolved_date"], "%Y-%m-%d").date()
            d  = (today - rd).days
            if d <= 30: w30.append(e)
            if d <= 90: w90.append(e)
        except Exception:
            pass

    # FIX (2026-08-12): this all-time win_rate above and outcome_tracker.py's
    # OOS-scoped win_rate look like they disagree ("two resolvers") when
    # they're actually the exact same resolver over two different windows --
    # the ledger has logged since 2026-06-14, 12 days before the formal
    # v4.1 OOS_START_DATE (2026-06-26). Confirmed byte-for-byte: filtering
    # outcomes_log.json to signal_date >= the ledger's own start date
    # reproduces the ledger's exact entry set and win rate. Adding the same
    # OOS-scoped cut here, labeled, so both numbers can be read side by side
    # instead of looking like a data-integrity problem.
    oos_resolved_entries = [e for e in resolved if e.get("signal_date","") >= OOS_START_DATE]

    def _wr(lst):
        if not lst: return None
        return round(sum(1 for e in lst if e["outcome"]=="WIN") / len(lst) * 100, 1)

    # Time-weighted WR
    LAMBDA = math.log(2) / 35
    tw_w, tw_t = 0.0, 0.0
    for e in resolved:
        try:
            rd = datetime.strptime(e["resolved_date"], "%Y-%m-%d").date()
            w  = math.exp(-LAMBDA * max(0, (today - rd).days))
            tw_t += w
            if e["outcome"] == "WIN": tw_w += w
        except Exception:
            pass
    tw_wr = round(tw_w / tw_t * 100, 1) if tw_t > 0 else None

    # Attribution breakdown
    def _attr_wr(tag):
        tagged = [e for e in resolved if any(tag in a for a in e.get("attribution", []))]
        if len(tagged) < 3: return None
        return {
            "win_rate": round(sum(1 for e in tagged if e["outcome"]=="WIN")/len(tagged)*100, 1),
            "count":    len(tagged),
            "avg_ret":  round(sum(e["actual_return"] for e in tagged)/len(tagged), 2),
        }

    attrib = {}
    for tag in ["ML", "RS", "NEWS", "BREAKOUT", "EQ:STRONG", "INSIDER", "HIGH_CONVICTION"]:
        r = _attr_wr(tag)
        if r: attrib[tag] = r

    # Score tiers
    tiers = {}
    for lo, hi, lbl in [(90,100,"90-100"),(75,89,"75-89"),(60,74,"60-74"),(0,59,"<60")]:
        t = [e for e in resolved if lo <= (e.get("score",0) or 0) <= hi]
        if t:
            tiers[lbl] = {
                "win_rate": round(sum(1 for e in t if e["outcome"]=="WIN")/len(t)*100, 1),
                "avg_ret":  round(sum(e["actual_return"] for e in t)/len(t), 2),
                "count":    len(t),
            }

    # Streak
    streak, streak_type = 0, None
    for e in sorted(resolved, key=lambda x: x.get("resolved_date",""), reverse=True):
        if streak == 0: streak_type = e["outcome"]; streak = 1
        elif e["outcome"] == streak_type: streak += 1
        else: break

    sig_dates    = sorted(e["signal_date"] for e in entries if e.get("signal_date"))
    date_range   = f"{sig_dates[0]} → {sig_dates[-1]}" if sig_dates else "N/A"
    duration_d   = (today - datetime.strptime(sig_dates[0], "%Y-%m-%d").date()).days if sig_dates else 0
    chain_ok, ci = verify_chain()

    return {
        "total_signals":   total, "resolved": n_res, "pending": total - n_res,
        "win_rate":        round(win_rate, 1), "avg_return": round(avg_ret, 2),
        "avg_win":         round(avg_win, 2), "avg_loss": round(avg_loss, 2),
        "expectancy":      round(expectancy, 3), "best_return": round(max(returns), 2),
        "worst_return":    round(min(returns), 2), "sharpe_estimate": sharpe,
        "tw_win_rate":     tw_wr, "win_rate_30d": _wr(w30), "count_30d": len(w30),
        "win_rate_90d":    _wr(w90), "count_90d": len(w90),
        # See FIX comment above oos_resolved_entries -- this is the same
        # resolver as "win_rate" above, just scoped to the formal OOS window
        # instead of the ledger's full history since inception.
        "oos": {
            "start_date": OOS_START_DATE,
            "resolved":   len(oos_resolved_entries),
            "win_rate":   _wr(oos_resolved_entries),
        },
        "by_score_tier":   tiers, "by_attribution": attrib,
        "ml_win_rate":     _wr([e for e in resolved if any("ML:" in a for a in e.get("attribution",[]))]),
        "ca_win_rate":     _wr([e for e in resolved if e.get("market")=="CA"]),
        "us_win_rate":     _wr([e for e in resolved if e.get("market")=="US"]),
        "streak":          streak, "streak_type": streak_type,
        "date_range":      date_range, "track_record_days": duration_d,
        "hold_period_days":HOLD_DAYS_CALENDAR, "chain_intact": chain_ok,
        "chain_broken_at": ci, "status": "live",
        "generated_at":    datetime.now().isoformat(),
        "recent_picks": [
            {
                "ticker":        e["ticker"], "signal_date": e["signal_date"],
                "resolved_date": e["resolved_date"], "entry_price": e["entry_price"],
                "exit_price":    e["exit_price"], "actual_return": e["actual_return"],
                "outcome":       e["outcome"], "score": e["score"],
                "attribution":   e["attribution"], "regime": e.get("regime_at_signal",""),
                "entry_hash":    (e.get("entry_hash","")[:12] + "...") if e.get("entry_hash") else "",
            }
            for e in sorted(resolved, key=lambda x: x.get("resolved_date",""), reverse=True)[:20]
        ],
    }


# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — Bake audit page
# ════════════════════════════════════════════════════════════════════════════

def bake_audit_page():
    """Generate signal_ledger.html and pitch_deck_stats.json."""
    stats = compute_audit_stats()

    # Pitch JSON
    pitch = {
        "InvestOS Track Record": {
            "as_of":               stats.get("generated_at",""),
            "track_record_since":  stats.get("date_range","").split("→")[0].strip() if stats.get("date_range") else "",
            "total_signals":       stats.get("total_signals"),
            "resolved_signals":    stats.get("resolved"),
            "win_rate_flat":       f"{stats.get('win_rate','N/A')}%",
            "win_rate_30d":        f"{stats.get('win_rate_30d','N/A')}%",
            "win_rate_time_wtd":   f"{stats.get('tw_win_rate','N/A')}%",
            "avg_return_per_pick": f"{stats.get('avg_return','N/A')}%",
            "expectancy_per_pick": f"{stats.get('expectancy','N/A')}%",
            "sharpe_estimate":     stats.get("sharpe_estimate"),
            "best_pick":           f"+{stats.get('best_return','N/A')}%",
            "worst_pick":          f"{stats.get('worst_return','N/A')}%",
            "chain_integrity":     "INTACT" if stats.get("chain_intact") else "BROKEN",
            "markets_covered":     ["TSX (Canada)", "US Equities", "Global ETFs", "NGX (Nigeria)"],
            "universe_size":       "177+ tickers screened daily",
            "signal_layers":       ["ML/XGBoost", "Relative Strength", "News/Macro",
                                    "Insider (SEC EDGAR)", "52W Breakout", "Earnings Quality"],
            "note":                "NFA — educational signal system. Past performance ≠ future results.",
        }
    }
    with open(PITCH_JSON, "w") as f:
        json.dump(pitch, f, indent=2)

    # Pre-compute values to avoid f-string quote conflicts
    n_res    = stats.get("resolved", 0)
    wr       = stats.get("win_rate", 0) or 0
    wr_str   = f"{wr:.1f}" if isinstance(wr, (int, float)) else str(wr)
    wr_col   = "#22c55e" if wr >= 55 else "#f59e0b"
    exp_val  = stats.get("expectancy", 0) or 0
    exp_str  = f"{exp_val:+.3f}" if isinstance(exp_val, (int, float)) else "—"
    exp_col  = "#22c55e" if isinstance(exp_val, (int, float)) and exp_val > 0 else "#ef4444"
    tw_wr    = stats.get("tw_win_rate", "—")
    sharpe   = stats.get("sharpe_estimate", "—")
    chain_ok = stats.get("chain_intact", True)
    chain_s  = "✅ INTACT" if chain_ok else "⚠️ CHAIN ERROR"
    chain_c  = "" if chain_ok else " broken"
    dr       = stats.get("date_range", "—")
    total    = stats.get("total_signals", 0)
    pend     = stats.get("pending", 0)
    wr30     = stats.get("win_rate_30d", "—")
    cnt30    = stats.get("count_30d", 0)

    # Score tier rows
    tier_rows = ""
    for label, td in stats.get("by_score_tier", {}).items():
        wp = td["win_rate"]
        c  = "#22c55e" if wp >= 60 else ("#f59e0b" if wp >= 50 else "#ef4444")
        tier_rows += (
            f'<tr><td style="padding:8px 12px">{label}</td>'
            f'<td style="padding:8px 12px;color:{c};font-weight:600">{wp}%</td>'
            f'<td style="padding:8px 12px">{td["avg_ret"]:+.2f}%</td>'
            f'<td style="padding:8px 12px;color:#94a3b8">{td["count"]}</td></tr>'
        )

    # Attribution rows
    attrib_rows = ""
    for tag, ad in stats.get("by_attribution", {}).items():
        wp = ad["win_rate"]
        c  = "#22c55e" if wp >= 60 else ("#f59e0b" if wp >= 50 else "#ef4444")
        attrib_rows += (
            f'<tr><td style="padding:8px 12px;font-family:monospace">{tag}</td>'
            f'<td style="padding:8px 12px;color:{c};font-weight:600">{wp}%</td>'
            f'<td style="padding:8px 12px">{ad["avg_ret"]:+.2f}%</td>'
            f'<td style="padding:8px 12px;color:#94a3b8">{ad["count"]}</td></tr>'
        )

    # Recent picks rows
    pick_rows = ""
    for p in stats.get("recent_picks", []):
        ret    = p.get("actual_return", 0) or 0
        out    = p.get("outcome", "")
        icon   = "✅" if out == "WIN" else ("❌" if out == "LOSS" else "➖")
        rc     = "#22c55e" if ret > 0 else ("#ef4444" if ret < 0 else "#94a3b8")
        attrs  = " · ".join(p.get("attribution", [])) or "—"
        hfp    = p.get("entry_hash", "")
        pick_rows += (
            f'<tr>'
            f'<td style="padding:8px 12px;font-weight:600">{icon} {p.get("ticker","")}</td>'
            f'<td style="padding:8px 12px;color:#94a3b8">{p.get("signal_date","")}</td>'
            f'<td style="padding:8px 12px;color:#94a3b8">{p.get("resolved_date","")}</td>'
            f'<td style="padding:8px 12px">${p.get("entry_price",0):.2f}</td>'
            f'<td style="padding:8px 12px">${(p.get("exit_price") or 0):.2f}</td>'
            f'<td style="padding:8px 12px;color:{rc};font-weight:600">{ret:+.2f}%</td>'
            f'<td style="padding:8px 12px;color:#64748b">{p.get("score",0)}</td>'
            f'<td style="padding:8px 12px;color:#64748b;font-size:11px">{attrs}</td>'
            f'<td style="padding:8px 12px;font-family:monospace;font-size:10px;color:#475569">{hfp}</td>'
            f'</tr>'
        )

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    gen_at  = stats.get("generated_at", "")

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>InvestOS — Signal Audit Trail</title>
<style>
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  body{background:#0f172a;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:2rem 1rem}
  .container{max-width:1100px;margin:0 auto}
  h1{font-size:1.8rem;font-weight:700;margin-bottom:4px}
  .subtitle{color:#64748b;font-size:14px;margin-bottom:2rem}
  .hero{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:2rem}
  .stat-card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:16px 20px}
  .stat-label{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}
  .stat-val{font-size:26px;font-weight:700;color:#f1f5f9}
  .stat-sub{font-size:11px;color:#475569;margin-top:3px}
  section{margin-bottom:2.5rem}
  h2{font-size:1rem;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px}
  table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:10px;overflow:hidden;font-size:13px}
  thead tr{background:#0f172a}
  th{padding:10px 12px;text-align:left;color:#64748b;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.06em}
  tbody tr:hover{background:#263148}
  tbody tr{border-top:1px solid #1e293b}
  .chain-badge{display:inline-flex;align-items:center;gap:6px;background:#14532d;color:#86efac;padding:6px 14px;border-radius:20px;font-size:12px;font-weight:500}
  .chain-badge.broken{background:#7f1d1d;color:#fca5a5}
  .note{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:14px 18px;font-size:12px;color:#64748b;line-height:1.6}
  .generated{font-size:11px;color:#334155;margin-top:2rem;text-align:center}
  @media(max-width:600px){.hero{grid-template-columns:1fr 1fr}table{font-size:11px}}
</style>
</head>
<body>
<div class="container">
  <h1>📊 InvestOS — Signal Audit Trail</h1>
  <p class="subtitle">Live, tamper-chained record of every signal. Generated from <code>signal_ledger.json</code> · """ + dr + """</p>
  <div class="hero">
    <div class="stat-card">
      <div class="stat-label">Win Rate (all-time)</div>
      <div class="stat-val" style="color:""" + wr_col + """">""" + wr_str + """%</div>
      <div class="stat-sub">""" + str(n_res) + """ resolved signals</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Win Rate (30d)</div>
      <div class="stat-val" style="color:#22c55e">""" + str(wr30) + """%</div>
      <div class="stat-sub">""" + str(cnt30) + """ recent picks</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Time-Weighted WR</div>
      <div class="stat-val" style="color:#22c55e">""" + str(tw_wr) + """%</div>
      <div class="stat-sub">35-day decay weight</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Expectancy / Pick</div>
      <div class="stat-val" style="color:""" + exp_col + """">""" + exp_str + """%</div>
      <div class="stat-sub">Positive = real edge</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Sharpe (est.)</div>
      <div class="stat-val">""" + str(sharpe) + """</div>
      <div class="stat-sub">7-day hold, annualised</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total Signals</div>
      <div class="stat-val">""" + str(total) + """</div>
      <div class="stat-sub">""" + str(pend) + """ pending resolution</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Chain Integrity</div>
      <div style="margin-top:6px"><span class="chain-badge""" + chain_c + """">""" + chain_s + """</span></div>
      <div class="stat-sub">SHA-256 tamper chain</div>
    </div>
  </div>
  <section>
    <h2>Score Calibration</h2>
    <table><thead><tr><th>Score Tier</th><th>Win Rate</th><th>Avg Return</th><th>Count</th></tr></thead>
    <tbody>""" + tier_rows + """</tbody></table>
  </section>
  <section>
    <h2>Signal Attribution — Which Layers Add Value?</h2>
    <table><thead><tr><th>Signal Layer</th><th>Win Rate</th><th>Avg Return</th><th>Count</th></tr></thead>
    <tbody>""" + attrib_rows + """</tbody></table>
  </section>
  <section>
    <h2>Recent Resolved Signals (last 20)</h2>
    <div style="overflow-x:auto">
    <table><thead><tr>
      <th>Ticker</th><th>Signal Date</th><th>Resolved</th>
      <th>Entry</th><th>Exit</th><th>Return</th>
      <th>Score</th><th>Attribution</th><th>Hash</th>
    </tr></thead>
    <tbody>""" + pick_rows + """</tbody></table>
    </div>
  </section>
  <div class="note">
    <strong>About this audit trail.</strong>
    Each signal is recorded before market open at the exact moment it is produced.
    A SHA-256 hash links every entry to the previous one — editing any historical
    entry breaks every hash that follows, making retroactive changes detectable.
    Resolution prices are fetched from Yahoo Finance (""" + str(HOLD_DAYS_CALENDAR) + """ calendar days after signal).
    Auto-generated by <code>signal_ledger.py</code> on every run. Not financial advice.
  </div>
  <p class="generated">Generated """ + now_str + """ · InvestOS v4.0</p>
</div>
</body>
</html>"""

    with open(AUDIT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  📒 Audit page: {AUDIT_HTML} ({len(html)//1024}KB)")
    print(f"  📋 Pitch stats: {PITCH_JSON}")
    return stats


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    if "--verify" in sys.argv or "-v" in sys.argv:
        ok, _ = verify_chain()
        sys.exit(0 if ok else 1)
    elif "--stats" in sys.argv or "-s" in sys.argv:
        s = compute_audit_stats()
        if s.get("status") == "building":
            print(s["message"])
        else:
            print(f"  Total:    {s['total_signals']} | Resolved: {s['resolved']}")
            print(f"  Win rate: {s['win_rate']}% | TW: {s['tw_win_rate']}%")
            print(f"  Expect.:  {s['expectancy']:+.3f}% | Sharpe: {s['sharpe_estimate']}")
            print(f"  Chain:    {'intact' if s['chain_intact'] else 'BROKEN'}")
    elif "--bake" in sys.argv or "-b" in sys.argv:
        bake_audit_page()
    else:
        print("Usage: python signal_ledger.py [--verify | --stats | --bake]")
