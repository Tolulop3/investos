"""
etf_signal_store.py — InvestOS ETF signal storage (ledger + score history)
============================================================================
Gives the ETF engine the same storage discipline the NA stock engine has:

  1. etf_signal_ledger.json — append-only, SHA-256 hash-chained record of
     every scored ETF signal (mirrors signal_ledger.py's doctrine: the
     hash covers the immutable signal fields; resolution fields live
     outside the hash and are synced in later from etf_outcomes.json).
     Until 2026-10-26 the ETF engine's only memory was etf_signals.json —
     overwritten daily — plus whatever the outcome tracker captured.
  2. etf_score_history.json — one score snapshot per ticker per day
     (mirrors intelligence_layers' score_history), so score evolution
     and calibration can be audited across the whole universe, not just
     the tickers the outcome tracker happened to log.

The ledger logs the SAME set as etf_outcome_tracker.log_etf_signals
(the full scored list), keyed (ticker, signal_date), so ledger and
outcomes reconcile 1:1. Both writers are idempotent per day: re-running
the daily pipeline never duplicates an entry.

Storage only — no scoring, threshold, or pick logic lives here.
"""

import hashlib
import json
import os
from datetime import datetime

LEDGER_FILE = "etf_signal_ledger.json"
HISTORY_FILE = "etf_score_history.json"
HISTORY_CAP = 500          # snapshots kept per ticker (~2 trading years)


def _sha256(data):
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _etf_entry_hash(entry, prev_hash):
    """Hash the immutable signal fields + previous hash (resolution excluded)."""
    payload = (
        str(prev_hash) +
        str(entry.get("ticker", "")) +
        str(entry.get("signal_date", "")) +
        str(entry.get("signal_time", "")) +
        str(entry.get("entry_price", 0)) +
        str(entry.get("score", 0)) +
        str(entry.get("category", "")) +
        str(entry.get("ret_90", 0)) +
        str(entry.get("above_ma200", ""))
    )
    return _sha256(payload)


# ── Ledger ───────────────────────────────────────────────────────────────────

def _load_ledger():
    if os.path.exists(LEDGER_FILE):
        try:
            with open(LEDGER_FILE) as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


def _save_ledger(entries):
    with open(LEDGER_FILE, "w") as f:
        json.dump(entries, f, indent=2, default=str)


def append_etf_signals(etf_result, run_time=None):
    """
    Append today's scored ETF signals to the hash-chained ledger.
    Returns the number of NEW entries (0 on a same-day re-run).
    """
    if not etf_result:
        return 0
    scored = etf_result.get("scored", [])
    if not scored:
        return 0

    entries = _load_ledger()
    today_str = datetime.now().strftime("%Y-%m-%d")
    now = run_time or datetime.now().isoformat()
    existing = {(e.get("ticker"), e.get("signal_date")) for e in entries}
    prev_hash = entries[-1].get("entry_hash", "GENESIS") if entries else "GENESIS"
    regime = etf_result.get("regime", "NEUTRAL")

    rrsp_tickers = {p["ticker"] for p in etf_result.get("rrsp_picks", [])}
    tfsa_tickers = {p["ticker"] for p in etf_result.get("tfsa_picks", [])}
    fhsa_tickers = {p["ticker"] for p in etf_result.get("fhsa_picks", [])}

    new_count = 0
    for e in scored:
        ticker = e.get("ticker")
        if not ticker or (ticker, today_str) in existing:
            continue
        entry = {
            # Immutable (hashed)
            "ticker":           ticker,
            "signal_date":      today_str,
            "signal_time":      now,
            "entry_price":      e.get("price"),
            "score":            e.get("score"),
            "category":         e.get("category"),
            "ret_30":           e.get("ret_30"),
            "ret_90":           e.get("ret_90"),
            "above_ma200":      e.get("above_ma200"),
            # Context (not hashed — mirrors signal_ledger.py doctrine)
            "name":             e.get("name", ticker),
            "track":            e.get("track"),
            "acct_pref":        e.get("acct_pref"),
            "sector_signal":    e.get("sector_signal"),
            "signal_boost":     e.get("signal_boost"),
            "range_pct":        e.get("range_pct"),
            "overextended":     e.get("overextended"),
            "div_yield":        e.get("div_yield"),
            "div_flag":         e.get("div_flag"),
            "regime_at_signal": regime,
            "reasons":          e.get("reasons", []),
            "acct_flags": {
                "rrsp": ticker in rrsp_tickers,
                "tfsa": ticker in tfsa_tickers,
                "fhsa": ticker in fhsa_tickers,
            },
            # Resolution (filled by sync_ledger_resolutions; outside hash)
            "resolved":         False,
            "resolved_date":    None,
            "exit_price":       None,
            "actual_return_pct": None,
            "outcome":          None,
            "prev_hash":        prev_hash,
            "entry_hash":       None,
        }
        h = _etf_entry_hash(entry, prev_hash)
        entry["entry_hash"] = h
        prev_hash = h
        entries.append(entry)
        existing.add((ticker, today_str))
        new_count += 1

    if new_count:
        _save_ledger(entries)
        print(f"   🔗 ETF ledger: appended {new_count} signals ({len(entries)} total)")
    return new_count


def verify_etf_chain():
    """
    Recompute the whole chain. Returns (ok, n_entries, first_bad_index).
    first_bad_index is None when the chain verifies.
    """
    entries = _load_ledger()
    prev_hash = "GENESIS"
    for i, entry in enumerate(entries):
        if entry.get("prev_hash") != prev_hash:
            return False, len(entries), i
        if _etf_entry_hash(entry, prev_hash) != entry.get("entry_hash"):
            return False, len(entries), i
        prev_hash = entry["entry_hash"]
    return True, len(entries), None


def sync_ledger_resolutions():
    """
    Copy resolution state from etf_outcomes.json into matching ledger
    entries (same (ticker, signal_date) key). Returns entries updated.
    Resolution fields are outside the hash, so the chain still verifies.
    """
    try:
        from etf_outcome_tracker import load_etf_outcomes
        outcomes = load_etf_outcomes()
    except Exception:
        return 0
    resolved = {(o.get("ticker"), o.get("signal_date")): o
                for o in outcomes if o.get("resolved")}
    if not resolved:
        return 0
    entries = _load_ledger()
    updated = 0
    for entry in entries:
        if entry.get("resolved"):
            continue
        o = resolved.get((entry.get("ticker"), entry.get("signal_date")))
        if not o:
            continue
        entry["resolved"] = True
        entry["resolved_date"] = o.get("resolved_date")
        entry["exit_price"] = o.get("exit_price")
        entry["actual_return_pct"] = o.get("actual_return_pct")
        entry["outcome"] = o.get("outcome")
        updated += 1
    if updated:
        _save_ledger(entries)
        print(f"   🔗 ETF ledger: synced {updated} resolutions")
    return updated


# ── Score history ────────────────────────────────────────────────────────────

def _load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2, default=str)


def update_etf_score_history(etf_result, run_time=None):
    """
    One snapshot per scored ticker per day (same-day re-runs overwrite
    that day's snapshot). Returns the number of tickers recorded.
    """
    if not etf_result:
        return 0
    scored = etf_result.get("scored", [])
    if not scored:
        return 0
    history = _load_history()
    today_str = datetime.now().strftime("%Y-%m-%d")
    for e in scored:
        ticker = e.get("ticker")
        if not ticker:
            continue
        snap = {
            "date":        today_str,
            "score":       e.get("score"),
            "price":       e.get("price"),
            "ret_30":      e.get("ret_30"),
            "ret_90":      e.get("ret_90"),
            "above_ma200": e.get("above_ma200"),
        }
        series = history.setdefault(ticker, [])
        idx = next((i for i, h in enumerate(series) if h.get("date") == today_str), None)
        if idx is not None:
            series[idx] = snap
        else:
            series.append(snap)
        del series[:-HISTORY_CAP]
    _save_history(history)
    return len(scored)
