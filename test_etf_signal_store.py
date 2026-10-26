"""
test_etf_signal_store.py — ETF storage discipline tests (2026-10-26).

Covers: hash-chain append/verify/tamper detection, same-day idempotence,
resolution sync staying outside the hash, score-history same-day
overwrite, feature-complete outcome rows, and the div_yield unit fix.
All file paths are monkeypatched to tmp dirs — the real data files are
never touched.
"""

import json
from datetime import datetime, timedelta

import pytest

import etf_signal_store as store
import etf_outcome_tracker as tracker
from etf_engine import _norm_yield_pct


def _entry(ticker, score, price, **kw):
    base = {
        "ticker": ticker, "name": ticker, "category": "CORE", "track": "core",
        "acct_pref": "ALL", "sector_signal": None, "overlap_group": "g",
        "mer": 0.1, "thesis": "index", "score": score, "reasons": ["r1"],
        "ret_30": 1.5, "ret_90": 4.2, "price": price, "above_ma200": True,
        "signal_boost": 0, "range_pct": 55, "overextended": False,
        "dca_recommended": False, "div_yield": 2.39, "div_flag": "NORMAL",
        "energy_confirmed": None,
    }
    base.update(kw)
    return base


def _result(entries, picks=()):
    return {
        "regime": "NEUTRAL",
        "scored": entries,
        "rrsp_picks": [e for e in entries if e["ticker"] in picks],
        "tfsa_picks": [],
        "fhsa_picks": [],
    }


@pytest.fixture()
def store_files(tmp_path, monkeypatch):
    ledger = tmp_path / "etf_signal_ledger.json"
    history = tmp_path / "etf_score_history.json"
    outcomes = tmp_path / "etf_outcomes.json"
    monkeypatch.setattr(store, "LEDGER_FILE", str(ledger))
    monkeypatch.setattr(store, "HISTORY_FILE", str(history))
    monkeypatch.setattr(tracker, "ETF_OUTCOMES_FILE", str(outcomes))
    return ledger, history, outcomes


# ── div_yield normalization ──────────────────────────────────────────────────

def test_norm_yield_pct_units():
    assert _norm_yield_pct(0.0239) == 2.39     # fraction form (stocks)
    assert _norm_yield_pct(2.39) == 2.39       # percent form (many ETFs — the XEG case)
    assert _norm_yield_pct(239.0) == 2.39      # already-100x-scaled repair
    assert _norm_yield_pct(12.5) == 12.5       # high-yield (covered call) percent
    assert _norm_yield_pct(0.125) == 12.5      # high-yield fraction form
    assert _norm_yield_pct(None) is None
    assert _norm_yield_pct(0) is None
    assert _norm_yield_pct("n/a") is None


# ── ledger: append, chain, idempotence, tamper ───────────────────────────────

def test_ledger_append_and_chain(store_files):
    entries = [_entry("AAA", 70.0, 10.0), _entry("BBB", 60.0, 20.0),
               _entry("CCC", 50.0, 30.0)]
    assert store.append_etf_signals(_result(entries, picks=("AAA",))) == 3
    ok, n, bad = store.verify_etf_chain()
    assert ok and n == 3 and bad is None
    ledger = json.loads(store_files[0].read_text())
    assert ledger[0]["prev_hash"] == "GENESIS"
    assert ledger[1]["prev_hash"] == ledger[0]["entry_hash"]
    assert ledger[0]["acct_flags"]["rrsp"] is True
    assert ledger[1]["acct_flags"]["rrsp"] is False
    assert ledger[0]["resolved"] is False


def test_ledger_same_day_rerun_is_idempotent(store_files):
    entries = [_entry("AAA", 70.0, 10.0)]
    assert store.append_etf_signals(_result(entries)) == 1
    assert store.append_etf_signals(_result(entries)) == 0
    ok, n, _ = store.verify_etf_chain()
    assert ok and n == 1


def test_ledger_tamper_detected(store_files):
    store.append_etf_signals(_result([_entry("AAA", 70.0, 10.0),
                                      _entry("BBB", 60.0, 20.0)]))
    ledger = json.loads(store_files[0].read_text())
    ledger[1]["score"] = 99.0                      # rewrite history
    store_files[0].write_text(json.dumps(ledger))
    ok, n, bad = store.verify_etf_chain()
    assert not ok and bad == 1


def test_ledger_resolution_sync_stays_outside_hash(store_files):
    today = datetime.now().strftime("%Y-%m-%d")
    store.append_etf_signals(_result([_entry("AAA", 70.0, 10.0)]))
    outcomes = [{
        "ticker": "AAA", "signal_date": today, "resolved": True,
        "resolved_date": today, "exit_price": 11.0,
        "actual_return_pct": 10.0, "outcome": "WIN",
    }]
    store_files[2].write_text(json.dumps(outcomes))
    assert store.sync_ledger_resolutions() == 1
    ledger = json.loads(store_files[0].read_text())
    assert ledger[0]["outcome"] == "WIN" and ledger[0]["resolved"] is True
    ok, _, _ = store.verify_etf_chain()            # resolution must not break the chain
    assert ok
    assert store.sync_ledger_resolutions() == 0   # idempotent


# ── score history ────────────────────────────────────────────────────────────

def test_score_history_same_day_overwrite_and_next_day_append(store_files, monkeypatch):
    store.update_etf_score_history(_result([_entry("AAA", 70.0, 10.0)]))
    store.update_etf_score_history(_result([_entry("AAA", 72.0, 10.5)]))  # re-run
    hist = json.loads(store_files[1].read_text())
    assert len(hist["AAA"]) == 1 and hist["AAA"][0]["score"] == 72.0

    real_dt = datetime
    class _FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt.now() + timedelta(days=1)
    monkeypatch.setattr(store, "datetime", _FakeDT)
    store.update_etf_score_history(_result([_entry("AAA", 65.0, 10.2)]))
    hist = json.loads(store_files[1].read_text())
    assert len(hist["AAA"]) == 2 and hist["AAA"][1]["score"] == 65.0


# ── outcome rows carry the feature snapshot ──────────────────────────────────

def test_outcome_rows_carry_features(store_files):
    e = _entry("AAA", 70.0, 10.0, sector_signal="TECH", signal_boost=15,
               reasons=["Strong 90d"])
    assert tracker.log_etf_signals(_result([e], picks=())) == 1
    rows = json.loads(store_files[2].read_text())
    row = rows[0]
    assert row["ret_90"] == 4.2 and row["ret_30"] == 1.5
    assert row["above_ma200"] is True and row["range_pct"] == 55
    assert row["sector_signal"] == "TECH" and row["signal_boost"] == 15
    assert row["div_yield"] == 2.39 and row["reasons"] == ["Strong 90d"]
    assert row["score_at_signal"] == 70.0 and row["entry_price"] == 10.0
