"""Unit tests for etf_outcome_tracker.py — classify, resolve, log — and the
one-off migrate_etf_sector_split.py.

Motivation: as of 2026-08-29 etf_outcomes.json had 600+ logged signals and
0 resolved, which looked like a resolution bug. It is not: the tracker was
added 2026-08-07 and RESOLUTION_DAYS is 30, so the first signal only becomes
eligible on 2026-09-06. These tests lock in that resolve_etf_outcomes() does
the right thing once signals age past the window, so that milestone is not a
surprise. Also covers the 2026-08-29 SECTOR -> SECTOR_COMMODITY/SECTOR_EARNINGS
split (threshold table, threshold_used recording, legacy alias, migration).

Run: python3 test_etf_outcome_tracker.py
"""

import json
import os
import sys
import tempfile
import types
from datetime import datetime, timedelta

import etf_outcome_tracker as t
import migrate_etf_sector_split as m


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _use_temp_outcomes_file():
    """Point the module at a throwaway outcomes file. Returns its path."""
    fd, path = tempfile.mkstemp(suffix=".json", prefix="etf_outcomes_test_")
    os.close(fd)
    os.unlink(path)  # start absent — load_etf_outcomes() handles that
    t.ETF_OUTCOMES_FILE = path
    return path


def _days_ago(n):
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")


def _entry(ticker="VOO", category="CORE", signal_date=None,
           entry_price=100.0, resolved=False):
    return {
        "ticker": ticker,
        "name": ticker,
        "category": category,
        "signal_date": signal_date or _days_ago(40),
        "signal_time": "2026-01-01T00:00:00",
        "score_at_signal": 70,
        "entry_price": entry_price,
        "regime_at_signal": "NEUTRAL",
        "acct_flags": {"rrsp": True, "tfsa": False, "fhsa": False},
        "resolved": resolved,
        "resolved_date": None,
        "exit_price": None,
        "actual_return_pct": None,
        "outcome": None,
    }


def _write(entries):
    json.dump(entries, open(t.ETF_OUTCOMES_FILE, "w"), indent=2, default=str)


def _read():
    return json.load(open(t.ETF_OUTCOMES_FILE))


def _install_fake_etf_engine(price_by_ticker):
    """Stub etf_engine so resolve's per-ticker fetch fallback is hermetic
    (real etf_engine imports yfinance, which isn't a test dependency)."""
    m = types.ModuleType("etf_engine")

    def _fetch_etf_data(ticker):
        p = price_by_ticker.get(ticker)
        return {"price": p} if p is not None else None

    m._fetch_etf_data = _fetch_etf_data
    sys.modules["etf_engine"] = m


def _remove_fake_etf_engine():
    sys.modules.pop("etf_engine", None)


# ─── _classify_etf_outcome ───────────────────────────────────────────────────

def test_classify_boundary_is_flat():
    # strict >, so a return exactly at the threshold is FLAT, not WIN
    assert t._classify_etf_outcome(0.75, "CORE") == "FLAT"
    assert t._classify_etf_outcome(-0.75, "CORE") == "FLAT"


def test_classify_win_loss_core():
    assert t._classify_etf_outcome(0.76, "CORE") == "WIN"
    assert t._classify_etf_outcome(-0.76, "CORE") == "LOSS"
    assert t._classify_etf_outcome(0.10, "CORE") == "FLAT"


def test_classify_uses_category_threshold():
    # DEFENSIVE threshold is 0.25 — a move that's FLAT for CORE is a WIN here
    assert t._classify_etf_outcome(0.30, "DEFENSIVE") == "WIN"
    assert t._classify_etf_outcome(0.30, "CORE") == "FLAT"
    # THEMATIC threshold is 1.50 — a move that's a WIN for CORE is FLAT here
    assert t._classify_etf_outcome(1.00, "THEMATIC") == "FLAT"
    assert t._classify_etf_outcome(1.60, "THEMATIC") == "WIN"


def test_classify_unknown_category_uses_default():
    assert t._classify_etf_outcome(0.80, "MYSTERY") == "WIN"      # > 0.75 default
    assert t._classify_etf_outcome(0.50, "MYSTERY") == "FLAT"
    assert t._classify_etf_outcome(0.80, None) == "WIN"


def test_classify_sector_split_commodity_vs_earnings():
    # Same +0.90% move: FLAT for SECTOR_COMMODITY (band 1.00), WIN for
    # SECTOR_EARNINGS (band 0.75) — the whole reason for splitting the category.
    assert t._classify_etf_outcome(0.90, "SECTOR_COMMODITY") == "FLAT"
    assert t._classify_etf_outcome(0.90, "SECTOR_EARNINGS") == "WIN"
    assert t._classify_etf_outcome(-0.90, "SECTOR_COMMODITY") == "FLAT"
    assert t._classify_etf_outcome(-0.90, "SECTOR_EARNINGS") == "LOSS"
    # commodity keeps the old lumped 1.00 band exactly
    assert t._classify_etf_outcome(1.01, "SECTOR_COMMODITY") == "WIN"
    assert t._classify_etf_outcome(1.00, "SECTOR_COMMODITY") == "FLAT"


def test_classify_legacy_sector_alias_still_resolves():
    # Pre-split logged entries carry "category": "SECTOR"; the alias keeps them
    # resolving at the old 1.00 band instead of silently hitting the 0.75 default.
    assert t._threshold_for("SECTOR") == 1.00
    assert t._classify_etf_outcome(0.90, "SECTOR") == "FLAT"
    assert t._classify_etf_outcome(1.10, "SECTOR") == "WIN"


def test_threshold_for_matches_table():
    for cat, band in t.CATEGORY_THRESHOLDS.items():
        assert t._threshold_for(cat) == band
    assert t._threshold_for("nope") == t.DEFAULT_THRESHOLD_PCT


# ─── resolve_etf_outcomes ────────────────────────────────────────────────────

def test_resolve_skips_signals_younger_than_window():
    _use_temp_outcomes_file()
    _write([_entry(signal_date=_days_ago(10))])
    n = t.resolve_etf_outcomes(current_prices={"VOO": 110.0})
    assert n == 0, f"expected 0 resolved, got {n}"
    assert _read()[0]["resolved"] is False


def test_resolve_marks_eligible_signal_with_current_price():
    _use_temp_outcomes_file()
    _write([_entry(ticker="VOO", category="CORE",
                   signal_date=_days_ago(31), entry_price=100.0)])
    n = t.resolve_etf_outcomes(current_prices={"VOO": 105.0})
    assert n == 1, f"expected 1 resolved, got {n}"
    row = _read()[0]
    assert row["resolved"] is True
    assert row["exit_price"] == 105.0
    assert row["actual_return_pct"] == 5.0, row["actual_return_pct"]
    assert row["outcome"] == "WIN"          # +5% > CORE 0.75 threshold
    assert row["resolved_date"] == datetime.now().date().isoformat()


def test_resolve_classifies_loss_and_flat():
    _use_temp_outcomes_file()
    _write([
        _entry(ticker="XLE", category="SECTOR_COMMODITY", signal_date=_days_ago(45), entry_price=100.0),
        _entry(ticker="TLT", category="DEFENSIVE", signal_date=_days_ago(45), entry_price=100.0),
    ])
    n = t.resolve_etf_outcomes(current_prices={"XLE": 97.0, "TLT": 100.10})
    assert n == 2, f"expected 2 resolved, got {n}"
    rows = {r["ticker"]: r for r in _read()}
    assert rows["XLE"]["outcome"] == "LOSS"   # -3% < -1.0 SECTOR_COMMODITY band
    assert rows["XLE"]["threshold_used"] == 1.00
    assert rows["TLT"]["outcome"] == "FLAT"   # +0.1% within DEFENSIVE 0.25 band
    assert rows["TLT"]["threshold_used"] == 0.25


def test_resolve_records_threshold_used_for_split_categories():
    _use_temp_outcomes_file()
    _write([
        _entry(ticker="ZEB.TO", category="SECTOR_EARNINGS", signal_date=_days_ago(40), entry_price=100.0),
        _entry(ticker="XEG.TO", category="SECTOR_COMMODITY", signal_date=_days_ago(40), entry_price=100.0),
    ])
    t.resolve_etf_outcomes(current_prices={"ZEB.TO": 100.9, "XEG.TO": 100.9})
    rows = {r["ticker"]: r for r in _read()}
    # +0.9%: WIN for earnings (0.75 band), FLAT for commodity (1.00 band)
    assert (rows["ZEB.TO"]["outcome"], rows["ZEB.TO"]["threshold_used"]) == ("WIN", 0.75)
    assert (rows["XEG.TO"]["outcome"], rows["XEG.TO"]["threshold_used"]) == ("FLAT", 1.00)


def test_resolve_missing_entry_price_stays_pending():
    _use_temp_outcomes_file()
    _write([_entry(signal_date=_days_ago(40), entry_price=None)])
    n = t.resolve_etf_outcomes(current_prices={"VOO": 110.0})
    assert n == 0, f"expected 0 resolved, got {n}"
    assert _read()[0]["resolved"] is False


def test_resolve_nonpositive_entry_price_stays_pending():
    _use_temp_outcomes_file()
    _write([_entry(signal_date=_days_ago(40), entry_price=0.0)])
    n = t.resolve_etf_outcomes(current_prices={"VOO": 110.0})
    assert n == 0
    assert _read()[0]["resolved"] is False


def test_resolve_uses_fetch_fallback_when_not_in_current_prices():
    _use_temp_outcomes_file()
    _write([_entry(ticker="ARKG", category="THEMATIC",
                   signal_date=_days_ago(40), entry_price=50.0)])
    _install_fake_etf_engine({"ARKG": 50.5})
    try:
        n = t.resolve_etf_outcomes(current_prices={})   # force fallback
    finally:
        _remove_fake_etf_engine()
    assert n == 1, f"expected 1 resolved via fallback, got {n}"
    row = _read()[0]
    assert row["resolved"] is True
    assert row["exit_price"] == 50.5
    assert row["actual_return_pct"] == 1.0, row["actual_return_pct"]
    assert row["outcome"] == "FLAT"   # +1% within THEMATIC 1.5 threshold


def test_resolve_missing_exit_price_stays_pending():
    _use_temp_outcomes_file()
    _write([_entry(ticker="ARKG", signal_date=_days_ago(40), entry_price=50.0)])
    _install_fake_etf_engine({})   # fetch returns None for everything
    try:
        n = t.resolve_etf_outcomes(current_prices={})
    finally:
        _remove_fake_etf_engine()
    assert n == 0, f"expected 0 resolved, got {n}"
    assert _read()[0]["resolved"] is False


def test_resolve_leaves_already_resolved_untouched():
    _use_temp_outcomes_file()
    done = _entry(ticker="VOO", signal_date=_days_ago(90), entry_price=100.0, resolved=True)
    done["exit_price"] = 100.0
    done["actual_return_pct"] = 0.0
    done["outcome"] = "FLAT"
    done["resolved_date"] = "2026-01-01"
    _write([done])
    n = t.resolve_etf_outcomes(current_prices={"VOO": 999.0})
    assert n == 0, f"expected 0 newly resolved, got {n}"
    row = _read()[0]
    assert row["exit_price"] == 100.0          # not overwritten with 999
    assert row["resolved_date"] == "2026-01-01"


def test_resolve_empty_file_returns_zero():
    _use_temp_outcomes_file()
    assert t.resolve_etf_outcomes(current_prices={}) == 0


# ─── log_etf_signals ─────────────────────────────────────────────────────────

def _etf_result():
    return {
        "scored": [
            {"ticker": "VOO", "name": "S&P 500", "category": "CORE", "score": 80, "price": 500.0},
            {"ticker": "ARKG", "name": "Genomics", "category": "THEMATIC", "score": 60, "price": 25.0},
        ],
        "rrsp_picks": [{"ticker": "VOO"}],
        "tfsa_picks": [],
        "fhsa_picks": [],
        "regime": "RISK_ON",
    }


def test_log_writes_one_entry_per_scored_ticker():
    _use_temp_outcomes_file()
    n = t.log_etf_signals(_etf_result())
    assert n == 2, f"expected 2 logged, got {n}"
    rows = {r["ticker"]: r for r in _read()}
    assert rows["VOO"]["entry_price"] == 500.0
    assert rows["VOO"]["category"] == "CORE"
    assert rows["VOO"]["signal_date"] == datetime.now().strftime("%Y-%m-%d")
    assert rows["VOO"]["acct_flags"]["rrsp"] is True
    assert rows["ARKG"]["acct_flags"]["rrsp"] is False
    assert rows["VOO"]["resolved"] is False


def test_log_is_idempotent_within_a_day():
    _use_temp_outcomes_file()
    t.log_etf_signals(_etf_result())
    n2 = t.log_etf_signals(_etf_result())
    assert n2 == 0, f"expected 0 logged on second same-day call, got {n2}"
    assert len(_read()) == 2


def test_log_empty_result_is_noop():
    _use_temp_outcomes_file()
    assert t.log_etf_signals({}) == 0
    assert t.log_etf_signals({"scored": []}) == 0


def test_log_entry_has_threshold_used_field():
    _use_temp_outcomes_file()
    t.log_etf_signals(_etf_result())
    assert _read()[0]["threshold_used"] is None   # set only at resolution


# ─── migrate_etf_sector_split ────────────────────────────────────────────────

def test_migrate_relabels_sector_by_ticker():
    rows = [
        {"ticker": "XEG.TO", "category": "SECTOR"},
        {"ticker": "XLE",    "category": "SECTOR"},
        {"ticker": "GLD",    "category": "SECTOR"},
        {"ticker": "ZGD.TO", "category": "SECTOR"},
        {"ticker": "ZEB.TO", "category": "SECTOR"},
        {"ticker": "XRE.TO", "category": "SECTOR"},
        {"ticker": "VOO",    "category": "CORE"},        # untouched
        {"ticker": "TLT",    "category": "DEFENSIVE"},   # untouched
    ]
    changed, unmapped = m.migrate(rows)
    assert changed == 6 and unmapped == []
    cats = {r["ticker"]: r["category"] for r in rows}
    assert cats["XEG.TO"] == cats["XLE"] == cats["GLD"] == cats["ZGD.TO"] == "SECTOR_COMMODITY"
    assert cats["ZEB.TO"] == cats["XRE.TO"] == "SECTOR_EARNINGS"
    assert cats["VOO"] == "CORE" and cats["TLT"] == "DEFENSIVE"


def test_migrate_is_idempotent():
    rows = [{"ticker": "XLE", "category": "SECTOR"}, {"ticker": "ZEB.TO", "category": "SECTOR"}]
    m.migrate(rows)
    changed, unmapped = m.migrate(rows)          # second pass
    assert changed == 0 and unmapped == []
    assert [r["category"] for r in rows] == ["SECTOR_COMMODITY", "SECTOR_EARNINGS"]


def test_migrate_flags_unmapped_sector_ticker():
    rows = [{"ticker": "MYSTERY.TO", "category": "SECTOR"}]
    changed, unmapped = m.migrate(rows)
    assert changed == 0 and unmapped == ["MYSTERY.TO"]
    assert rows[0]["category"] == "SECTOR"       # left alone, alias still resolves it


def test_migrated_categories_all_exist_in_threshold_table():
    for cat in ("SECTOR_COMMODITY", "SECTOR_EARNINGS", "SECTOR"):
        assert cat in t.CATEGORY_THRESHOLDS


# ─── Runner ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_classify_boundary_is_flat,
        test_classify_win_loss_core,
        test_classify_uses_category_threshold,
        test_classify_unknown_category_uses_default,
        test_classify_sector_split_commodity_vs_earnings,
        test_classify_legacy_sector_alias_still_resolves,
        test_threshold_for_matches_table,
        test_resolve_skips_signals_younger_than_window,
        test_resolve_marks_eligible_signal_with_current_price,
        test_resolve_classifies_loss_and_flat,
        test_resolve_records_threshold_used_for_split_categories,
        test_resolve_missing_entry_price_stays_pending,
        test_resolve_nonpositive_entry_price_stays_pending,
        test_resolve_uses_fetch_fallback_when_not_in_current_prices,
        test_resolve_missing_exit_price_stays_pending,
        test_resolve_leaves_already_resolved_untouched,
        test_resolve_empty_file_returns_zero,
        test_log_writes_one_entry_per_scored_ticker,
        test_log_is_idempotent_within_a_day,
        test_log_empty_result_is_noop,
        test_log_entry_has_threshold_used_field,
        test_migrate_relabels_sector_by_ticker,
        test_migrate_is_idempotent,
        test_migrate_flags_unmapped_sector_ticker,
        test_migrated_categories_all_exist_in_threshold_table,
    ]
    passed = failed = 0
    for tc in tests:
        try:
            tc()
            print(f"  PASS  {tc.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {tc.__name__}: {e}")
            failed += 1
        except Exception as e:
            import traceback
            print(f"  ERROR {tc.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed}/{passed+failed} passed")
    if failed:
        raise SystemExit(1)
