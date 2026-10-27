"""
test_sim_book.py — unit tests for sim_book.py (synthetic bars, no network).

Run: python -m pytest test_sim_book.py -q
"""
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sim_book as sb

BASE = date(2026, 10, 20)  # a Tuesday


def bars(*rows):
    """rows: (day_offset, open, high, low, close) relative to BASE."""
    return [{"date": BASE + timedelta(days=o), "open": o_, "high": h,
             "low": l, "close": c} for (o, o_, h, l, c) in rows]


def pos(**kw):
    p = {
        "symbol": "EURUSD=X", "label": "EUR/USD", "direction": "LONG",
        "conviction": 70, "entry_date": BASE.isoformat(), "entry": 1.1000,
        "target": 1.1250, "stop": 1.0900, "hold_days": 7,
        "expiry_date": (BASE + timedelta(days=7)).isoformat(),
        "risk_amount": 100.0, "units": 10000.0, "status": "open",
        "exit_date": None, "exit_price": None, "exit_reason": None,
        "gross_pnl": None, "cost": None, "net_pnl": None, "r_multiple": None,
    }
    p.update(kw)
    return p


# ── Path resolution ────────────────────────────────────────────

def test_long_target_hit():
    p = pos()
    out = sb.resolve_position("fx", p, bars(
        (1, 1.1000, 1.1100, 1.0950, 1.1050),
        (2, 1.1050, 1.1260, 1.1000, 1.1200),   # high breaches target
    ), BASE + timedelta(days=2))
    assert out is not None and out["exit_reason"] == "target"
    assert out["exit_price"] == 1.1250
    assert out["gross_pnl"] == 250.0
    assert out["cost"] == 1.20                 # 10,000 units x 0.00012 spread
    assert out["net_pnl"] == 248.80
    assert out["r_multiple"] == 2.488


def test_long_stop_hit():
    p = pos()
    out = sb.resolve_position("fx", p, bars(
        (1, 1.1000, 1.1050, 1.0890, 1.0920),   # low breaches stop
    ), BASE + timedelta(days=1))
    assert out["exit_reason"] == "stop" and out["exit_price"] == 1.0900
    assert out["net_pnl"] == -101.20           # -100 gross - 1.20 cost


def test_same_day_both_hit_stop_wins():
    p = pos()
    out = sb.resolve_position("fx", p, bars(
        (1, 1.1000, 1.1300, 1.0850, 1.1200),   # both target and stop in range
    ), BASE + timedelta(days=1))
    assert out["exit_reason"] == "stop"


def test_entry_day_bar_never_walked():
    p = pos()
    out = sb.resolve_position("fx", p, bars(
        (0, 1.1000, 1.1300, 1.0800, 1.1200),   # wild entry-day bar — ignored
        (1, 1.1200, 1.1210, 1.1190, 1.1205),   # quiet day after
    ), BASE + timedelta(days=1))
    assert out is None                          # still open, no false trigger


def test_short_mirror():
    p = pos(direction="SHORT", target=1.0750, stop=1.1100)
    out = sb.resolve_position("fx", p, bars(
        (1, 1.1000, 1.1050, 1.0740, 1.0800),   # low breaches target (short)
    ), BASE + timedelta(days=1))
    assert out["exit_reason"] == "target" and out["gross_pnl"] == 250.0


def test_expiry_settles_at_close():
    p = pos(hold_days=3, expiry_date=(BASE + timedelta(days=3)).isoformat())
    out = sb.resolve_position("fx", p, bars(
        (1, 1.1000, 1.1100, 1.0980, 1.1050),
        (2, 1.1050, 1.1080, 1.1000, 1.1020),
        (3, 1.1020, 1.1040, 1.0990, 1.1010),   # expiry day close = settle
    ), BASE + timedelta(days=4))
    assert out["exit_reason"] == "expiry" and out["exit_price"] == 1.1010
    assert out["gross_pnl"] == 10.0


def test_still_open_before_expiry():
    p = pos()
    out = sb.resolve_position("fx", p, bars(
        (1, 1.1000, 1.1100, 1.0980, 1.1050),
    ), BASE + timedelta(days=1))
    assert out is None


# ── Opening rules ──────────────────────────────────────────────

def _call(**kw):
    c = {"symbol": "EURUSD=X", "label": "EUR/USD", "direction": "LONG",
         "conviction": 70, "entry": 1.1000, "target": 1.1250,
         "stop": 1.0900, "hold_days": 7, "key_driver": "test",
         "entry_date": BASE}
    c.update(kw)
    return c


def test_open_sizes_at_one_pct_risk():
    book = sb.new_book("fx")
    assert sb._open_position("fx", book, _call(), BASE) is True
    p = book["positions"][0]
    assert p["risk_amount"] == 100.0           # 1% of 10,000
    assert p["units"] == 10000.0               # 100 / |1.10 - 1.09|
    assert p["expiry_date"] == (BASE + timedelta(days=7)).isoformat()


def test_one_position_per_symbol_and_skip_recorded():
    book = sb.new_book("fx")
    assert sb._open_position("fx", book, _call(), BASE) is True
    # same call next day while still open -> skipped, not stacked
    assert sb._open_position("fx", book, _call(entry_date=BASE + timedelta(days=1)),
                             BASE + timedelta(days=1)) is False
    assert len(book["positions"]) == 1
    assert len(book["skipped"]) == 1
    assert book["skipped"][0]["reason"] == "position_open"


def test_same_day_duplicate_ignored():
    book = sb.new_book("fx")
    assert sb._open_position("fx", book, _call(), BASE) is True
    assert sb._open_position("fx", book, _call(), BASE) is False
    assert len(book["positions"]) == 1 and len(book["skipped"]) == 0


def test_reentry_after_close_allowed():
    book = sb.new_book("fx")
    sb._open_position("fx", book, _call(), BASE)
    book["positions"][0]["status"] = "closed"
    book["positions"][0]["net_pnl"] = 50.0
    nxt = BASE + timedelta(days=2)
    assert sb._open_position("fx", book, _call(entry_date=nxt), nxt) is True
    # sizing now risks 1% of 10,050 realized equity
    assert book["positions"][1]["risk_amount"] == 100.50


# ── Costs + stats ──────────────────────────────────────────────

def test_crypto_cost_model():
    p = pos(symbol="BTC-USD", label="BTC/USD", entry=60000.0,
            target=70000.0, stop=55000.0, units=0.02, risk_amount=100.0)
    out = sb.resolve_position("crypto_official", p, bars(
        (1, 60000.0, 71000.0, 59000.0, 69000.0),
    ), BASE + timedelta(days=1))
    assert out["exit_reason"] == "target"
    assert out["gross_pnl"] == 200.0
    assert out["cost"] == round(0.02 * 60000.0 * 0.0030, 2)  # 3.60


def test_book_stats_math():
    book = sb.new_book("fx")
    book["positions"] = [
        dict(pos(), status="closed", net_pnl=200.0, r_multiple=2.0,
             exit_date="2026-10-22"),
        dict(pos(), status="closed", net_pnl=-100.0, r_multiple=-1.0,
             exit_date="2026-10-23", symbol="USDJPY=X"),
        dict(pos(), status="open", symbol="GBPUSD=X"),
    ]
    s = sb.book_stats(book)
    assert s["trades_closed"] == 2 and s["wins"] == 1 and s["losses"] == 1
    assert s["win_rate"] == 50.0
    assert s["profit_factor"] == 2.0
    assert s["expectancy_r"] == 0.5
    assert s["total_net_pnl"] == 100.0 and s["equity"] == 10100.0
    assert s["positions_open"] == 1
    assert s["by_symbol"]["EURUSD=X"]["net_pnl"] == 200.0


def test_parse_hold_days():
    assert sb._parse_hold_days("1-3 days", 5) == 3
    assert sb._parse_hold_days("3-7 days", 5) == 7
    assert sb._parse_hold_days("14-30 days", 14) == 30
    assert sb._parse_hold_days(None, 5) == 5
    assert sb._parse_hold_days(10, 5) == 10


# ── Driver flow with a fake fetcher ────────────────────────────

def test_update_fx_sim_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fx_result = {"active_calls": [{
        "pair": "EUR/USD", "symbol": "EURUSD=X", "direction": "LONG",
        "conviction": 73, "entry": 1.1000, "price": 1.1000,
        "target": 1.1250, "stop": 1.0900, "hold_period": "3-7 days",
        "key_driver": "test",
    }]}
    def fake_fetch(symbol, start, end):
        return bars((1, 1.1000, 1.1260, 1.0990, 1.1200))
    stats = sb.update_fx_sim(fx_result, today=BASE, fetcher=fake_fetch)
    assert stats["positions_open"] == 1 and stats["trades_closed"] == 0
    # next day: position resolves at target
    stats = sb.update_fx_sim({"active_calls": []},
                             today=BASE + timedelta(days=1),
                             fetcher=fake_fetch)
    assert stats["trades_closed"] == 1 and stats["wins"] == 1
    saved = json.load(open("fx_sim_book.json"))
    assert saved["positions"][0]["exit_reason"] == "target"
