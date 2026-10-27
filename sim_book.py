"""
sim_book.py — InvestOS Simulated Trade Books (FX + Crypto)
============================================================
The FX and crypto engines issue directional calls with an entry, a
target and a stop — but their outcome trackers only stamp the price
when a flat window expires and call it WIN/LOSS. That is not how the
trade would have lived: a stop hit on day one is a loss even if the
price later recovers, and a tally of wins says nothing about money.

This module keeps a SIMULATED PORTFOLIO per engine — paper money,
never real orders — under these rules (v1, 2026-10-26):

  - $10,000 starting capital per book; each position risks 1% of the
    book's realized equity at open (size = risk / |entry - stop|).
  - Path-based resolution from daily OHLC bars: starting the bar
    AFTER the entry day, a position closes at its target (+R), at its
    stop (-1R), or at the close of the last bar on/before its hold
    expiry. If target and stop both fall inside one day's range, the
    STOP is assumed hit first (conservative — never flatter the book).
  - Costs are charged on every trade: a stated round-trip spread per
    FX pair; 0.30% of notional round-trip for crypto (fees+slippage).
  - One open position per instrument. Calls that arrive while a
    position is open are recorded as skipped, never stacked.
  - FX book: opens from the engine's active_calls (the same set the
    outcome tracker logs). It was genesis-built by replaying the
    logged call history (fx_outcomes.json carries entry/target/stop
    for every call) under these same rules.
  - Crypto gets TWO books, never mixed in reporting: the OFFICIAL
    book takes only calls the engine stands behind (directional,
    conviction >= 65 — the tracker's own bar); the SHADOW book takes
    every directional call, so sub-threshold calls still generate
    calibration evidence.

Signal logic is untouched — this is measurement, the same doctrine
as the NA outcome book, applied to engines whose trades are defined
by a target/stop path rather than a holding horizon.
"""

import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

START_CAPITAL = 10000.0
RISK_PCT = 0.01

# Crypto official-book gate — mirrors crypto_outcome_tracker.py's
# ACTIVE_CALL_MIN_CONVICTION so the sim grades the same calls the
# official outcome book grades.
CRYPTO_OFFICIAL_MIN_CONVICTION = 65

# Round-trip spread cost, in price units, per FX symbol. Deliberately
# a touch conservative vs typical retail spreads.
FX_SPREAD = {
    "EURUSD=X": 0.00012,
    "GBPUSD=X": 0.00015,
    "USDJPY=X": 0.012,
    "JPY=X":    0.012,   # the engine's actual USD/JPY symbol
    "USDCAD=X": 0.00015,
    "CAD=X":    0.00015, # the engine's actual USD/CAD symbol
    "XAUUSD=X": 0.35,
    "GC=F":     0.35,    # the engine's actual gold symbol
}
FX_SPREAD_DEFAULT = 0.0002
FX_GOLD_SPREAD = 0.35

# Crypto round-trip cost as a fraction of notional (0.15%/side).
CRYPTO_COST_PCT = 0.0030

BOOK_FILES = {
    "fx":              "fx_sim_book.json",
    "crypto_official": "crypto_sim_book.json",
    "crypto_shadow":   "crypto_sim_shadow.json",
}

_HOLD_FALLBACK = {"fx": 5, "crypto_official": 14, "crypto_shadow": 14}


# ============================================================
# HELPERS
# ============================================================

def _today():
    return datetime.now().date()


def _parse_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_hold_days(text, fallback):
    """'3-7 days' -> 7 (the stated upper bound); '14-30 days' -> 30."""
    if text is None:
        return fallback
    if isinstance(text, (int, float)):
        return int(text)
    nums = [int(n) for n in re.findall(r"\d+", str(text))]
    return max(nums) if nums else fallback


def fetch_daily_bars(symbol, start_date, end_date):
    """
    Daily OHLC bars from Yahoo's chart API (the engines' own source).
    Returns [{date, open, high, low, close}] ascending; [] on failure —
    callers leave positions open and retry next run rather than guess.
    """
    try:
        p1 = int(datetime(start_date.year, start_date.month, start_date.day).timestamp())
        end_excl = end_date + timedelta(days=1)
        p2 = int(datetime(end_excl.year, end_excl.month, end_excl.day).timestamp())
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
               + urllib.parse.quote(symbol, safe="")
               + f"?period1={p1}&period2={p2}&interval=1d&events=history")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        result = (payload.get("chart", {}).get("result") or [None])[0]
        if not result:
            return []
        stamps = result.get("timestamp") or []
        quote = ((result.get("indicators", {}).get("quote") or [{}])[0])
        bars = []
        for i, ts in enumerate(stamps):
            try:
                o, h = quote["open"][i], quote["high"][i]
                l, c = quote["low"][i], quote["close"][i]
            except Exception:
                continue
            if None in (o, h, l, c):
                continue
            bars.append({
                "date":  datetime.utcfromtimestamp(ts).date(),
                "open":  float(o), "high": float(h),
                "low":   float(l), "close": float(c),
            })
        bars.sort(key=lambda b: b["date"])
        return bars
    except Exception:
        return []


# ============================================================
# BOOK STATE
# ============================================================

def new_book(kind):
    return {
        "book":      kind,
        "created":   datetime.now().isoformat(),
        "config": {
            "start_capital": START_CAPITAL,
            "risk_pct":      RISK_PCT,
            "cost_model":    "fx: per-symbol round-trip spread (price units); "
                             "crypto: 0.30% of notional round trip",
            "resolution":    "daily OHLC path from the bar after entry; "
                             "stop wins same-day ties; expiry settles at close",
        },
        "positions": [],
        "skipped":   [],
        "stats":     {},
    }


def load_book(kind):
    path = BOOK_FILES[kind]
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, dict) and "positions" in data:
                return data
        except Exception:
            pass
    return new_book(kind)


def save_book(kind, book):
    book["stats"] = book_stats(book)
    try:
        with open(BOOK_FILES[kind], "w") as f:
            json.dump(book, f, indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️  Sim book save failed ({kind}): {e}")
    return book


def book_equity(book):
    """Realized equity: start capital + closed net P&L."""
    closed = sum(p.get("net_pnl", 0) for p in book["positions"]
                 if p.get("status") == "closed")
    return START_CAPITAL + closed


# ============================================================
# POSITIONS
# ============================================================

def _trade_cost(kind, pos):
    if kind == "fx":
        spread = FX_SPREAD.get(pos["symbol"])
        if spread is None:
            spread = FX_GOLD_SPREAD if "gold" in (pos.get("label") or "").lower() \
                     else FX_SPREAD_DEFAULT
        return pos["units"] * spread
    return pos["units"] * pos["entry"] * CRYPTO_COST_PCT


def _close_position(kind, pos, exit_date, exit_price, reason):
    sign = 1 if pos["direction"] == "LONG" else -1
    gross = pos["units"] * (exit_price - pos["entry"]) * sign
    cost = _trade_cost(kind, pos)
    net = gross - cost
    pos["status"]      = "closed"
    pos["exit_date"]   = exit_date.isoformat()
    pos["exit_price"]  = round(exit_price, 5)
    pos["exit_reason"] = reason
    pos["gross_pnl"]   = round(gross, 2)
    pos["cost"]        = round(cost, 2)
    pos["net_pnl"]     = round(net, 2)
    pos["r_multiple"]  = round(net / pos["risk_amount"], 3) if pos["risk_amount"] else 0
    return pos


def resolve_position(kind, pos, bars, today):
    """
    Pure path resolution. `bars` = daily bars covering entry..today.
    Returns the closed position dict, or None if still open.
    The entry day's own bar is never walked — the signal fired
    intraday at the entry price, so that day's full range would
    grade moves that happened before the call existed.
    """
    entry_date = _parse_date(pos["entry_date"])
    expiry     = _parse_date(pos["expiry_date"])
    if entry_date is None or expiry is None:
        return None

    path = [b for b in bars if entry_date < b["date"] <= expiry]
    for bar in path:
        if pos["direction"] == "LONG":
            hit_stop   = bar["low"]  <= pos["stop"]
            hit_target = bar["high"] >= pos["target"]
        else:
            hit_stop   = bar["high"] >= pos["stop"]
            hit_target = bar["low"]  <= pos["target"]
        if hit_stop:      # stop wins ties — conservative by design
            return _close_position(kind, pos, bar["date"], pos["stop"], "stop")
        if hit_target:
            return _close_position(kind, pos, bar["date"], pos["target"], "target")

    if today > expiry:
        settle = [b for b in bars if entry_date < b["date"] <= expiry]
        if settle:
            last = settle[-1]
            return _close_position(kind, pos, last["date"], last["close"], "expiry")
    return None


def _open_position(kind, book, call, today):
    """
    Open a sim position from a normalized call:
    {symbol, label, direction, conviction, entry, target, stop,
     hold_days, key_driver}
    One open position per symbol; a repeat of the same day's call is
    ignored (the engines re-emit held calls on every run).
    """
    symbol = call.get("symbol")
    direction = call.get("direction")
    entry, target, stop = call.get("entry"), call.get("target"), call.get("stop")
    if not symbol or direction not in ("LONG", "SHORT"):
        return False
    if None in (entry, target, stop) or entry <= 0 or stop == entry:
        return False

    entry_date = call.get("entry_date") or today
    for p in book["positions"]:
        if p["symbol"] == symbol and p["entry_date"] == entry_date.isoformat():
            return False  # this exact call is already in the book
    for p in book["positions"]:
        if p["symbol"] == symbol and p.get("status") == "open":
            book["skipped"].append({
                "date": entry_date.isoformat(), "symbol": symbol,
                "direction": direction,
                "conviction": call.get("conviction"),
                "reason": "position_open",
            })
            return False

    risk_amount = round(book_equity(book) * RISK_PCT, 2)
    units = risk_amount / abs(entry - stop)
    hold_days = call.get("hold_days") or _HOLD_FALLBACK[kind]
    pos = {
        "id":          f"{symbol}-{entry_date.isoformat()}-{len(book['positions']) + 1}",
        "symbol":      symbol,
        "label":       call.get("label") or symbol,
        "direction":   direction,
        "conviction":  call.get("conviction"),
        "entry_date":  entry_date.isoformat(),
        "entry":       entry,
        "target":      target,
        "stop":        stop,
        "hold_days":   hold_days,
        "expiry_date": (entry_date + timedelta(days=hold_days)).isoformat(),
        "risk_amount": risk_amount,
        "units":       round(units, 6),
        "key_driver":  call.get("key_driver"),
        "status":      "open",
        "exit_date":   None, "exit_price": None, "exit_reason": None,
        "gross_pnl":   None, "cost": None, "net_pnl": None, "r_multiple": None,
    }
    book["positions"].append(pos)
    return True


def settle_book(kind, book, today, fetcher=fetch_daily_bars):
    """Resolve every open position against bars through `today`."""
    cache = {}
    closed = 0
    for pos in book["positions"]:
        if pos.get("status") != "open":
            continue
        symbol = pos["symbol"]
        if symbol not in cache:
            start = _parse_date(pos["entry_date"]) or today
            cache[symbol] = fetcher(symbol, start, today)
        if resolve_position(kind, pos, cache[symbol], today) is not None:
            closed += 1
    return closed


# ============================================================
# STATS
# ============================================================

def book_stats(book):
    closed = [p for p in book["positions"] if p.get("status") == "closed"]
    opened = [p for p in book["positions"] if p.get("status") == "open"]
    wins   = [p for p in closed if p.get("net_pnl", 0) > 0]
    losses = [p for p in closed if p.get("net_pnl", 0) <= 0]
    gross_win  = sum(p["net_pnl"] for p in wins)
    gross_loss = -sum(p["net_pnl"] for p in losses)
    total_net  = sum(p.get("net_pnl", 0) for p in closed)

    # Equity curve by exit date + peak-to-trough drawdown
    curve = [(book.get("created", "")[:10], START_CAPITAL)]
    equity = START_CAPITAL
    for p in sorted(closed, key=lambda x: x.get("exit_date") or ""):
        equity += p.get("net_pnl", 0)
        curve.append((p.get("exit_date"), round(equity, 2)))
    peak, max_dd = START_CAPITAL, 0.0
    for _, eq in curve:
        peak = max(peak, eq)
        max_dd = min(max_dd, eq - peak)

    by_symbol = {}
    for p in closed:
        s = by_symbol.setdefault(p["symbol"],
                                 {"trades": 0, "wins": 0, "net_pnl": 0.0})
        s["trades"] += 1
        s["wins"] += 1 if p.get("net_pnl", 0) > 0 else 0
        s["net_pnl"] = round(s["net_pnl"] + p.get("net_pnl", 0), 2)

    return {
        "trades_closed":  len(closed),
        "positions_open": len(opened),
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       round(100 * len(wins) / len(closed), 1) if closed else 0,
        "profit_factor":  round(gross_win / gross_loss, 2) if gross_loss > 0 else None,
        "expectancy_r":   round(sum(p.get("r_multiple", 0) for p in closed) / len(closed), 3) if closed else 0,
        "total_net_pnl":  round(total_net, 2),
        "equity":         round(START_CAPITAL + total_net, 2),
        "max_drawdown":   round(max_dd, 2),
        "skipped_calls":  len(book.get("skipped", [])),
        "by_symbol":      by_symbol,
        "equity_curve":   [{"date": d, "equity": e} for d, e in curve],
    }


# ============================================================
# ENGINE DRIVERS
# ============================================================

def update_fx_sim(fx_result, today=None, fetcher=fetch_daily_bars):
    """Daily maintenance of the FX sim book from a live engine result."""
    today = today or _today()
    book = load_book("fx")
    settle_book("fx", book, today, fetcher)
    opened = 0
    for call in (fx_result or {}).get("active_calls", []):
        norm = {
            "symbol":    call.get("symbol"),
            "label":     call.get("pair"),
            "direction": call.get("direction"),
            "conviction": call.get("conviction"),
            "entry":     call.get("entry") or call.get("price"),
            "target":    call.get("target"),
            "stop":      call.get("stop"),
            "hold_days": _parse_hold_days(call.get("hold_period"),
                                          _HOLD_FALLBACK["fx"]),
            "key_driver": call.get("key_driver"),
            "entry_date": today,
        }
        if _open_position("fx", book, norm, today):
            opened += 1
    save_book("fx", book)
    if opened:
        print(f"   🎰 FX sim: opened {opened} positions ({len(book['positions'])} total)")
    return book["stats"]


def _crypto_call(symbol, asset, today):
    return {
        "symbol":    symbol,
        "label":     asset.get("name") or symbol,
        "direction": asset.get("direction"),
        "conviction": asset.get("conviction"),
        "entry":     asset.get("entry") or asset.get("price"),
        "target":    asset.get("target"),
        "stop":      asset.get("stop"),
        "hold_days": _parse_hold_days(asset.get("hold_period"), 14),
        "key_driver": asset.get("key_driver"),
        "entry_date": today,
    }


def update_crypto_sim(crypto_result, today=None, fetcher=fetch_daily_bars):
    """Daily maintenance of BOTH crypto books (official + shadow)."""
    today = today or _today()
    assets = (crypto_result or {}).get("assets", {})
    out = {}
    for kind, min_conv in (("crypto_official", CRYPTO_OFFICIAL_MIN_CONVICTION),
                           ("crypto_shadow", 1)):
        book = load_book(kind)
        settle_book(kind, book, today, fetcher)
        for symbol, asset in assets.items():
            if asset.get("direction") not in ("LONG", "SHORT"):
                continue
            if (asset.get("conviction") or 0) < min_conv:
                continue
            _open_position(kind, book, _crypto_call(symbol, asset, today), today)
        save_book(kind, book)
        out[kind] = book["stats"]
    return out


# ============================================================
# FX GENESIS — replay the logged call history under sim rules
# ============================================================

def rescore_fx_book(outcomes_file="fx_outcomes.json", today=None,
                    fetcher=fetch_daily_bars):
    """
    Rebuild the FX sim book from scratch by replaying every logged
    call (fx_outcomes.json) chronologically under sim rules: settle
    opens as of each call's date before opening it, so sizing and the
    one-position rule evolve the way they would have live.
    """
    today = today or _today()
    try:
        with open(outcomes_file) as f:
            calls = json.load(f)
    except Exception:
        calls = []
    calls = [c for c in calls if c.get("direction") in ("LONG", "SHORT")
             and c.get("entry_price") and c.get("target") and c.get("stop")]
    calls.sort(key=lambda c: (c.get("signal_date") or "", c.get("pair") or ""))

    book = new_book("fx")
    book["created"] = datetime.now().isoformat()
    book["genesis"] = ("replayed from fx_outcomes.json under sim rules "
                       f"on {today.isoformat()}")

    cache = {}
    def _cached(symbol, start, end):
        if symbol not in cache:
            cache[symbol] = fetcher(symbol, _parse_date(calls[0]["signal_date"])
                                    if calls else start, today)
        return [b for b in cache[symbol] if start <= b["date"] <= end]

    for c in calls:
        entry_date = _parse_date(c.get("signal_date"))
        if entry_date is None:
            continue
        settle_book("fx", book, entry_date, _cached)
        _open_position("fx", book, {
            "symbol":    c.get("symbol"),
            "label":     c.get("pair"),
            "direction": c.get("direction"),
            "conviction": c.get("conviction_at_signal"),
            "entry":     c.get("entry_price"),
            "target":    c.get("target"),
            "stop":      c.get("stop"),
            "hold_days": _parse_hold_days(c.get("hold_period_stated"),
                                          _HOLD_FALLBACK["fx"]),
            "key_driver": c.get("key_driver"),
            "entry_date": entry_date,
        }, entry_date)
    settle_book("fx", book, today, _cached)
    save_book("fx", book)
    return book["stats"]


if __name__ == "__main__":
    stats = rescore_fx_book()
    print("FX sim book (replayed):", json.dumps(stats, indent=2, default=str))
