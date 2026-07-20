"""
ngx_outcome_tracker.py — NGX Paper Signal Outcome Tracker
==========================================================
Tracks NGX paper signals from Day 1 through validation phases.

Resolves outcomes using the ticker's own price movement over a 14-day
window (changed from 7 to 14 days: oil commodity cycles are longer than
a week — see resolve_ngx_outcomes()), via /v1/companies (NGN Markets,
confirmed available on the Free tier with real price data):
  entry_price  = live price captured at signal-log time (log_ngx_signals)
  exit_price   = live price fetched at resolution time (14+ days later)
  actual_return_pct = (exit_price - entry_price) / entry_price * 100
  WIN  = actual_return_pct > +2.0%
  LOSS = actual_return_pct < -2.0%
  FLAT = actual_return_pct in between
regime_at_resolve is still captured and logged for context, but no
longer decides the outcome (it did previously — that was resolving
against macro direction, not the ticker's own price, because /v1/companies
was believed to lack price data; that premise turned out to be false).

This gives the model a validation loop before Day 31 (RESTRICTED phase).
Without this, there's no way to know if the macro scoring predicts
real NGX movements before real money is committed.

Files:
  ngx_outcomes.json   — all logged/resolved signal entries
  ngx_snapshot.json   — latest macro state (written by ngx_screener)
"""

import json
import os
import urllib.request
from datetime import datetime, date


NGX_OUTCOMES_FILE = "ngx_outcomes.json"
NGN_MARKETS_BASE   = "https://api.ngnmarket.com/v1"

WIN_THRESHOLD_PCT  = 2.0   # actual_return_pct > this -> WIN
LOSS_THRESHOLD_PCT = -2.0  # actual_return_pct < this -> LOSS


def _get_api_key():
    return os.environ.get("NGN_MARKETS_KEY", "").strip()


def _bare_symbol(ticker):
    """Strip the .LG suffix used internally — /v1/companies returns bare symbols."""
    return (ticker or "").replace(".LG", "").upper()


def fetch_companies_prices(verbose=False):
    """
    Fetch current spot prices for all NGX companies from /v1/companies
    (Free tier — confirmed to include real price data, unlike the
    endpoints ngx_price_engine.py tries, which return nothing on this
    plan). Returns {BARE_SYMBOL: price}, or {} on any failure — never
    raises, so callers can treat a missing price the same whether the
    key is absent, the request failed, or the ticker just isn't listed.
    """
    key = _get_api_key()
    if not key:
        return {}

    headers = {
        "Authorization": f"Bearer {key}",
        "Accept":        "application/json",
        "User-Agent":    "InvestOS/4.0",
    }

    all_companies = []
    page = 1
    while True:
        url = f"{NGN_MARKETS_BASE}/companies?page={page}&limit=50"
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode())
        except Exception as e:
            if verbose:
                print(f"   ⚠️  NGX /companies fetch failed on page {page}: {e}")
            break

        page_data = data.get("data", {}).get("data", [])
        if not page_data:
            break
        all_companies.extend(page_data)

        pagination = data.get("data", {}).get("pagination", {}) or {}
        total = pagination.get("total")
        limit = pagination.get("limit", 50)
        if total and len(all_companies) >= total:
            break
        if len(page_data) < limit:
            break
        page += 1
        if page > 10:  # safety cap
            break

    prices = {}
    for c in all_companies:
        sym   = (c.get("symbol") or "").upper()
        price = c.get("price")
        if sym and price is not None:
            prices[sym] = float(price)

    if verbose:
        print(f"   📊 NGX /companies: {len(prices)} prices fetched "
              f"({len(all_companies)} companies total)")
    return prices


# ── I/O helpers ──────────────────────────────────────────────────────────────

def load_ngx_outcomes():
    if os.path.exists(NGX_OUTCOMES_FILE):
        try:
            return json.load(open(NGX_OUTCOMES_FILE))
        except Exception:
            pass
    return []


def save_ngx_outcomes(outcomes):
    try:
        json.dump(outcomes, open(NGX_OUTCOMES_FILE, "w"), indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️  NGX outcome save failed: {e}")


# ── Log today's NGX signals ───────────────────────────────────────────────────

def log_ngx_signals(ngx_result, run_time=None):
    """
    Log today's NGX signals for future resolution.
    Called after run_ngx_screen() returns.

    Only logs signals (not watch list) during PAPER phase.
    Captures macro context AND entry_price (live spot price from
    /v1/companies) at signal time — entry_price can only ever be
    captured live, right now; there is no historical/point-in-time
    lookup, so a signal logged without it (key missing, fetch failed)
    can never be resolved by price later (see resolve_ngx_outcomes).
    """
    if not ngx_result:
        return 0

    signals = ngx_result.get("signals", [])
    if not signals:
        return 0

    outcomes  = load_ngx_outcomes()
    today_str = datetime.now().strftime("%Y-%m-%d")
    now       = run_time or datetime.now().isoformat()

    # Don't double-log same signals on same date
    logged_today = {
        o["ticker"] for o in outcomes
        if o.get("signal_date") == today_str and o.get("resolved") is False
    }

    # Capture macro context at signal time
    macro_at_signal = {
        "regime":       ngx_result.get("macro_regime", "NEUTRAL"),
        "macro_score":  ngx_result.get("macro_score", 0),
        "fx_stress":    ngx_result.get("fx_stress", 0),
        "brent_trend":  ngx_result.get("brent_trend", "FLAT"),
        "basket":       ngx_result.get("basket_regime", "UNKNOWN"),
    }

    prices = fetch_companies_prices(verbose=True)

    new_logged = 0
    for sig in signals:
        ticker = sig.get("ticker")
        if not ticker or ticker in logged_today:
            continue

        entry = {
            "ticker":           ticker,
            "name":             sig.get("name", ticker.replace(".LG", "")),
            "sector":           sig.get("sector", "unknown"),
            "tier":             sig.get("tier", 2),
            "signal_date":      today_str,
            "signal_time":      now,
            "score_at_signal":  sig.get("score", 0),
            "persistence":      sig.get("persistence", ""),
            "phase":            ngx_result.get("phase", "PAPER_ONLY"),
            "phase_days":       ngx_result.get("phase_days", 0),
            "macro_at_signal":  macro_at_signal,
            "entry_price":      prices.get(_bare_symbol(ticker)),  # None if unavailable

            # Resolution fields (filled in after 14 days)
            "resolved":         False,
            "resolved_date":    None,
            "score_at_resolve": None,
            "regime_at_resolve":None,
            "exit_price":       None,
            "actual_return_pct":None,
            "outcome":          None,   # WIN | LOSS | FLAT
            "outcome_reason":   None,
        }
        outcomes.append(entry)
        new_logged += 1

    save_ngx_outcomes(outcomes)
    if new_logged:
        print(f"   📝 NGX outcomes: logged {new_logged} signals ({len(outcomes)} total)")
    return new_logged


# ── Resolve matured signals ───────────────────────────────────────────────────

def resolve_ngx_outcomes(ngx_result):
    """
    Resolve signals that are ≥14 days old, against the TICKER'S OWN PRICE
    MOVEMENT (entry_price captured at signal time vs. exit_price fetched
    now), not macro regime. Changed from 7 to 14 days: oil commodity
    cycles are longer than a week.

    regime_at_resolve is still captured and logged for context, but no
    longer decides WIN/LOSS/FLAT — see module docstring for why.

    Signals logged before entry_price capture existed (or where a price
    fetch failed at signal time or resolution time) cannot be resolved by
    price and are left unresolved / retried — never silently defaulted
    to LOSS.
    """
    if not ngx_result:
        return 0

    outcomes  = load_ngx_outcomes()
    today     = date.today()
    resolved  = 0

    # Build current score map from all_scored — still used to detect a
    # ticker dropped from the NGX scored universe (existing fallback,
    # unchanged), even though score no longer decides WIN/LOSS/FLAT.
    current_scores = {
        s["ticker"]: s["score"]
        for s in ngx_result.get("all_scored", [])
    }
    current_regime = ngx_result.get("macro_regime", "NEUTRAL")
    exit_prices    = fetch_companies_prices(verbose=True)

    # Debug: show what we're working with (first unresolved outcome)
    unresolved = [o for o in outcomes if not o.get("resolved")]
    if unresolved:
        sample = unresolved[0]
        try:
            sd = datetime.strptime(str(sample.get("signal_date", "")), "%Y-%m-%d").date()
            dp = (today - sd).days
            print(f"   [NGX resolve] today={today} | first_signal={sample.get('signal_date')} "
                  f"| days={dp} | ticker={sample.get('ticker')} "
                  f"| resolved_field={repr(sample.get('resolved'))}")
        except Exception as _de:
            print(f"   [NGX resolve] debug error: {_de} | sample={sample}")

    for o in outcomes:
        # Explicit bool check — handles both Python False and JSON false
        if o.get("resolved") is True:
            continue

        try:
            signal_date = datetime.strptime(
                str(o.get("signal_date", "")), "%Y-%m-%d"
            ).date()
        except ValueError:
            # Bad date format — skip but log
            print(f"   [NGX resolve] bad signal_date: {o.get('signal_date')}")
            continue

        days_passed = (today - signal_date).days

        if days_passed < 14:
            continue  # not yet — need 14 days (oil cycles > 1 week)

        ticker        = o.get("ticker", "")
        score_exit    = current_scores.get(ticker)
        regime_exit   = current_regime

        # score_exit is None → ticker dropped from the NGX scored universe.
        # This means we have NO signal to evaluate (not the same as a LOSS).
        # Mark resolved=False and skip — it will retry on the next run.
        # (Existing fallback, unchanged.)
        if score_exit is None:
            o["outcome_reason"] = "Ticker no longer scoring — left as UNRESOLVED"
            continue

        entry_price = o.get("entry_price")
        exit_price  = exit_prices.get(_bare_symbol(ticker))

        # Missing price data (either side) -> graceful fallback: leave
        # unresolved, retry next run. NEVER silently default to LOSS.
        if entry_price is None:
            o["outcome_reason"] = (
                "No entry_price captured at signal time (predates price "
                "capture, or fetch failed then) — cannot resolve by price, "
                "left UNRESOLVED"
            )
            continue
        if exit_price is None:
            o["outcome_reason"] = (
                "Price fetch failed at resolution time — left UNRESOLVED, "
                "will retry next run"
            )
            continue

        actual_return_pct = (exit_price - entry_price) / entry_price * 100

        o["resolved"]          = True
        o["resolved_date"]     = today.isoformat()
        o["score_at_resolve"]  = score_exit
        o["regime_at_resolve"] = regime_exit   # logged for context only
        o["exit_price"]        = round(exit_price, 4)
        o["actual_return_pct"] = round(actual_return_pct, 2)

        # Determine outcome — ticker's own price movement, not regime
        if actual_return_pct > WIN_THRESHOLD_PCT:
            o["outcome"] = "WIN"
        elif actual_return_pct < LOSS_THRESHOLD_PCT:
            o["outcome"] = "LOSS"
        else:
            o["outcome"] = "FLAT"
        o["outcome_reason"] = (
            f"Price {entry_price}→{exit_price} ({actual_return_pct:+.2f}%) | "
            f"regime at resolve: {regime_exit} (context only, not decisive)"
        )

        resolved += 1

    save_ngx_outcomes(outcomes)
    if resolved:
        print(f"   ✅ NGX outcomes: resolved {resolved} signals")
    return resolved


# ── Summary report ────────────────────────────────────────────────────────────

def ngx_outcome_summary():
    """
    Print NGX paper signal performance summary.
    Returns dict with key metrics.
    """
    outcomes  = load_ngx_outcomes()
    if not outcomes:
        return {}

    resolved  = [o for o in outcomes if o.get("resolved")]
    pending   = [o for o in outcomes if not o.get("resolved")]

    wins    = sum(1 for o in resolved if o.get("outcome") == "WIN")
    losses  = sum(1 for o in resolved if o.get("outcome") == "LOSS")
    flats   = sum(1 for o in resolved if o.get("outcome") == "FLAT")
    total_r = len(resolved)

    win_rate = round(wins / total_r * 100, 1) if total_r > 0 else 0

    # By sector
    sector_wins = {}
    sector_total = {}
    for o in resolved:
        sec = o.get("sector", "unknown")
        sector_total[sec] = sector_total.get(sec, 0) + 1
        if o.get("outcome") == "WIN":
            sector_wins[sec] = sector_wins.get(sec, 0) + 1

    # By tier
    t1_resolved = [o for o in resolved if o.get("tier") == 1]
    t1_wins     = sum(1 for o in t1_resolved if o.get("outcome") == "WIN")
    t1_wr       = round(t1_wins / len(t1_resolved) * 100, 1) if t1_resolved else 0

    summary = {
        "total_logged":  len(outcomes),
        "total_resolved":total_r,
        "pending":       len(pending),
        "wins":          wins,
        "losses":        losses,
        "flats":         flats,
        "win_rate":      win_rate,
        "t1_win_rate":   t1_wr,
        "sector_wins":   sector_wins,
        "sector_total":  sector_total,
    }

    return summary


def print_ngx_outcome_report():
    """Print formatted NGX outcome report."""
    s = ngx_outcome_summary()
    if not s:
        print("   No NGX outcomes logged yet")
        return

    print(f"\n{'='*50}")
    print(f"  NGX PAPER SIGNAL OUTCOMES")
    print(f"{'='*50}")
    print(f"  Total logged:    {s['total_logged']}")
    print(f"  Resolved:        {s['total_resolved']}")
    print(f"  Pending (7d):    {s['pending']}")

    if s['total_resolved'] > 0:
        print(f"\n  WIN RATE:        {s['win_rate']}%  "
              f"({s['wins']}W / {s['losses']}L / {s['flats']}F)")
        print(f"  Tier 1 WR:       {s['t1_win_rate']}%")

        if s['sector_total']:
            print(f"\n  BY SECTOR:")
            for sec, total in sorted(s['sector_total'].items(),
                                     key=lambda x: -x[1]):
                wins  = s['sector_wins'].get(sec, 0)
                wr    = round(wins / total * 100) if total else 0
                bar   = "█" * (wr // 10)
                print(f"    {sec:<12} {bar:<10} {wr}%  ({wins}/{total})")
    else:
        print(f"\n  No resolved signals yet — check back after Day 7")
    print(f"{'='*50}")
    return s
