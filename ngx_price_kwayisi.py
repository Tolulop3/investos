"""
NGX Price Engine — free-feed adapter (AFX / kwayisi.org)
=========================================================
Second price source for ngx_price_engine, built as a drop-in replacement
for the paid NGN Markets feed (whose per-stock endpoints need an upgraded
plan — the reason the NGX module went silent on 2026-09-24).

Data sources (free, no key):
  Listings/EOD : https://afx.kwayisi.org/ngx/  (+ ?page=2, …)
                 Full NGX table (ticker, volume, price, change), ASI value,
                 gainers/losers counts, official trading summary
                 (date, total volume, deals).
  History      : https://afx.kwayisi.org/chart/ngx/<ticker>
                 Highstock JS with daily closes back ~10 years
                 ([d("YYYY-MM-DD"),close], …) → MA50 / MA200 computed here.

Returns exactly the dict shapes ngx_price_engine produces and writes the
same ngx_price_cache.json format (via ngx_price_engine._save_to_cache),
so ngx_screener.py needs no changes.

Activation (wired in ngx_price_engine, see integration notes):
  NGX_PRICE_SOURCE=kwayisi   → gate_level() >= 1 and fetch_all_ngx_prices()
                               delegates to fetch_all_ngx_prices_kwayisi().

Caveats: third-party scraper site, no SLA. Any fetch/parse failure returns
{} / partial results and the screener falls back to macro-only scoring,
exactly as it does today when the paid feed fails.
"""

import os
import re
import json
import time
import math
import urllib.request
from datetime import datetime
from html.parser import HTMLParser

KWAYISI_BASE = "https://afx.kwayisi.org"
LISTINGS_URL = KWAYISI_BASE + "/ngx/"
CHART_URL    = KWAYISI_BASE + "/chart/ngx/{sym}"
FETCH_UA     = "InvestOS/4.0 (ngx_price_kwayisi)"

# Reuse the engine's cache writer so the on-disk format can never drift.
try:
    from ngx_price_engine import _save_to_cache, PRICE_CACHE_FILE
except Exception:  # standalone use (tests) — identical local copy
    PRICE_CACHE_FILE = "ngx_price_cache.json"

    def _save_to_cache(ticker, data):
        try:
            cache = {}
            try:
                cache = json.load(open(PRICE_CACHE_FILE))
            except Exception:
                pass
            if "prices" not in cache:
                cache["prices"] = {}
            cache["prices"][ticker] = {
                "data":      data,
                "cached_at": datetime.now().isoformat(),
            }
            json.dump(cache, open(PRICE_CACHE_FILE, "w"), indent=2)
        except Exception:
            pass


# ── Fetching ─────────────────────────────────────────────────────────────────

def _fetch_text(url, timeout=20):
    """GET a URL as text. Isolated so tests can monkeypatch the transport."""
    req = urllib.request.Request(url, headers={
        "User-Agent": FETCH_UA,
        "Accept":     "text/html,application/javascript,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return raw.decode("utf-8", errors="replace")


# ── HTML table extraction ────────────────────────────────────────────────────
# kwayisi serves minified HTML with omitted </td>/</tr> end tags, so the
# parser closes a cell on the next cell/row start and a row on the next
# row/table end.

class _TableExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables = []          # [ [ [cell, ...], ... ], ... ]
        self._table = None
        self._row = None
        self._cell = None

    def _close_cell(self):
        if self._cell is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell).split()))
        self._cell = None

    def _close_row(self):
        self._close_cell()
        if self._row is not None and self._table is not None:
            if any(c for c in self._row):
                self._table.append(self._row)
        self._row = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._close_row()
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._close_cell()
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th"):
            self._close_cell()
        elif tag == "tr":
            self._close_row()
        elif tag == "table" and self._table is not None:
            self._close_row()
            self.tables.append(self._table)
            self._table = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def _extract_tables(html):
    p = _TableExtractor()
    p.feed(html)
    return p.tables


def _num(text):
    """'1,234.50' / '+0.00' / '-0.25' / '' → float or None."""
    if text is None:
        return None
    t = str(text).replace(",", "").replace("−", "-").strip()
    if t in ("", "-", "+"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


# ── Listings page ────────────────────────────────────────────────────────────

def parse_listings_page(html):
    """
    Parse one /ngx/ listings page.
    Returns (rows, meta):
      rows: {SYM: {"close": float, "change": float|None, "volume": int|None}}
      meta: {"asi": float, "asi_change": float, "gainers": int|None,
             "losers": int|None, "total_listings": int|None,
             "date": "YYYY-MM-DD"|None, "volume": int|None, "deals": int|None}
    Meta fields are only present on page 1 (all default None otherwise).
    """
    rows, meta = {}, {}
    tables = _extract_tables(html)

    for table in tables:
        if not table:
            continue
        header = [c.strip().lower() for c in table[0]]
        # ASI summary table
        if header and header[0].startswith("asi index") and len(table) > 1:
            m = re.search(r"([\d,]+\.\d+)\s*\(([+-]?[\d,]+\.\d+)\)", table[1][0])
            if m:
                meta["asi"] = _num(m.group(1))
                meta["asi_change"] = _num(m.group(2))
            continue
        # Listings table
        if header[:5] == ["ticker", "name", "volume", "price", "change"]:
            for cells in table[1:]:
                if len(cells) < 5:
                    continue
                sym = cells[0].strip().upper()
                close = _num(cells[3])
                if not sym or close is None:
                    continue
                vol = _num(cells[2])
                rows[sym] = {
                    "close":  close,
                    "change": _num(cells[4]),
                    "volume": int(vol) if vol is not None else None,
                }

    text = re.sub(r"<[^>]+>", " ", html)
    text = " ".join(text.split())

    m = re.search(r"Top Gainers\s*\(\s*(\d+)\s*\)", text)
    if m:
        meta["gainers"] = int(m.group(1))
    m = re.search(r"Bottom Losers\s*\(\s*(\d+)\s*\)", text)
    if m:
        meta["losers"] = int(m.group(1))
    m = re.search(r"Showing \d+ - \d+ of (\d+) listings", text)
    if m:
        meta["total_listings"] = int(m.group(1))
    m = re.search(
        r"NGX TRADING SUMMARY FOR [A-Z]+, ([A-Z]+ \d{1,2}, \d{4}):"
        r".*?total of ([\d,]+) shares in ([\d,]+) deals", text)
    if m:
        try:
            meta["date"] = datetime.strptime(
                m.group(1).title(), "%B %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
        meta["volume"] = int(m.group(2).replace(",", ""))
        meta["deals"] = int(m.group(3).replace(",", ""))

    return rows, meta


def fetch_market_tables(verbose=False):
    """
    Fetch all listings pages. Returns (rows, meta) merged across pages.
    Returns ({}, {}) if page 1 cannot be fetched/parsed.
    """
    try:
        html = _fetch_text(LISTINGS_URL)
    except Exception as e:
        if verbose:
            print(f"  ⚠️  kwayisi listings fetch failed: {e}")
        return {}, {}
    rows, meta = parse_listings_page(html)

    total = meta.get("total_listings") or len(rows)
    pages = max(1, math.ceil(total / 100))
    for page in range(2, pages + 1):
        try:
            more, _ = parse_listings_page(
                _fetch_text(f"{LISTINGS_URL}?page={page}"))
            rows.update(more)
        except Exception as e:
            if verbose:
                print(f"  ⚠️  kwayisi listings page {page} failed: {e}")
        time.sleep(0.10)

    if verbose:
        print(f"  📊 kwayisi: {len(rows)} listings parsed "
              f"(date {meta.get('date')})")
    return rows, meta


def market_snapshot_from(rows, meta):
    """Build the __market__ dict in ngx_price_engine's snapshot shape."""
    if not rows or meta.get("asi") is None:
        return {}
    asi = meta["asi"]
    asi_chg_abs = meta.get("asi_change")
    prev_asi = asi - asi_chg_abs if asi_chg_abs is not None else None
    asi_change_pct = (asi_chg_abs / prev_asi * 100) if prev_asi else 0.0

    adv = meta.get("gainers")
    dec = meta.get("losers")
    if adv is None or dec is None:  # fall back to counting table signs
        adv = sum(1 for r in rows.values() if (r["change"] or 0) > 0)
        dec = sum(1 for r in rows.values() if (r["change"] or 0) < 0)
    total = meta.get("total_listings") or len(rows)
    if dec:
        ratio = round(adv / dec, 2)
    else:
        ratio = float(adv) if adv else 0.0

    return {
        "asi":             round(asi, 2),
        "asi_change_pct":  round(asi_change_pct, 4),
        "adv_dec_ratio":   ratio,
        "advancers":       adv,
        "decliners":       dec,
        "unchanged":       max(0, total - adv - dec),
        "volume":          meta.get("volume"),
        "deals":           meta.get("deals"),
        "date":            meta.get("date", ""),
        "calls_remaining": None,
        "source":          "kwayisi",
    }


# ── History (MA50 / MA200) ───────────────────────────────────────────────────

_CHART_POINT_RE = re.compile(r'd\("(\d{4}-\d{2}-\d{2})"\),\s*([\d.]+)')


def parse_chart_history(js_text):
    """Chart JS → [(date, close), …] ascending by date."""
    pts = [(d, float(v)) for d, v in _CHART_POINT_RE.findall(js_text or "")]
    pts.sort(key=lambda p: p[0])
    return pts


def fetch_history(sym):
    """Daily closes for one ticker from its chart endpoint ([] on failure)."""
    try:
        return parse_chart_history(_fetch_text(CHART_URL.format(sym=sym.lower())))
    except Exception:
        return []


def _moving_average(closes, window):
    if len(closes) < window:
        return None
    return round(sum(closes[-window:]) / window, 2)


# ── Engine-compatible API ────────────────────────────────────────────────────

def _build_price_data(row, sym, table_date, history=None):
    close = row["close"]
    change_abs = row["change"] if row["change"] is not None else 0.0
    prev_close = round(close - change_abs, 2)
    change_pct = (change_abs / prev_close * 100) if prev_close else 0.0

    if history is None:
        history = fetch_history(sym)
    closes = [c for _, c in history]
    # Reconcile with today's table close (chart can lag a session).
    if table_date and history and history[-1][0] < table_date:
        closes.append(close)
    elif table_date and history and history[-1][0] == table_date:
        closes[-1] = close

    ma50 = _moving_average(closes, 50)
    ma200 = _moving_average(closes, 200)

    return {
        "close":       round(float(close), 2),
        "prev_close":  prev_close if prev_close else None,
        "change_pct":  round(change_pct, 2),
        "volume":      row["volume"],
        "ma50":        ma50,
        "ma200":       ma200,
        "above_ma50":  (close > ma50) if ma50 else None,
        "above_ma200": (close > ma200) if ma200 else None,
        "source":      "kwayisi",
    }


def fetch_ngx_market_snapshot_kwayisi(verbose=False):
    """Market snapshot dict (same shape as fetch_ngx_market_snapshot)."""
    rows, meta = fetch_market_tables(verbose=verbose)
    return market_snapshot_from(rows, meta)


def fetch_ngx_price_kwayisi(ticker, verbose=False):
    """Single-ticker price dict (same shape as fetch_ngx_price) or None."""
    rows, meta = fetch_market_tables(verbose=verbose)
    sym = ticker.replace(".LG", "").upper()
    row = rows.get(sym)
    if not row:
        return None
    return _build_price_data(row, sym, meta.get("date"))


def fetch_all_ngx_prices_kwayisi(tickers, verbose=False):
    """
    Drop-in replacement for ngx_price_engine.fetch_all_ngx_prices.
    Returns {ticker_as_passed: price_data} and writes ngx_price_cache.json
    (including the __market__ snapshot) in the engine's format.
    """
    rows, meta = fetch_market_tables(verbose=verbose)
    if not rows:
        return {}

    market = market_snapshot_from(rows, meta)
    if market:
        _save_to_cache("__market__", market)
        if verbose:
            print(f"  🟢 NGX ASI: {market['asi_change_pct']:+.2f}% | "
                  f"Breadth: {market['advancers']} adv / "
                  f"{market['decliners']} dec (ratio: {market['adv_dec_ratio']:.2f})")

    results = {}
    for ticker in tickers:
        sym = ticker.replace(".LG", "").upper()
        row = rows.get(sym)
        if not row:
            if verbose:
                print(f"  ⚠️  kwayisi: {sym} not in listings table")
            continue
        data = _build_price_data(row, sym, meta.get("date"))
        _save_to_cache(ticker, data)
        results[ticker] = data
        time.sleep(0.05)

    if verbose:
        print(f"  📊 NGX prices (kwayisi): {len(results)}/{len(tickers)} fetched")
    return results
