"""
NGX Price Engine — free-feed adapter (tradelines.com.ng)
=========================================================
Third price source for ngx_price_engine, built as a drop-in replacement
for the paid NGN Markets feed (whose per-stock endpoints need an upgraded
plan — the reason the NGX module went silent on 2026-09-24).

Why this source: the kwayisi adapter (ngx_price_kwayisi.py) works, but
GitHub Actions runners cannot reach afx.kwayisi.org (run #864 log,
2026-10-26: "urlopen error [Errno 101] Network is unreachable"), and the
Wayback Machine captures that page only ~monthly — too stale for EOD.
tradelines.com.ng is on ordinary hosting and publishes the full official
daily price list as an HTML table.

Data source (free, no key):
  Daily post : "Price List of Traded Equities: <Weekday, D Month, YYYY>"
               discovered via the WordPress REST API
               (wp-json/wp/v2/stock_price, newest first); the table on
               the post page carries Stock / PClose / Open / High / Low /
               Close / Chg / %Chg / Deals / Volume / Value for ~146
               listings — prev_close and change_pct are native columns.
  History    : ngx_history.json, committed in this repo. Seeded once from
               the tradelines post archive (Dec 2025 → Oct 2026), then
               appended one row per trading day by fetch_all below (the
               daily workflow's `git add -A` commit persists it). MA50 /
               MA200 are computed from it. If the file is missing, MAs
               are None and accumulate from that day forward.

Returns exactly the dict shapes ngx_price_engine produces and writes the
same ngx_price_cache.json format (via ngx_price_engine._save_to_cache),
so ngx_screener.py needs no changes. The market snapshot prefers the NGN
snapshot endpoint (real ASI; works on the free plan) when its date matches
the price table's trading date, and falls back to table-derived breadth
(ASI fields None — the screener tolerates that) otherwise.

Activation (wired in ngx_price_engine):
  NGX_PRICE_SOURCE=tradelines → gate_level() >= 1 and the fetch_*
                               functions delegate here.

Caveats: third-party blog, no SLA. Any fetch/parse failure returns
{} / partial results and the screener falls back to macro-only scoring,
exactly as it does when the paid feed fails.
"""

import os
import re
import json
import time
import urllib.request
from datetime import datetime
from html.parser import HTMLParser

WP_API = "https://tradelines.com.ng/wp-json/wp/v2/stock_price"
FETCH_UA = "InvestOS/4.0 (ngx_price_tradelines)"
HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "ngx_history.json")
HISTORY_KEEP = 260   # trading days retained per symbol (MA200 + buffer)

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

def _fetch_text(url, timeout=25):
    """GET a URL as text. Isolated so tests can monkeypatch the transport."""
    req = urllib.request.Request(url, headers={
        "User-Agent": FETCH_UA,
        "Accept":     "text/html,application/json,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return raw.decode("utf-8", errors="replace")


# ── Table parsing ────────────────────────────────────────────────────────────

class _TableExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.tables = []
        self.cur = None
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
            self.cur = []
        elif self.in_table and tag == "tr":
            self.row = []
        elif self.in_table and tag in ("td", "th"):
            self.cell = ""

    def handle_endtag(self, tag):
        if tag == "table" and self.in_table:
            self.tables.append(self.cur)
            self.in_table = False
        elif self.in_table and tag == "tr" and self.row is not None:
            self.cur.append(self.row)
            self.row = None
        elif self.in_table and tag in ("td", "th") and self.cell is not None:
            self.row.append(re.sub(r"\s+", " ", self.cell).strip())
            self.cell = None

    def handle_data(self, data):
        if self.cell is not None:
            self.cell += data


def _extract_tables(html):
    p = _TableExtractor()
    p.feed(html or "")
    return p.tables


def _num(text):
    """'1,234.50' / '-0.25' / '--' / '' → float or None."""
    if text is None:
        return None
    t = str(text).replace(",", "").replace("−", "-").strip()
    if t in ("", "-", "+", "--"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _sym(text):
    """'AFRINSURE [MRF]' → 'AFRINSURE'; plain symbols pass through."""
    return re.sub(r"\s*\[.*?\]\s*", "", text or "").strip().upper()


_MONTHS = {m: i + 1 for i, m in enumerate([
    "January", "February", "March", "April", "May", "June", "July",
    "August", "September", "October", "November", "December"])}
_TITLE_DATE_RE = re.compile(
    r"(\d{1,2})(?:st|nd|rd|th)\s+(" + "|".join(_MONTHS) + r"),?\s+(\d{4})")


def parse_title_date(title):
    """'Price List of Traded Equities: Monday, 26th October, 2026' → ISO."""
    m = _TITLE_DATE_RE.search(title or "")
    if not m:
        return None
    return "%04d-%02d-%02d" % (int(m.group(3)), _MONTHS[m.group(2)],
                               int(m.group(1)))


def parse_price_table(html):
    """
    Post page HTML → {SYM: row} for the daily price table, or {}.
    Row: {close, prev_close, change, change_pct, open, high, low,
          volume, deals} — floats/ints or None where the table has '--'.
    The table is located by its header (Stock … PClose … Close), and
    columns are mapped by header name so reordering cannot corrupt data.
    """
    for table in _extract_tables(html):
        if not table or len(table) < 10:
            continue
        header = [c.strip().lower() for c in table[0]]
        if "stock" not in header or "pclose" not in header \
                or "close" not in header:
            continue
        col = {name: i for i, name in enumerate(header)}

        def get(row, name):
            i = col.get(name)
            return row[i] if i is not None and i < len(row) else None

        rows = {}
        for row in table[1:]:
            if len(row) < 6:
                continue
            sym = _sym(get(row, "stock"))
            close = _num(get(row, "close"))
            if not sym or close is None:
                continue
            vol = _num(get(row, "volume"))
            deals = _num(get(row, "deals"))
            rows[sym] = {
                "close":      close,
                "prev_close": _num(get(row, "pclose")),
                "change":     _num(get(row, "chg")),
                "change_pct": _num(get(row, "%chg")),
                "open":       _num(get(row, "open")),
                "high":       _num(get(row, "high")),
                "low":        _num(get(row, "low")),
                "volume":     int(vol) if vol is not None else None,
                "deals":      int(deals) if deals is not None else None,
            }
        if len(rows) >= 50:   # a real price list, not a stray table
            return rows
    return {}


# ── Latest post discovery ────────────────────────────────────────────────────

def latest_post(verbose=False):
    """Newest price-list post → (page_url, trading_date_iso) or (None, None)."""
    try:
        raw = _fetch_text(WP_API + "?per_page=1&orderby=date&order=desc"
                                   "&_fields=link,title,date")
        posts = json.loads(raw)
        if not posts:
            return None, None
        post = posts[0]
        url = post.get("link")
        date = parse_title_date((post.get("title") or {}).get("rendered", ""))
        if not date:   # title format drifted — publication date is close
            date = (post.get("date") or "")[:10] or None
        return url, date
    except Exception as e:
        if verbose:
            print(f"  ⚠️  tradelines post discovery failed: {e}")
        return None, None


def fetch_price_table(verbose=False):
    """Latest daily table → (rows, {"date": iso}) or ({}, {})."""
    url, date = latest_post(verbose=verbose)
    if not url:
        return {}, {}
    try:
        rows = parse_price_table(_fetch_text(url))
    except Exception as e:
        if verbose:
            print(f"  ⚠️  tradelines table fetch failed: {e}")
        return {}, {}
    if not rows and verbose:
        print("  ⚠️  tradelines: no price table found in latest post")
    return rows, {"date": date}


# ── History (MA50 / MA200) ───────────────────────────────────────────────────

def _load_history():
    """ngx_history.json → {SYM: [(date, close), …] ascending}; {} on error."""
    try:
        raw = json.load(open(HISTORY_FILE))
        return {s: [(d, float(c)) for d, c in series]
                for s, series in raw.items()}
    except Exception:
        return {}


def _save_history(hist):
    try:
        out = {s: [[d, c] for d, c in series] for s, series in hist.items()}
        json.dump(out, open(HISTORY_FILE, "w"), indent=1)
    except Exception:
        pass


def _append_history(hist, rows, date):
    """Add (or refresh) one trading day for every table symbol."""
    if not date:
        return False
    changed = False
    for sym, row in rows.items():
        series = hist.setdefault(sym, [])
        if series and series[-1][0] == date:
            if series[-1][1] != row["close"]:
                series[-1] = (date, row["close"])
                changed = True
        elif not series or series[-1][0] < date:
            series.append((date, row["close"]))
            del series[:-HISTORY_KEEP]
            changed = True
    return changed


def _moving_average(closes, window):
    if len(closes) < window:
        return None
    return round(sum(closes[-window:]) / window, 2)


# ── Engine-compatible API ────────────────────────────────────────────────────

def _build_price_data(row, table_date, history_series=None):
    close = row["close"]
    prev_close = row["prev_close"]
    if prev_close is None and row["change"] is not None:
        prev_close = round(close - row["change"], 2)
    change_pct = row["change_pct"]
    if change_pct is None and prev_close:
        change_pct = round((close - prev_close) / prev_close * 100, 2)

    closes = [c for _, c in (history_series or [])]
    # Reconcile with today's table close (history can lag a session).
    if table_date and history_series:
        if history_series[-1][0] < table_date:
            closes.append(close)
        elif history_series[-1][0] == table_date:
            closes[-1] = close

    ma50 = _moving_average(closes, 50)
    ma200 = _moving_average(closes, 200)

    return {
        "close":       round(float(close), 2),
        "prev_close":  round(prev_close, 2) if prev_close else None,
        "change_pct":  round(change_pct, 2) if change_pct is not None else 0.0,
        "volume":      row["volume"],
        "ma50":        ma50,
        "ma200":       ma200,
        "above_ma50":  (close > ma50) if ma50 else None,
        "above_ma200": (close > ma200) if ma200 else None,
        "source":      "tradelines",
    }


def snapshot_from_table(rows, date):
    """__market__ dict derived from the price table (ASI unavailable)."""
    adv = sum(1 for r in rows.values() if (r["change"] or 0) > 0)
    dec = sum(1 for r in rows.values() if (r["change"] or 0) < 0)
    total = len(rows)
    if dec:
        ratio = round(adv / dec, 2)
    else:
        ratio = float(adv) if adv else 0.0
    vols = [r["volume"] for r in rows.values() if r["volume"] is not None]
    deals = [r["deals"] for r in rows.values() if r["deals"] is not None]
    return {
        "asi":             None,
        "asi_change_pct":  None,
        "adv_dec_ratio":   ratio,
        "advancers":       adv,
        "decliners":       dec,
        "unchanged":       max(0, total - adv - dec),
        "volume":          sum(vols) if vols else None,
        "deals":           sum(deals) if deals else None,
        "date":            date or "",
        "calls_remaining": None,
        "source":          "tradelines",
    }


def _snapshot(rows, meta, verbose=False):
    """
    Market snapshot for the table's trading date: the NGN snapshot (real
    ASI, free-plan endpoint) when it is for the same date, else the
    table-derived fallback.
    """
    date = meta.get("date")
    try:
        from ngx_price_engine import _fetch_ngn_market_snapshot
        ngn = _fetch_ngn_market_snapshot(verbose=verbose)
        if ngn and ngn.get("asi") is not None and ngn.get("date") == date:
            return ngn
        if verbose and ngn:
            print(f"  ℹ️  tradelines: NGN snapshot date {ngn.get('date')} "
                  f"≠ table date {date} — using table-derived breadth")
    except Exception:
        pass
    return snapshot_from_table(rows, date)


def fetch_ngx_market_snapshot_tradelines(verbose=False):
    """Market snapshot dict (same shape as fetch_ngx_market_snapshot)."""
    rows, meta = fetch_price_table(verbose=verbose)
    if not rows:
        return {}
    return _snapshot(rows, meta, verbose=verbose)


def fetch_ngx_price_tradelines(ticker, verbose=False):
    """Single-ticker price dict (same shape as fetch_ngx_price) or None."""
    rows, meta = fetch_price_table(verbose=verbose)
    sym = ticker.replace(".LG", "").upper()
    row = rows.get(sym)
    if not row:
        return None
    hist = _load_history()
    return _build_price_data(row, meta.get("date"), hist.get(sym, []))


def fetch_all_ngx_prices_tradelines(tickers, verbose=False):
    """
    Drop-in replacement for ngx_price_engine.fetch_all_ngx_prices.
    Returns {ticker_as_passed: price_data} and writes ngx_price_cache.json
    (including the __market__ snapshot) in the engine's format. Also
    appends the day to ngx_history.json (persisted by the workflow's
    `git add -A` commit).
    """
    rows, meta = fetch_price_table(verbose=verbose)
    if not rows:
        return {}
    date = meta.get("date")

    market = _snapshot(rows, meta, verbose=verbose)
    if market:
        _save_to_cache("__market__", market)
        if verbose:
            print(f"  🟢 NGX snapshot ({market.get('source')}): "
                  f"{market['advancers']} adv / {market['decliners']} dec "
                  f"(ratio: {market['adv_dec_ratio']:.2f})")

    hist = _load_history()
    if _append_history(hist, rows, date):
        _save_history(hist)

    results = {}
    for ticker in tickers:
        sym = ticker.replace(".LG", "").upper()
        row = rows.get(sym)
        if not row:
            if verbose:
                print(f"  ⚠️  tradelines: {sym} not in price table")
            continue
        data = _build_price_data(row, date, hist.get(sym, []))
        _save_to_cache(ticker, data)
        results[ticker] = data
        time.sleep(0.05)

    if verbose:
        print(f"  📊 NGX prices (tradelines): {len(results)}/{len(tickers)} fetched")
    return results
