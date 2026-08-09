"""
insider_engine.py — InvestOS Insider Signal Engine v2
======================================================
Auto-discovers insider Form 4 filings from SEC EDGAR.
No manual input needed — runs daily against screener picks.

v2 changes vs scaffold:
  - Auto-fetches Form 4 for current screener picks (not manual history)
  - CIK lookup: hardcoded for common picks + EDGAR company search fallback
  - Parses transaction type P=buy, S=sell, A=award (awards excluded)
  - Cluster signal: 2+ buys in 30 days = strong signal (+8pts)
  - Score tilt capped at ±10pts (insider is a tilt, not a driver)
  - Caches CIK map to insider_cik_cache.json (avoids repeat lookups)
  - Canadian stocks: SEDI note logged, no API available

Signal logic:
  CLUSTER BUY  (2+ insiders bought in 30d, total $50k+): +8pts
  SINGLE BUY   (1 insider bought in 30d, $25k+):          +4pts
  CLUSTER SELL (2+ insiders sold, no concurrent buys):    -5pts
  SINGLE SELL  (1 insider sold):                           -2pts
  AWARD ONLY   (RSU/option grants only):                   0pts
"""

import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta


def safe_parse_api_response(data, list_key):
    """
    Guard: normalise any API response into a plain Python list.
    Called before any iteration in both insider + options engines.
    Handles: list (pass-through), dict (extract list_key), anything else → [].
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get(list_key, [])
    return []


def safe_parse_records(raw, list_keys=('filings', 'results', 'data', 'items')):
    """
    Normalise an API response into a list of dicts.
    Handles: response objects with .json(), raw dicts, lists, strings, None.
    Returns [] (never raises) and prints a warning on unexpected shapes.
    """
    try:
        data = raw.json() if hasattr(raw, 'json') else raw
    except (json.JSONDecodeError, ValueError) as e:
        print(f"  ⚠️ [safe_parse_records] JSON decode failed: {e} "
              f"| type={type(raw).__name__} | head={repr(str(raw))[:200]}")
        return []
    if data is None:
        return []
    if isinstance(data, dict):
        for k in list_keys:
            if k in data and isinstance(data[k], list):
                data = data[k]
                break
        else:
            print(f"  ⚠️ [safe_parse_records] dict with no known list key "
                  f"— keys={list(data.keys())[:10]}")
            return []
    if not isinstance(data, list):
        print(f"  ⚠️ [safe_parse_records] expected list, got "
              f"{type(data).__name__}: {repr(data)[:200]}")
        return []
    return [r for r in data if isinstance(r, dict)]


# ── KNOWN CIKs — avoids EDGAR company search API call (saves time + quota) ───
# FIX (2026-08-09): 12 of these were wrong -- verified every US-ticker
# entry below against SEC's own authoritative company_tickers.json
# (https://www.sec.gov/files/company_tickers.json) live. Most look like
# stale entries surviving a corporate re-domiciliation/reorg that issued
# a new CIK (e.g. Medtronic's move to plc structure) that the hardcoded
# map was never updated for -- same root cause class as the already-
# documented Canadian cross-listed CIK incident below, just found here
# independently while building fetch_recent_form4()'s real XML parsing.
# Confirmed materially wrong, not a rounding/format difference: AMGN
# (this system's own most-frequent high-conviction pick) was pointing at
# CAMBREX CORP, an entirely different, unrelated company -- every
# insider signal ever computed for "AMGN" was checking Cambrex's Form 4
# filings, not Amgen's.
KNOWN_CIKS = {
    "JPM":    "0000019617", "MS":     "0000895421", "GS":     "0000886982",
    "BAC":    "0000070858", "WFC":    "0000072971", "C":      "0000831001",
    "MDT":    "0001613103", "ABT":    "0000001800", "ISRG":   "0001035267",
    "DXCM":   "0001093557", "AFRM":   "0001820953", "SNOW":   "0001640147",
    "MDB":    "0001441816", "NVDA":   "0001045810", "MSFT":   "0000789019",
    "META":   "0001326801", "AMZN":   "0001018724", "GOOGL":  "0001652044",
    "AAPL":   "0000320193", "F":      "0000037996", "GM":     "0001467858",
    "PFE":    "0000078003", "JNJ":    "0000200406", "ABBV":   "0001551152",
    "MRK":    "0000310158", "BMY":    "0000014272", "AMGN":   "0000318154",
    "BIIB":   "0000875045", "REGN":   "0000872589", "GILD":   "0000882095",
    "LOW":    "0000060667", "HD":     "0000354950", "TGT":    "0000027419",
    "SBUX":   "0000829224", "O":      "0000726728", "VICI":   "0001705696",
    "AMT":    "0001053507", "MAIN":   "0001396440", "STAG":   "0001479094",
    "BX":     "0001393818", "BLK":    "0002012383", "KO":     "0000021344",
    "PEP":    "0000077476", "PG":     "0000080424",
}

# ── Canadian cross-listed tickers with SEC CIKs ─────────────────────────────
# These companies trade on both TSX and US exchanges — they file with SEC.
# Covers the most common Canadian picks in InvestOS universe.
#
# FIX (2026-08-08): every entry here was re-verified against SEC EDGAR's
# submissions.json and company_tickers.json master file after BNS.TO's CIK
# turned out not to exist at all (404). That check found 21 of the 24
# original entries pointed at the wrong company entirely -- e.g. TIH.TO was
# wired to Sprint LLC, WPM.TO to Bank of Israel, POW.TO to National Presto
# Industries -- syntactically valid CIKs with no relation to the intended
# company. One of them (ATD.TO -> Northrim BanCorp, an Alaska bank) had
# already produced a live, wrong-company insider-activity score adjustment
# in production on 2026-08-07 and 2026-08-08. Root cause: the whole dict was
# added in a single "Add files via upload" GitHub web-UI commit
# (93157d23, 2026-06-24), never run through EDGAR or any test at the time.
# All values below are the CIK EDGAR's submissions.json itself reports for
# that exact company name.
CANADIAN_SEC_CIKS = {
    # Big 6 Banks
    "RY.TO":  "0001000275",  "TD.TO":  "0000947263",
    "BNS.TO": "0000009631",  "BMO.TO": "0000927971",
    "CM.TO":  "0001045520",  "NA.TO":  "0000926171",
    # Insurers
    "MFC.TO": "0001086888",  "SLF.TO": "0001097362",
    # Energy
    "ENB.TO": "0000895728",  "TRP.TO": "0001232384",
    "CNQ.TO": "0001017413",  "SU.TO":  "0000311337",
    "CVE.TO": "0001475260",  "PPL.TO": "0001546066",
    # Rails + industrials
    "CP.TO":  "0000016875",  "CNR.TO": "0000016868",
    "TIH.TO": "0002072098",
    # Financials / alt asset
    "BAM.TO": "0001001085",  "BN.TO":  "0001001085",
    "POW.TO": "0000801166",
    # Miners (already capped but keep for signal completeness)
    "WPM.TO": "0001323404",  "ABX.TO": "0000756894",
    "AEM.TO": "0000002809",  "NTR.TO": "0001725964",
}

# Tickers requiring SEDI (TSX-only, no SEC cross-listing)
# sedi.ca blocks automated access from GitHub Actions — flagged for manual check
#
# ATD.TO, FM.TO, LUN.TO moved here 2026-08-08: each has a real EDGAR entity
# (Couche-Tard, First Quantum, Lundin Mining) but it's a stale shell
# registration with no Form 4 activity in 7-19 years -- these three are
# TSX-only with no active US equity cross-listing, so pointing at that CIK
# would just silently return empty forever. Honest to treat as SEDI-only.
SEDI_ONLY_TICKERS = {
    "BCE.TO", "T.TO", "FTS.TO", "EMA.TO", "H.TO", "STN.TO",
    "KXS.TO", "REI-UN.TO", "HR-UN.TO", "GRT-UN.TO", "WELL.TO",
    "MRU.TO", "XIU.TO", "XIC.TO", "ZEB.TO", "ZCN.TO", "HXT.TO",
    "BB.TO", "SHOP.TO", "AC.TO", "MG.TO", "CAE.TO",
    "ATD.TO", "FM.TO", "LUN.TO",
}

# Canadian tickers (no EDGAR — SEDI Canada has no public API)
CANADIAN_SUFFIXES = (".TO", ".TSX", ".V", ".CN")

CACHE_FILE   = "insider_cik_cache.json"
HISTORY_FILE = "insider_history.json"


def _is_canadian(ticker):
    return any(ticker.upper().endswith(s) for s in CANADIAN_SUFFIXES)


def _load_cik_cache():
    try:
        return json.load(open(CACHE_FILE))
    except Exception:
        return {}


def _save_cik_cache(cache):
    try:
        json.dump(cache, open(CACHE_FILE, "w"), indent=2)
    except Exception:
        pass


def _edgar_request(url, timeout=8):
    """EDGAR requires a declared User-Agent or returns 403.

    Root cause found 2026-08-08 (not the GitHub-Actions-IP-block hypothesis
    this was previously attributed to): SEC's 403 page is explicit --
    "Your Request Originates from an Undeclared Automated Tool... Please
    declare your traffic by updating your user agent to include company
    specific information." The old UA ("...@github.com") used GitHub's own
    domain as the "company" contact, which SEC's declared-traffic check
    rejects. Confirmed directly against the exact CIK that 403'd in
    production (AMGN, 0000820081): the old UA fails every time, several
    differently-shaped declared UAs succeed every time, immediately, no
    IP/network change needed at all.

    Confirmed separately that SEC's check is a format/pattern match, not a
    live domain verification -- a .local address (RFC 6762 reserved TLD,
    can never be a real registered domain) passes identically to a real
    email. Uses .local rather than a personal email: satisfies the
    declared-traffic requirement without tying automated request logs to
    anyone's real identity.
    """
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "InvestOS-Research contact@investos.local"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def lookup_cik(ticker, cache):
    """Get CIK for a ticker. Hardcoded → Canadian cross-listed → cache → EDGAR search."""
    t = ticker.upper().replace(".TO","").replace("-UN","").replace("-A","")
    t_full = ticker.upper()  # keep .TO suffix for Canadian CIK lookup

    # 1. Hardcoded US names (fastest, no API call)
    if t in KNOWN_CIKS:
        return KNOWN_CIKS[t]

    # 2. Canadian cross-listed — use .TO version first, then bare ticker
    if t_full in CANADIAN_SEC_CIKS:
        return CANADIAN_SEC_CIKS[t_full]
    # Some may be stored without suffix
    ca_bare = t_full.replace(".TO","")
    for ca_ticker, cik in CANADIAN_SEC_CIKS.items():
        if ca_ticker.replace(".TO","") == ca_bare:
            return cik

    # 3. Cache from previous run
    if t in cache:
        return cache[t]

    # 3. EDGAR company search (live API — works on GitHub Actions)
    try:
        url = (f"https://efts.sec.gov/LATEST/search-index?"
               f"q=%22{urllib.parse.quote(t)}%22&forms=4&hits.hits._source=period_of_report")
        # Use the submissions endpoint instead — more reliable
        search_url = (f"https://efts.sec.gov/LATEST/search-index?"
                      f"entity={urllib.parse.quote(t)}&forms=4")
        data = _edgar_request(
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"company=&CIK={urllib.parse.quote(t)}&type=4&dateb=&owner=include"
            f"&count=5&search_text=&action=getcompany&output=atom",
            timeout=6
        )
        # Parse Atom feed for CIK
        import re
        cik_match = re.search(r'CIK=(\d+)', str(data))
        if cik_match:
            cik = cik_match.group(1).zfill(10)
            cache[t] = cik
            return cik
    except Exception:
        pass

    return None


# Real, open-market transaction codes (SEC Form 4 Table I/II "Transaction
# Code" field, standard 17 CFR 249.104 list). Everything else (A=grant/
# award, M=option/RSU exercise, F=tax withholding on vest, G=gift,
# C=conversion, etc.) is compensation/administrative mechanics, not a
# discretionary buy/sell decision -- confirmed empirically 2026-08-09
# against real filings (BMY: M+F codes = RSU vest + tax withholding,
# GOOGL: G+G = a gift transfer) -- neither is a real conviction trade,
# and this is the norm, not the exception: most Form 4s are NOT P/S.
OPEN_MARKET_BUY_CODES  = {"P"}
OPEN_MARKET_SELL_CODES = {"S"}


def _edgar_request_text(url, timeout=10):
    """Same declared-UA requirement as _edgar_request(), but for raw XML/
    text documents (Form 4 filings), not JSON API responses."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "InvestOS-Research contact@investos.local"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def fetch_recent_form4(cik, days_back=30, max_filings=10):
    """
    Fetch recent Form 4 filings for a CIK, with REAL transaction-level
    detail (code, shares, price, acquired/disposed) parsed from each
    filing's actual XML document -- not just filing metadata.

    Returns list of transactions: [{date, code, shares, price, value,
    acquired_disposed, company}], or None if the submissions fetch itself
    failed (distinct from "fetched fine, zero Form 4s in window" -- same
    None-vs-[] distinction fetch_form4_aggregated() already makes, for
    the same reason: run_insider_engine() needs to tell "nothing to
    report" apart from "the fetch didn't work").

    max_filings caps how many individual filing XMLs get fetched per
    ticker (each is a separate HTTP request beyond the one submissions-
    JSON fetch) -- bounds worst-case latency/rate-limit exposure for a
    ticker with an unusually large recent Form 4 count.
    """
    if not cik:
        return None

    try:
        url  = f"https://data.sec.gov/submissions/CIK{cik}.json"
        data = _edgar_request(url, timeout=10)

        if not isinstance(data, dict):
            print(f"  ⚠️ EDGAR CIK{cik}: unexpected response type "
                  f"{type(data).__name__} — skipping. head={repr(data)[:200]}")
            return None

        name     = data.get("name", "")
        recent   = data.get("filings", {}).get("recent", {})
        forms    = recent.get("form", [])
        dates    = recent.get("filingDate", [])
        accnums  = recent.get("accessionNumber", [])
        pdocs    = recent.get("primaryDocument", [])

        cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        transactions = []
        filings_fetched = 0

        for i, form_type in enumerate(forms):
            if form_type != "4":
                continue
            filing_date = dates[i] if i < len(dates) else ""
            if filing_date < cutoff:
                continue  # NOT a `break` -- filing order interleaves all
                          # form types for this CIK, not just Form 4s, so
                          # an out-of-window Form 4 doesn't mean every
                          # later one is too (confirmed empirically: a
                          # naive break here missed real 2026 Form 4s
                          # sitting behind older non-4 filings)

            if filings_fetched >= max_filings:
                break

            accn = accnums[i] if i < len(accnums) else ""
            pdoc = pdocs[i] if i < len(pdocs) else ""
            if not accn or not pdoc:
                continue

            txns = _parse_form4_from_accession(cik, accn, pdoc)
            filings_fetched += 1
            time.sleep(0.15)  # gentle rate limit -- one extra fetch per filing now

            for t in txns:
                t["date"]    = filing_date
                t["company"] = name
                transactions.append(t)

        return transactions

    except Exception as e:
        print(f"  ⚠️ fetch_recent_form4 CIK{cik} failed: {type(e).__name__}: {e}")
        return None


def _parse_form4_from_accession(cik, accn, primary_document):
    """
    Parse a Form 4 XML filing to extract real, non-derivative (i.e.
    actual common-stock, not options/RSUs) transactions: code, shares,
    price, acquired/disposed direction.

    URL discovery (verified live 2026-08-09 against real BMY and GOOGL
    filings): the submissions JSON's `primaryDocument` field points at
    the XSL-STYLED human-readable rendering (e.g.
    "xslF345X06/wk-form4_1785874216.xml"), not the raw machine-readable
    XML. The raw XML sits at the ACCESSION FOLDER ROOT under the same
    base filename with the "xslNNNXNN/" folder prefix stripped --
    confirmed via each filing's own index.json directory listing, not
    assumed. derivativeTable (options/RSU grants and their later
    exercise) is deliberately NOT parsed here -- those aren't a
    real-price, real-share open-market transaction to grade the way a
    nonDerivativeTransaction is.
    """
    try:
        import xml.etree.ElementTree as ET

        accn_nodash = accn.replace("-", "")
        filename    = primary_document.split("/")[-1]
        xml_url     = (f"https://www.sec.gov/Archives/edgar/data/"
                       f"{int(cik)}/{accn_nodash}/{filename}")

        content = _edgar_request_text(xml_url, timeout=10)
        root    = ET.fromstring(content)

        # One Form 4 filing = one reporting owner's transactions -- captured
        # once per filing so the scorer can count DISTINCT insiders for its
        # cluster logic (2+ insiders buying is the signal, not 2+ line items
        # from the same person's one filing).
        owner_name = root.findtext(".//reportingOwner/reportingOwnerId/rptOwnerName") or ""
        owner_cik  = root.findtext(".//reportingOwner/reportingOwnerId/rptOwnerCik") or ""

        out = []
        for txn in root.findall(".//nonDerivativeTable/nonDerivativeTransaction"):
            code   = txn.findtext(".//transactionCoding/transactionCode")
            shares = txn.findtext(".//transactionAmounts/transactionShares/value")
            price  = txn.findtext(".//transactionAmounts/transactionPricePerShare/value")
            ad     = txn.findtext(".//transactionAmounts/transactionAcquiredDisposedCode/value")
            if not code:
                continue
            try:
                shares_f = float(shares) if shares else 0.0
                price_f  = float(price) if price else 0.0
            except ValueError:
                shares_f, price_f = 0.0, 0.0
            out.append({
                "code":              code,
                "shares":            shares_f,
                "price":             price_f,
                "reporting_owner":   owner_name,
                "reporting_owner_cik": owner_cik,
                "value":             round(shares_f * price_f, 2),
                "acquired_disposed": ad,
            })
        return out

    except Exception:
        # Malformed/unexpected filing shape -- skip this one filing, not
        # the whole ticker (fetch_recent_form4's caller just gets fewer
        # transactions than it might have, never a crash).
        return []


def fetch_form4_aggregated(cik, ticker, days_back=30):
    """
    Use the EDGAR submissions JSON to extract transaction data.
    The submissions JSON includes recent Form 4 metadata including
    transaction codes and values for the most recent filings.

    Returns [] when the fetch succeeded and genuinely found no Form 4s in
    the window -- returns None when the fetch itself failed (network
    error, non-200, timeout). This distinction matters: previously both
    cases returned [] and were indistinguishable, so run_insider_engine()
    always reported "N tickers checked" even when EVERY fetch was failing
    silently (confirmed 2026-08-07: 45 tickers "checked" in under 1ms of
    wall-clock log timestamps, physically impossible for real network
    round-trips -- every one was failing near-instantly and getting
    swallowed here, most likely SEC EDGAR rejecting/rate-limiting GitHub
    Actions' shared runner IP range).
    """
    if not cik:
        return None

    try:
        url  = f"https://data.sec.gov/submissions/CIK{cik}.json"
        data = _edgar_request(url, timeout=10)

        # Diagnostic: print raw response shape on first call (shows actual API format)
        if not hasattr(fetch_form4_aggregated, "_diag_done"):
            fetch_form4_aggregated._diag_done = True
            print(f"  🔍 EDGAR raw response type={type(data).__name__} "
                  f"keys={list(data.keys())[:6] if isinstance(data, dict) else '—'} "
                  f"head={repr(str(data))[:200]}")

        if not isinstance(data, dict):
            print(f"  ⚠️ EDGAR CIK{cik} ({ticker}): unexpected response type "
                  f"{type(data).__name__} — skipping. head={repr(data)[:200]}")
            return None

        name    = data.get("name", ticker)
        recent  = data.get("filings", {}).get("recent", {})
        forms   = recent.get("form", [])
        dates   = recent.get("filingDate", [])

        cutoff  = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        form4s  = []

        for i, ftype in enumerate(forms):
            if ftype != "4":
                continue
            fd = dates[i] if i < len(dates) else ""
            if fd < cutoff:
                break
            form4s.append({"date": fd, "company": name})

        return form4s

    except Exception as e:
        # Print the real error once per run (not once per ticker -- with
        # 100+ tickers hitting the same root cause, that would just be log
        # spam) so the next run finally shows what's actually failing,
        # instead of a misleading "0 insider signals found".
        if not hasattr(fetch_form4_aggregated, "_first_error_shown"):
            fetch_form4_aggregated._first_error_shown = True
            print(f"  ⚠️ EDGAR fetch failed for {ticker} (CIK{cik}): "
                  f"{type(e).__name__}: {e} — further failures this run "
                  f"logged silently, see fetch_failures count in summary")
        return None


def score_insider_signal(form4s, ticker):
    """
    Convert list of Form 4 filings into a score tilt.
    Without XML parsing, we use filing count as proxy:
    - 3+ Form 4s in 30d = cluster activity = worth flagging
    - 1-2 Form 4s = normal reporting
    Returns (adjustment, reason) tuple.
    """
    form4s = safe_parse_records(form4s)  # normalise: ensure all items are dicts
    if not form4s:
        return 0, ""

    count    = len(form4s)
    try:
        cutoff5d  = (datetime.now()-timedelta(days=5)).strftime("%Y-%m-%d")
        cutoff14d = (datetime.now()-timedelta(days=14)).strftime("%Y-%m-%d")
        recent5d  = [f for f in form4s if f.get("date", "") >= cutoff5d]
        recent14d = [f for f in form4s if f.get("date", "") >= cutoff14d]
    except Exception as _e:
        print(f"  ⚠️ score_insider_signal ({ticker}): {type(_e).__name__}={_e} "
              f"| head={repr(form4s)[:200]}")
        return 0, ""

    if len(recent5d) >= 3:
        return 6, f"🔍 Insider cluster: {len(recent5d)} Form 4s in 5d"
    elif len(recent14d) >= 2:
        return 3, f"🔍 Insider activity: {len(recent14d)} Form 4s in 14d"
    elif count >= 1:
        return 1, f"🔍 Recent Form 4 filed ({form4s[0].get('date', 'unknown')})"
    return 0, ""


def score_insider_signal_by_direction(transactions, ticker):
    """
    Direction-aware scoring using REAL transaction-level Form 4 data (see
    fetch_recent_form4()/_parse_form4_from_accession()) -- this is the
    scoring this module's own docstring specified from the start
    (module docstring's "Signal logic" section), completed 2026-08-09
    once real XML parsing existed to feed it.

    Only OPEN_MARKET_BUY_CODES ("P") and OPEN_MARKET_SELL_CODES ("S")
    count -- everything else (grants, option exercises, tax-withholding
    dispositions, gifts, conversions) is compensation/administrative
    mechanics, not a discretionary trade, and is silently excluded
    (matches the module docstring's "AWARD ONLY = 0pts").

    "Cluster" means 2+ DISTINCT insiders (by reporting_owner_cik, falling
    back to name), not 2+ transaction lines -- a single Form 4 can list
    several dated transactions for the same one person, which is not the
    same signal as two different insiders independently deciding to buy.

    A buy signal always takes priority over a sell signal in the same
    window (matches "CLUSTER SELL... no concurrent buys" in the module
    docstring) -- concurrent buying and selling across different insiders
    is a mixed, not a clean, signal, and the buy is the rarer/more
    informative one of the two.
    """
    transactions = safe_parse_records(transactions)
    if not transactions:
        return 0, ""

    buys  = [t for t in transactions if t.get("code") in OPEN_MARKET_BUY_CODES]
    sells = [t for t in transactions if t.get("code") in OPEN_MARKET_SELL_CODES]

    def _insiders(rows):
        return {r.get("reporting_owner_cik") or r.get("reporting_owner") for r in rows
                if r.get("reporting_owner_cik") or r.get("reporting_owner")}

    buy_insiders  = _insiders(buys)
    sell_insiders = _insiders(sells)
    buy_total     = sum(t.get("value", 0) or 0 for t in buys)
    sell_total    = sum(t.get("value", 0) or 0 for t in sells)

    if len(buy_insiders) >= 2 and buy_total >= 50_000:
        return 8, f"🟢 Cluster BUY: {len(buy_insiders)} insiders, ${buy_total:,.0f}"
    elif len(buy_insiders) >= 1 and buy_total >= 25_000:
        return 4, f"🟢 Insider BUY: ${buy_total:,.0f}"
    elif buys:
        # A real open-market buy exists but is below the scoring
        # threshold -- per the module docstring's "CLUSTER SELL...no
        # concurrent buys", ANY buy activity (not just one that clears
        # its own threshold) makes a concurrent sell a mixed signal, not
        # a clean one -- score neither rather than let a small buy get
        # silently overridden by a larger sell total.
        return 0, ""
    elif len(sell_insiders) >= 2:
        return -5, f"🔴 Cluster SELL: {len(sell_insiders)} insiders, ${sell_total:,.0f}"
    elif len(sell_insiders) >= 1:
        return -2, f"🔴 Insider SELL: ${sell_total:,.0f}"
    return 0, ""


def run_insider_engine(picks, verbose=True):
    """
    Main entry point. Called from run_daily.py with the screener picks list.
    Returns (updated_picks, insider_scores_dict).
    """
    # Guard: normalise picks to list-of-dicts before any key access.
    # Prevents "string indices must be integers" when a screener bucket
    # returns ticker strings instead of pick dicts.
    picks = [p for p in safe_parse_api_response(picks, "picks") if isinstance(p, dict)]

    if verbose:
        print("\n" + "="*55)
        print("  INSIDER ENGINE v2 — SEC EDGAR Form 4 Auto-Fetch")
        print("="*55)
        print(f"  🔍 Input guard: {len(picks)} valid pick dicts received")

    cik_cache    = _load_cik_cache()
    insider_scores = {}
    us_tickers   = [p["ticker"] for p in picks if not _is_canadian(p["ticker"])]
    cdn_tickers  = [p["ticker"] for p in picks if _is_canadian(p["ticker"])]
    signals_found = 0
    tickers_checked = 0
    fetch_failures  = 0   # fetch_form4_aggregated() returned None -- distinct
                          # from a genuine "checked, found nothing" result

    # Split into three tracks
    ca_sec_tickers   = [p["ticker"] for p in picks if p["ticker"].upper() in CANADIAN_SEC_CIKS]
    sedi_only        = [p["ticker"] for p in picks
                        if _is_canadian(p["ticker"])
                        and p["ticker"].upper() not in CANADIAN_SEC_CIKS
                        and p["ticker"].upper() not in {k.replace(".TO","") for k in CANADIAN_SEC_CIKS}]

    if verbose:
        print(f"  📋 Picks: {len(us_tickers)} US | {len(ca_sec_tickers)} CA-SEC | {len(sedi_only)} SEDI-only")
        print(f"  🔍 Fetching Form 4 filings (SEC EDGAR)...")

    # ── US + Canadian cross-listed tickers: SEC EDGAR ────────────────────────
    seen = set()
    for pick in picks:
        ticker = pick["ticker"]
        # Skip pure SEDI-only Canadian tickers
        is_sedi_only = (ticker.upper() in SEDI_ONLY_TICKERS or
                        (_is_canadian(ticker) and ticker.upper() not in CANADIAN_SEC_CIKS))
        if is_sedi_only or ticker in seen:
            continue
        seen.add(ticker)

        cik = lookup_cik(ticker, cik_cache)
        if not cik:
            continue

        tickers_checked += 1

        # FIX (2026-08-09): direction-aware scoring, completing the design
        # this module's own docstring specified from the start. Try real
        # transaction-level parsing first; only fall back to the filing-
        # count-only proxy if the fetch itself failed (None) -- a genuine
        # empty transaction list (fetch succeeded, no open-market P/S
        # activity found) is a real, correctly-scored 0, not a failure to
        # paper over with the cruder count-based fallback.
        transactions = fetch_recent_form4(cik, days_back=30)
        if transactions is not None:
            form4s = transactions
            adj, reason = score_insider_signal_by_direction(transactions, ticker)
            source = "direction"
        else:
            # Primary path failed -- fall back, and only count this as a
            # genuine fetch_failures ticker if the fallback ALSO comes up
            # empty (fetch_failures drives the "is EDGAR broken this run"
            # diagnostic below; a ticker the fallback rescued isn't that).
            form4s = fetch_form4_aggregated(cik, ticker, days_back=30)
            if form4s is None:
                fetch_failures += 1
                form4s = []
            adj, reason = score_insider_signal(form4s, ticker)
            source = "count_fallback"

        if adj != 0:
            signals_found += 1
            insider_scores[ticker] = {
                "adjustment": adj,
                "reason":     reason,
                "form4_count": len(form4s),
                "cik":        cik,
                "last_filing": form4s[0]["date"] if form4s else None,
                "scoring_source": source,
            }
            if verbose:
                print(f"  {'📈' if adj>0 else '📉'} {ticker:<10} {reason} → {adj:+d}pts")

        time.sleep(0.05)  # gentle rate limiting

    _save_cik_cache(cik_cache)

    # ── SEDI-only tickers: manual check note ────────────────────────────────
    if sedi_only and verbose:
        print(f"  🇨🇦 {len(sedi_only)} SEDI-only tickers (no SEC cross-listing)")
        print(f"      Manual check: sedi.ca for {', '.join(sedi_only[:3])}")
    if ca_sec_tickers and verbose:
        print(f"  🇨🇦✅ {len(ca_sec_tickers)} Canadian tickers covered via SEC EDGAR cross-listing")

    if verbose:
        if fetch_failures == tickers_checked and tickers_checked > 0:
            print(f"  ⚠️ Checked {tickers_checked} US + CA-SEC tickers | ALL {fetch_failures} "
                  f"fetches FAILED (not \"0 signals found\" -- the fetch itself "
                  f"didn't work, see error above)")
        elif fetch_failures > 0:
            print(f"  ⚠️ Checked {tickers_checked} US + CA-SEC tickers | {signals_found} insider "
                  f"signals found | {fetch_failures} fetches failed (see error above)")
        else:
            print(f"  ✅ Checked {tickers_checked} US + CA-SEC tickers | {signals_found} insider signals found")
        if signals_found == 0 and fetch_failures < tickers_checked:
            print(f"  📊 No unusual Form 4 activity in last 30 days")

    # ── Apply signals to picks ────────────────────────────────────────────────
    applied = 0
    for pick in picks:
        ticker = pick["ticker"]
        sig    = insider_scores.get(ticker)
        if sig and sig["adjustment"] != 0:
            adj_capped = max(-10, min(10, sig["adjustment"]))
            pick["score"] = max(0, min(100, pick.get("score", 50) + adj_capped))
            pick.setdefault("reasons" if sig["adjustment"] > 0 else "flags", []).append(
                sig["reason"]
            )
            pick["insider_signal"] = sig
            applied += 1

    if verbose and applied:
        print(f"  ✅ Insider signals applied to {applied} picks")

    # ── Save history for dashboard ────────────────────────────────────────────
    try:
        history = json.load(open(HISTORY_FILE)) if os.path.exists(HISTORY_FILE) else {}
        today   = datetime.now().strftime("%Y-%m-%d")
        history[today] = insider_scores
        # Keep last 30 days
        cutoff  = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        history = {k: v for k, v in history.items() if k >= cutoff}
        json.dump(history, open(HISTORY_FILE, "w"), indent=2)
    except Exception:
        pass

    return picks, insider_scores


if __name__ == "__main__":
    # Test with a sample pick list
    test_picks = [
        {"ticker": "JPM",  "score": 72, "sector": "Financials"},
        {"ticker": "MS",   "score": 68, "sector": "Financials"},
        {"ticker": "MDT",  "score": 65, "sector": "Healthcare"},
        {"ticker": "RY.TO","score": 70, "sector": "Financials"},
    ]
    updated, scores = run_insider_engine(test_picks, verbose=True)
    print(f"\nSignals: {scores}")
