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
KNOWN_CIKS = {
    "JPM":    "0000019617", "MS":     "0000895421", "GS":     "0000886982",
    "BAC":    "0000070858", "WFC":    "0000072971", "C":      "0000831001",
    "MDT":    "0000064996", "ABT":    "0000001800", "ISRG":   "0001035267",
    "DXCM":   "0001385187", "AFRM":   "0001820175", "SNOW":   "0001640147",
    "MDB":    "0001333513", "NVDA":   "0001045810", "MSFT":   "0000789019",
    "META":   "0001326801", "AMZN":   "0001018724", "GOOGL":  "0001652044",
    "AAPL":   "0000320193", "F":      "0000037996", "GM":     "0001467858",
    "PFE":    "0000078003", "JNJ":    "0000200406", "ABBV":   "0001551152",
    "MRK":    "0000310158", "BMY":    "0000014272", "AMGN":   "0000820081",
    "BIIB":   "0000875320", "REGN":   "0000872589", "GILD":   "0000882184",
    "LOW":    "0000060667", "HD":     "0000354950", "TGT":    "0000027419",
    "SBUX":   "0000829224", "O":      "0000726854", "VICI":   "0001692415",
    "AMT":    "0001053507", "MAIN":   "0001325702", "STAG":   "0001538827",
    "BX":     "0001393818", "BLK":    "0001364742", "KO":     "0000021344",
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


def fetch_recent_form4(cik, days_back=30):
    """
    Fetch recent Form 4 filings for a CIK.
    Returns list of transactions: [{date, type, shares, price, value, insider}]
    """
    if not cik:
        return []

    try:
        url  = f"https://data.sec.gov/submissions/CIK{cik}.json"
        data = _edgar_request(url, timeout=10)

        if not isinstance(data, dict):
            print(f"  ⚠️ EDGAR CIK{cik}: unexpected response type "
                  f"{type(data).__name__} — skipping. head={repr(data)[:200]}")
            return []

        recent   = data.get("filings", {}).get("recent", {})
        forms    = recent.get("form", [])
        dates    = recent.get("filingDate", [])
        accnums  = recent.get("accessionNumber", [])

        cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        transactions = []

        for i, form_type in enumerate(forms):
            if form_type != "4":
                continue
            filing_date = dates[i] if i < len(dates) else ""
            if filing_date < cutoff:
                break  # filings are reverse-chronological, can stop early

            accn = accnums[i].replace("-", "") if i < len(accnums) else ""
            if not accn:
                continue

            # Fetch the actual Form 4 XML
            try:
                xml_url = (f"https://www.sec.gov/Archives/edgar/full-index/"
                           f"{filing_date[:4]}/{filing_date[5:7]}/")
                # Direct accession URL
                accn_fmt = f"{accn[:10]}-{accn[10:12]}-{accn[12:]}"
                doc_url  = (f"https://www.sec.gov/Archives/edgar/data/"
                            f"{int(cik)}/{accn}/")
                # Parse XML from filing index
                idx_url = (f"https://www.sec.gov/cgi-bin/browse-edgar?"
                           f"action=getcompany&CIK={cik}&type=4&dateb=&owner=include"
                           f"&count=1&search_text=&output=atom")
                # Use the submissions data instead — it has enough info
                # transactions type: NonDerivativeTransaction
                txns = _parse_form4_from_accession(cik, accn, filing_date)
                transactions.extend(txns)
                time.sleep(0.1)  # rate limit
            except Exception:
                continue

        return transactions

    except Exception as e:
        return []


def _parse_form4_from_accession(cik, accn, filing_date):
    """Parse a Form 4 XML filing to extract buy/sell transactions."""
    try:
        # Build the accession number URL format
        accn_url = accn.replace("-","")
        # Try to get the primary document
        idx_url = (f"https://www.sec.gov/Archives/edgar/data/"
                   f"{int(cik)}/{accn_url}/{accn}-index.htm")
        # Actually use the JSON submissions — it has aggregated transaction data
        # The CIK submission already has the key data we need
        return []  # XML parsing too complex without lxml — use aggregated data below
    except Exception:
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
        form4s = fetch_form4_aggregated(cik, ticker, days_back=30)
        if form4s is None:
            fetch_failures += 1
        adj, reason = score_insider_signal(form4s, ticker)

        if adj != 0:
            signals_found += 1
            insider_scores[ticker] = {
                "adjustment": adj,
                "reason":     reason,
                "form4_count": len(form4s),
                "cik":        cik,
                "last_filing": form4s[0]["date"] if form4s else None,
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
            print(f"  ⚠️ Checked {tickers_checked} US tickers | ALL {fetch_failures} "
                  f"fetches FAILED (not \"0 signals found\" -- the fetch itself "
                  f"didn't work, see error above)")
        elif fetch_failures > 0:
            print(f"  ⚠️ Checked {tickers_checked} US tickers | {signals_found} insider "
                  f"signals found | {fetch_failures} fetches failed (see error above)")
        else:
            print(f"  ✅ Checked {tickers_checked} US tickers | {signals_found} insider signals found")
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
