#!/usr/bin/env python3
"""
NGX API Endpoint Diagnostic
Run this locally or add as a GitHub Actions step to test which endpoints
are accessible on the current NGN Markets API plan.

Usage:
  NGN_MARKETS_KEY=your_key python ngx_diagnostic.py

Results will show which endpoints return 200 (accessible) vs 403 (paid tier only).
"""

import urllib.request, json, os, sys

KEY  = os.environ.get("NGN_MARKETS_KEY", "")
BASE = "https://api.ngnmarket.com/v1"
SAMPLE_TICKER = "DANGCEM"  # Dangote Cement — most liquid NGX stock

if not KEY:
    print("ERROR: Set NGN_MARKETS_KEY environment variable")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {KEY}",
    "Accept": "application/json",
    "User-Agent": "InvestOS/4.0"
}

tests = [
    # Format: (path, label, what_to_look_for)
    ("/market/snapshot",                      "Market snapshot (known free)", ["asi", "breadth", "adv"]),
    ("/market/movers",                        "Market movers",                ["ticker", "price", "close", "change"]),
    ("/market/movers?type=gainers&limit=30",  "Movers gainers",               ["ticker", "price"]),
    ("/market/movers?type=losers&limit=30",   "Movers losers",                ["ticker", "price"]),
    (f"/companies/{SAMPLE_TICKER}",           "Company profile",              ["price", "close", "current"]),
    (f"/equities/{SAMPLE_TICKER}",            "Equities quote",               ["price", "close", "current"]),
    (f"/market/equities/{SAMPLE_TICKER}",     "Market equities quote",        ["price", "close"]),
    ("/companies",                            "All companies list",            ["ticker", "symbol"]),
    ("/equities",                             "All equities list",             ["ticker", "symbol"]),
    ("/market/equities",                      "Market equities bulk",         ["ticker", "price"]),
]

print(f"NGX API Diagnostic — {SAMPLE_TICKER}")
print("="*60)
print()

accessible = []
blocked    = []

for path, label, look_for in tests:
    url = f"{BASE}{path}"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read().decode()
            data = json.loads(body) if body.strip().startswith("{") or body.strip().startswith("[") else {}
            body_lower = body.lower()
            
            found_fields = [f for f in look_for if f in body_lower]
            status = "✅ 200"
            accessible.append(path)
            
            print(f"{status} {label}")
            print(f"   URL: {url}")
            if found_fields:
                print(f"   Contains: {found_fields}")
            # Print first 200 chars of response
            print(f"   Response: {body[:200]}")
            print()
            
    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode()[:100]
        except: pass
        status = f"❌ {e.code}"
        blocked.append(path)
        print(f"{status} {label}")
        print(f"   URL: {url}")
        print()

print("="*60)
print(f"Accessible: {len(accessible)}")
print(f"Blocked:    {len(blocked)}")
if accessible:
    print(f"\nFree endpoints:")
    for p in accessible:
        print(f"  {p}")
