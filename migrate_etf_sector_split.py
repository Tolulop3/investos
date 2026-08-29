"""migrate_etf_sector_split.py — one-off, run once at deploy of the SECTOR split.

Context: 2026-08-29 the ETF engine's single "SECTOR" category was split into
SECTOR_COMMODITY (XEG.TO, XLE, GLD, ZGD.TO) and SECTOR_EARNINGS (ZEB.TO, XRE.TO)
because the two halves move ~5-6x apart and can't share one resolution
threshold. etf_outcomes.json entries logged before the split still carry
"category": "SECTOR". Nothing is resolved yet (first resolution 2026-09-06), so
re-labelling now makes the split effective for every outcome that ever resolves.

Idempotent: re-running is a no-op once no "SECTOR" rows remain. Only touches
rows whose category is exactly "SECTOR"; resolved rows and every other category
are left alone. CATEGORY_THRESHOLDS also keeps a "SECTOR" -> 1.00 alias, so a
row this script can't map still resolves sanely.

Run:  python3 migrate_etf_sector_split.py            # migrate ./etf_outcomes.json
      python3 migrate_etf_sector_split.py --dry-run  # report only, no write
"""

import json
import os
import sys

ETF_OUTCOMES_FILE = "etf_outcomes.json"

# Mirror of the split in etf_engine.py's ETF_UNIVERSE (kept literal here so this
# script doesn't need to import etf_engine, which pulls in yfinance).
SECTOR_COMMODITY = {"XEG.TO", "XLE", "GLD", "ZGD.TO"}
SECTOR_EARNINGS  = {"ZEB.TO", "XRE.TO"}


def remap_category(ticker, category):
    """Return the post-split category for one entry. Non-SECTOR rows and
    unmappable SECTOR tickers come back unchanged."""
    if category != "SECTOR":
        return category
    if ticker in SECTOR_COMMODITY:
        return "SECTOR_COMMODITY"
    if ticker in SECTOR_EARNINGS:
        return "SECTOR_EARNINGS"
    return category


def migrate(outcomes):
    """Mutate `outcomes` in place. Returns (n_changed, [unmapped_tickers])."""
    changed = 0
    unmapped = []
    for o in outcomes:
        old = o.get("category")
        new = remap_category(o.get("ticker"), old)
        if new != old:
            o["category"] = new
            changed += 1
        elif old == "SECTOR":
            unmapped.append(o.get("ticker"))
    return changed, unmapped


def _main(argv):
    dry_run = "--dry-run" in argv
    if not os.path.exists(ETF_OUTCOMES_FILE):
        print(f"   {ETF_OUTCOMES_FILE} not found — nothing to migrate")
        return 0

    outcomes = json.load(open(ETF_OUTCOMES_FILE))
    before = sum(1 for o in outcomes if o.get("category") == "SECTOR")
    changed, unmapped = migrate(outcomes)

    print(f"   SECTOR rows before: {before}")
    print(f"   re-labelled:        {changed}")
    if unmapped:
        from collections import Counter
        print(f"   ⚠️  left as SECTOR (ticker not in split map): {dict(Counter(unmapped))}")

    if dry_run:
        print("   --dry-run: no file written")
        return 0

    if changed:
        json.dump(outcomes, open(ETF_OUTCOMES_FILE, "w"), indent=2, default=str)
        print(f"   ✅ wrote {ETF_OUTCOMES_FILE}")
    else:
        print("   nothing to write")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
