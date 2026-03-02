# InvestOS 📊

Personal AI investment signal system. Runs automatically every morning via GitHub Actions. Screens 500+ stocks, scans 15 news sources, generates FX and crypto signals, and emails you a clean brief before markets open.

**Live dashboard:** `https://[your-username].github.io/investos`

---

## What it does

Every weekday at ~6-7am Toronto time, automatically:

- Screens 500+ TSX + US + Global stocks using ML scoring
- Reads 15 free RSS news feeds for macro signals
- Generates FX directional calls (EUR/USD, GBP/USD, USD/CAD, USD/JPY, Gold)
- Signals for BTC and SOL
- Computes a deployment plan — you type how much you're investing, it tells you exactly where each dollar goes
- Bakes everything into the dashboard HTML
- Sends you a morning brief email with the key picks
- Emails you only if the run fails (separate failure alert)

---

## Setup (one time, ~20 minutes)

### 1. Fork this repo
Click **Fork** at the top right of this page.

### 2. Enable GitHub Pages
Repo → Settings → Pages → Source: **Deploy from branch** → Branch: `main` → Folder: `/ (root)` → Save

Your dashboard will be live at `https://[your-username].github.io/investos`

### 3. Set up Gmail for sending emails

1. Create a dedicated Gmail account (e.g. `yourname.investos@gmail.com`)
2. In that Gmail account: Settings → Security → **2-Step Verification** → enable it
3. Then: Settings → Security → **App Passwords** → create one called "InvestOS"
4. Copy the 16-character app password

### 4. Add GitHub Secrets
Repo → Settings → Secrets and variables → Actions → **New repository secret**

Add these three secrets:

| Secret name      | Value                                      |
|------------------|--------------------------------------------|
| `GMAIL_USER`     | `yourname.investos@gmail.com`              |
| `GMAIL_PASSWORD` | The 16-character app password from step 3  |
| `NOTIFY_EMAIL`   | Your personal email where you want briefs  |

### 5. Update your account balances
Open `portfolio_engine.py` and update the two balance lines:

```python
"FHSA": {
    "balance": 200,   # ← change to your actual FHSA balance
    ...
"TFSA": {
    "balance": 10000, # ← change to your actual TFSA balance
```

Commit and push. The next scheduled run picks it up automatically.

### 6. Trigger a manual run to test
Repo → Actions → **InvestOS Daily Run** → **Run workflow** → Run

Watch the logs. If it succeeds, check your email and your dashboard URL.

---

## File structure

```
investos/
├── .github/
│   └── workflows/
│       └── daily_run.yml      ← GitHub Actions schedule
├── run_daily.py               ← Master runner + email sender
├── portfolio_engine.py        ← CONFIG (update balances here)
├── stock_screener.py          ← 500+ stock universe
├── news_analyzer.py           ← 15 RSS news sources
├── ml_engine.py               ← XGBoost ML + regime filter
├── intelligence_layers.py     ← RS rankings, score history
├── fx_engine.py               ← FX + Gold signals
├── crypto_engine.py           ← BTC + SOL signals
├── risk_engine.py             ← Stress tests, accuracy tracking
├── content_engine.py          ← Social content generator
├── dashboard.html             ← Your live dashboard (auto-updated)
├── requirements.txt           ← Python dependencies
├── .gitignore                 ← Keeps credentials out of repo
└── README.md                  ← This file
```

---

## Two emails. That's it.

**Morning brief** — arrives after every successful run. Regime, top picks, deployment plan, FX call, crypto signals. Everything you need in under a minute.

**Failure alert** — only when something breaks. Check the Actions log to see what went wrong.

Nothing else ever lands in your inbox from this system.

---

## The bucket structure

Your TFSA is split into four buckets with different failure modes:

| Bucket | % | Purpose | If it fails |
|--------|---|---------|-------------|
| Floor | 50% | Dividend core — ENB, RY, T.TO | Still pays 5-7%/yr dividends |
| Model | 30% | ML growth picks | Max loss = 30% of TFSA |
| Swing | 15% | Short-term trades | Max loss = 15% of TFSA |
| Crypto | 5% | BTC + SOL | Max loss = 5% of TFSA |

If the ML model fails completely for 12 months — the Floor still pays dividends, the FHSA still compounds, worst case is survivable.

---

## Not financial advice

This is a personal tool for signal filtering. The model makes mistakes. Always verify before executing. Use stop losses. Never invest money you cannot afford to lose.
