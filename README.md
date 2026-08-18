# INVESTOS

Automated daily investment intelligence platform. Every weekday morning, a
GitHub Actions pipeline screens a stock universe, scores it with a
gradient-boosted ML model layered on top of rules-based fundamentals, reads
macro signals from a basket of financial news feeds, sizes positions by
volatility and Kelly criterion, and bakes the result into a live dashboard.

**Live dashboard:** [tolulop3.github.io/investos](https://tolulop3.github.io/investos)

## What it does

- **Screens** a North American equity universe daily, scoring each name on
  momentum, growth, value, safety, and liquidity.
- **Layers ML on top** — an XGBoost model trained on the system's own
  historical outcomes, gated behind a minimum-accuracy bar before anything
  it produces is trusted for sizing.
- **Reads the macro tape** — aggregates multiple financial news sources into
  sector-level sentiment and a market regime read (risk-on / neutral /
  risk-off), which scales position sizing up or down.
- **Sizes positions** with a blend of Kelly criterion (edge-aware) and
  volatility targeting, with hard concentration and sector-diversification
  caps.
- **Tracks every call it makes** against real outcomes, splits performance
  by score tier and ML-confidence bucket, and surfaces a rolling walk-forward
  view of whether the system is actually adding alpha over time — not just a
  single point-in-time backtest.
- **Extends to ETFs and select international markets** with dedicated
  scoring engines tuned to each asset class's own account rules.

## Status

Personal research project, not a public product and not investment advice.
The system is under active, continuous development — rules, weights, and
gates change as evidence accumulates from live (out-of-sample) results.

## Stack

Python, XGBoost / scikit-learn, GitHub Actions (scheduling + CI), a
vanilla-JS dashboard baked to static HTML and served via GitHub Pages.

---

*NFA · Educational only. Not affiliated with any brokerage or exchange.*
