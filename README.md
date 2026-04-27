# openinsider-notifier

## Daily SEC insider signal scan

The daily scanner uses official SEC current Form 4/4-A filings, parses the
ownership XML, keeps only non-derivative code-P acquisitions by eligible
officers/directors, and enriches the result with free Yahoo Finance price and
ADV60 data.

Run it locally:

```bash
uv run python -m research.daily_signal_scan \
  --output-dir data/live/sec_daily \
  --state-file data/live/sec_daily/state.json \
  --lookback-hours 96 \
  --max-entries 500
```

The scanner writes:

- `latest_form4_filings.csv`
- `raw_form4_transactions_latest.csv`
- `eligible_purchase_history.csv`
- `watchlist_candidates_latest.csv`
- `watchlist_episodes_latest.csv`
- `alert_messages_latest.md`
- `daily_digest.md`
- `candidate_forward_returns.csv`
- `forward_return_report.md`
- `candidate_history.csv`
- `candidate_episode_history.csv`
- `daily_diagnostics.md`

The monitor rule is intentionally strict:

- SEC Form 4/4-A non-derivative transaction code `P`
- acquisition only
- officer/director or named officer
- purchase value at least `$100,000`
- current price no more than `15%` above insider VWAP
- purchase value / ADV60 at least `2%`, or purchase value at least `$1m`

Alert candidates are stricter: purchase value / ADV60 at least `5%`, or
purchase value at least `$2m`. The workflow `.github/workflows/sec-daily-scan.yaml`
runs this daily, commits `data/live/sec_daily`, sends Matrix alerts when
`ARGENTARIS_MATRIX_PASSWORD` is present, and sends Discord alerts when
`DISCORD_WEBHOOK_URL` is present.

Each candidate is also assigned an explainable `research_score` and
`research_tier`. The score is intentionally simple and stored with JSON
reasons/caveats so every alert can be audited later. The workflow updates a
free-data forward-return audit after every scan:

```bash
uv run python -m research.forward_returns
```

## Free-data insider backtest

This repo includes a prototype backtest that uses only free data:

- `trades.jsonl` for OpenInsider-style insider purchase events
- Yahoo Finance daily adjusted prices via `yfinance`
- `SPY` as the default benchmark

Run the backtest:

```bash
uv run python -m backtest.free_data_backtest \
  --events trades.jsonl \
  --output-dir data/results/free_backtest
```

Download a larger free OpenInsider history first:

```bash
uv run python -m backtest.download_openinsider \
  --days 1095 \
  --output data/raw/openinsider_3y.jsonl

uv run python -m backtest.free_data_backtest \
  --events data/raw/openinsider_3y.jsonl \
  --output-dir data/results/openinsider_3y
```

The command writes:

- `normalized_events.csv`
- `signals.csv`
- `event_returns.csv`
- `event_summary.csv`
- `daily_portfolio.csv`
- `portfolio_summary.csv`

The built-in strategy variants are:

- `all_purchases`: all open-market purchases above `--min-value`
- `officer_director`: purchases by likely officers/directors
- `cluster`: 2+ distinct insiders in the same ticker within the cluster window
- `cluster_plus_30pct_drawdown`: cluster buys where the stock is at least
  30% below its trailing 52-week high

Default assumptions:

- minimum purchase value: `$100,000`
- minimum cluster value: `$250,000`
- cluster window: `7` calendar days, used as a free-data approximation for
  5 trading days
- hold period: `252` trading days
- entry: first available adjusted close after the filing date
- exit: adjusted close after the hold period

This is a free-data prototype, not a production-grade institutional backtest.
Yahoo Finance can omit delisted tickers, which creates survivorship bias, and
the current version does not include point-in-time valuation or buyback data.
Those should come from SEC filings if this signal is worth deeper research.
