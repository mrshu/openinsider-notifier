# Insider Cluster Research Protocol

## Objective

Test whether SEC Form 4 insider purchase clusters can support a
long-horizon US equity strategy with a minimum 366-calendar-day hold.
The research goal is falsification: the primary test determines whether
the signal is worth further work, and robustness grids may not be used as
model-selection evidence by themselves.

## Primary Hypothesis

Clustered Form 4 code-P non-derivative acquisitions by eligible insiders
produce positive 12-month sector/size-adjusted alpha after realistic
entry timing, costs, liquidity constraints, and duplicate-signal controls.

## Preregistered Primary Strategy

- Source: SEC Form 4 and Form 4/A XML.
- Transactions: non-derivative only.
- Transaction code: `P` only.
- Acquired/disposed code: `A` only.
- Minimum single-owner purchase value: `$100,000`.
- Eligible insiders: CEO, CFO, COO, President, Chairman, named officers,
  and directors.
- Exclude passive 10% owners unless they are also officers or directors.
- Cluster: at least 2 distinct eligible insiders buying the same issuer
  within 5 trading days.
- Minimum cluster value: `$250,000`.
- Signal time: latest SEC filing acceptance timestamp needed to satisfy
  the cluster.
- Entry: next tradable open after the cluster signal is public.
- Primary entry rule: no insider-VWAP skip.
- VWAP rule is an ablation: skip if entry price is more than 15% above
  insider VWAP.
- Hold: minimum 366 calendar days.
- Exit: first tradable day after 366 days, unless delisting, merger close,
  bankruptcy, fraud/restatement, or halted/untradeable security.
- Costs: at least 10 bps each side plus liquidity/slippage model.
- Position sizing: equal weight, maximum 5% initial position, maximum
  30 names, maximum 20% sector exposure.
- Universe: US common stocks only. Exclude ETFs, funds, preferreds,
  warrants, SPAC shells, and ADRs unless tested separately.
- Duplicate signals: collapse qualifying purchases for the same issuer
  into one cluster episode; allow only one active issuer position until
  the prior position exits.

## Required Variants

The primary test is the pass/fail anchor. These variants are ablations or
robustness checks, not a license to select the best result.

- Raw eligible Form 4 purchases.
- Single-insider purchases >= `$100,000`.
- Cluster only.
- Cluster + 366-calendar-day hold.
- Cluster + valuation.
- Cluster + valuation + buyback/share-count reduction.
- Cluster + valuation + buyback + FCF/no-distress.

## Robustness Grid

- Cluster window: 3, 5, 10 trading days.
- Minimum purchase: `$50,000`, `$100,000`, `$250,000`, `$500,000`.
- Minimum cluster value: `$100,000`, `$250,000`, `$500,000`, `$1,000,000`.
- Entry lag: next open, next close, +1 trading day.
- Hold: 90d, 180d, 366d, 504d.

Sensitivity-grid winners do not count as evidence unless they are
directionally consistent with the primary result and survive
multiple-testing controls.

## Bias Controls

- No transaction-date trading.
- No trading before public filing acceptance.
- No current-universe filtering.
- No look-ahead fundamentals.
- Include delisted names if the price source supports them; otherwise mark
  the run `SURVIVORSHIP-BIASED` and non-production.
- Preserve historical CIK/ticker mappings.
- Use split-adjusted and dividend-adjusted total returns where available.
- Include merger exits, bankruptcies, halted/untradeable cases, and
  delisting returns where possible.
- Track every tested variant and every dropped record with a reason.
- Separate train, validation, and test periods before any tuning.
- Report sector-neutral and size-neutral benchmarks where possible.

## Required Artifacts

- `raw_form4_transactions.csv`
- `eligible_owner_purchases.csv`
- `cluster_signals.csv`
- `cluster_signals_filtered.csv`
- `trades.csv`
- `positions.csv`
- `daily_portfolio_returns.csv`
- `event_returns.csv`
- `summary_metrics.csv`
- `diagnostics.md`

## Required Diagnostics

- Filings downloaded.
- Filings parsed.
- Form 4 vs Form 4/A count.
- P transactions found.
- Transactions dropped by reason.
- Unique issuers.
- Unique insiders.
- Cluster signals.
- Price matches.
- Missing price cases.
- Delisted cases.
- Factor availability by year.
- Signal count by year.
- Signal count by sector and market-cap bucket.

## Required Synthetic Tests

- Transaction-date lookahead is impossible.
- Cluster date is the second qualifying public filing timestamp.
- Duplicate active issuer signals are suppressed.
- 366-calendar-day minimum hold is enforced.
- Split-adjusted prices are used correctly.
- Missing/delisted tickers are not silently dropped.

## Conclusion Standard

Do not claim statistical edge unless:

- The preregistered primary strategy is positive out of sample.
- Median trade return is positive.
- Drawdown is acceptable.
- Results survive costs and liquidity haircuts.
- Results are not dominated by one sector, one year, or a few extreme
  winners.
- The same conclusion holds under reasonable parameter perturbations.
- Multiple-testing and data-snooping risks are explicitly addressed.

Conclusion labels:

- `tradeable`: passes primary and robustness standards with production-grade
  data.
- `do not trade`: fails the primary test or fails core robustness checks.
- `inconclusive`: promising but blocked by data quality, coverage, or
  insufficient out-of-sample evidence.
- `exploratory only`: uses Yahoo/free data without valid delisting and
  point-in-time coverage.
