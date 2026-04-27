from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests


SEC_HEADERS = {
    "User-Agent": "openinsider-notifier research contact@example.com",
    "Accept-Encoding": "gzip, deflate",
}

TAGS = {
    "shares": [
        "EntityCommonStockSharesOutstanding",
        "CommonStocksIncludingAdditionalPaidInCapitalSharesOutstanding",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingDiluted",
    ],
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
    ],
    "ocf": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "capex": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ],
    "net_income": ["NetIncomeLoss"],
    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
}


def sec_get_json(url: str, sleep_seconds: float) -> dict[str, Any]:
    time.sleep(sleep_seconds)
    response = requests.get(url, headers=SEC_HEADERS, timeout=60)
    response.raise_for_status()
    return response.json()


def load_companyfacts(cik: str, cache_dir: Path, sleep_seconds: float) -> dict[str, Any]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"CIK{str(int(cik)).zfill(10)}.json"
    if path.exists():
        return json.loads(path.read_text())
    data = sec_get_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(int(cik)).zfill(10)}.json", sleep_seconds)
    path.write_text(json.dumps(data), encoding="utf-8")
    return data


def fact_rows(companyfacts: dict[str, Any], tags: list[str]) -> list[dict[str, Any]]:
    facts = companyfacts.get("facts", {}).get("us-gaap", {})
    rows = []
    for tag in tags:
        fact = facts.get(tag)
        if not fact:
            continue
        for unit, values in fact.get("units", {}).items():
            for value in values:
                row = dict(value)
                row["tag"] = tag
                row["unit"] = unit
                rows.append(row)
    return rows


def latest_fact_before(
    companyfacts: dict[str, Any],
    tags: list[str],
    signal_date: pd.Timestamp,
    *,
    annualish: bool = False,
) -> float | None:
    candidates = []
    for row in fact_rows(companyfacts, tags):
        filed = pd.to_datetime(row.get("filed"), errors="coerce")
        if pd.isna(filed) or filed > signal_date:
            continue
        if annualish:
            start = pd.to_datetime(row.get("start"), errors="coerce")
            end = pd.to_datetime(row.get("end"), errors="coerce")
            if pd.isna(start) or pd.isna(end):
                continue
            days = (end - start).days
            if days < 250 or days > 390:
                continue
        try:
            candidates.append((filed, float(row["val"])))
        except (KeyError, TypeError, ValueError):
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def annual_series_before(companyfacts: dict[str, Any], tags: list[str], signal_date: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for row in fact_rows(companyfacts, tags):
        filed = pd.to_datetime(row.get("filed"), errors="coerce")
        start = pd.to_datetime(row.get("start"), errors="coerce")
        end = pd.to_datetime(row.get("end"), errors="coerce")
        if pd.isna(filed) or filed > signal_date or pd.isna(start) or pd.isna(end):
            continue
        days = (end - start).days
        if days < 250 or days > 390:
            continue
        try:
            rows.append({"filed": filed, "end": end, "val": float(row["val"])})
        except (KeyError, TypeError, ValueError):
            continue
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["end", "filed"]).drop_duplicates("end", keep="last")


def cagr(first: float | None, last: float | None, years: float) -> float | None:
    if first is None or last is None or first <= 0 or years <= 0:
        return None
    return (last / first) ** (1 / years) - 1


def build_ticker_cik_map(signal_dir: Path) -> dict[str, str]:
    eligible = pd.read_csv(signal_dir / "eligible_owner_purchases.csv")
    return (
        eligible.dropna(subset=["ticker", "cik"])
        .drop_duplicates("ticker")
        .set_index("ticker")["cik"]
        .astype(str)
        .to_dict()
    )


def overlay_context(returns: pd.DataFrame, ticker_cik: dict[str, str], cache_dir: Path) -> pd.DataFrame:
    returns = returns.copy().reset_index(drop=True)
    returns["context_row_id"] = range(len(returns))
    rows = []
    for trade in returns.itertuples(index=False):
        cik = ticker_cik.get(trade.ticker)
        if not cik:
            continue
        facts = load_companyfacts(cik, cache_dir, 0.12)
        signal_date = pd.Timestamp(trade.signal_datetime)
        shares = latest_fact_before(facts, TAGS["shares"], signal_date)
        revenue_series = annual_series_before(facts, TAGS["revenue"], signal_date)
        equity_series = annual_series_before(facts, TAGS["stockholders_equity"], signal_date)
        ocf = latest_fact_before(facts, TAGS["ocf"], signal_date, annualish=True)
        capex = latest_fact_before(facts, TAGS["capex"], signal_date, annualish=True)
        fcf = ocf - capex if ocf is not None and capex is not None else None
        market_cap = shares * trade.entry_price if shares is not None else None

        revenue_years = len(revenue_series)
        revenue_cagr_5y = None
        if revenue_years >= 5:
            tail = revenue_series.tail(5)
            revenue_cagr_5y = cagr(tail.iloc[0]["val"], tail.iloc[-1]["val"], max(len(tail) - 1, 1))
        equity_cagr_5y = None
        if len(equity_series) >= 5:
            tail = equity_series.tail(5)
            equity_cagr_5y = cagr(tail.iloc[0]["val"], tail.iloc[-1]["val"], max(len(tail) - 1, 1))

        rows.append(
            {
                "context_row_id": trade.context_row_id,
                "signal_id": trade.signal_id,
                "ticker": trade.ticker,
                "signal_datetime": trade.signal_datetime,
                "shares_outstanding": shares,
                "market_cap": market_cap,
                "market_cap_bucket": market_cap_bucket(market_cap),
                "purchase_to_market_cap": (
                    trade.signal_value / market_cap
                    if market_cap not in (None, 0) and pd.notna(trade.signal_value)
                    else None
                ),
                "filing_history_years": revenue_years,
                "revenue_cagr_5y": revenue_cagr_5y,
                "equity_cagr_5y": equity_cagr_5y,
                "fcf": fcf,
                "fcf_yield": fcf / market_cap if fcf is not None and market_cap else None,
                "long_history": revenue_years >= 5,
                "positive_revenue_cagr_5y": revenue_cagr_5y is not None and revenue_cagr_5y > 0,
                "positive_equity_cagr_5y": equity_cagr_5y is not None and equity_cagr_5y > 0,
            }
        )
    context = pd.DataFrame(rows)
    if context.empty:
        return returns.drop(columns=["context_row_id"])
    enriched = returns.merge(
        context.drop(columns=["signal_id", "ticker", "signal_datetime"]),
        on="context_row_id",
        how="left",
        validate="one_to_one",
    )
    return enriched.drop(columns=["context_row_id"])


def market_cap_bucket(market_cap: float | None) -> str | None:
    if market_cap is None or pd.isna(market_cap):
        return None
    if market_cap < 300_000_000:
        return "micro"
    if market_cap < 2_000_000_000:
        return "small"
    if market_cap < 10_000_000_000:
        return "mid"
    return "large"


def summarize(frame: pd.DataFrame, label: str) -> dict[str, object]:
    if frame.empty:
        return {"variant": label, "n": 0}
    return {
        "variant": label,
        "n": len(frame),
        "tickers": frame["ticker"].nunique(),
        "avg_return": frame["net_return"].mean(),
        "median_return": frame["net_return"].median(),
        "avg_excess": frame["excess_return"].mean(),
        "median_excess": frame["excess_return"].median(),
        "beat_rate": (frame["excess_return"] > 0).mean(),
        "avg_market_cap": frame["market_cap"].mean(),
        "avg_purchase_to_market_cap": frame["purchase_to_market_cap"].mean(),
    }


def pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value) * 100:.1f}%"


def money(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"${float(value):,.0f}"


def build_summary(enriched: pd.DataFrame) -> pd.DataFrame:
    p = enriched[enriched["signal_type"] == "purchase"].copy()
    p["signal_value_to_adv60"] = pd.to_numeric(p["signal_value_to_adv60"], errors="coerce")
    intensity_threshold = p.loc[
        (p["ticker"] != "TKO") & p["signal_value_to_adv60"].notna(),
        "signal_value_to_adv60",
    ].quantile(0.6)
    rows = [
        summarize(p, "purchase baseline"),
        summarize(p[p["ticker"] != "TKO"], "purchase ex-TKO"),
        summarize(p[p["market_cap_bucket"].isin(["mid", "large"])], "purchase mid/large cap"),
        summarize(p[(p["ticker"] != "TKO") & p["market_cap_bucket"].isin(["mid", "large"])], "purchase ex-TKO mid/large cap"),
        summarize(p[p["long_history"] == True], "purchase long filing history"),
        summarize(p[p["positive_revenue_cagr_5y"] == True], "purchase positive 5y revenue CAGR"),
        summarize(p[p["positive_equity_cagr_5y"] == True], "purchase positive 5y equity CAGR"),
        summarize(
            p[
                (p["ticker"] != "TKO")
                & p["market_cap_bucket"].isin(["mid", "large"])
                & (p["positive_revenue_cagr_5y"] == True)
            ],
            "purchase ex-TKO mid/large + positive revenue CAGR",
        ),
        summarize(
            p[
                (p["ticker"] != "TKO")
                & p["market_cap_bucket"].isin(["mid", "large"])
                & (p["positive_revenue_cagr_5y"] == True)
                & (p["signal_value_to_adv60"] >= intensity_threshold)
            ],
            "ex-TKO mid/large + positive revenue CAGR + top 40% ADV intensity",
        ),
    ]
    for bucket, group in p.groupby("market_cap_bucket", observed=True):
        rows.append(summarize(group, f"purchase market cap {bucket}"))
    return pd.DataFrame(rows)


def write_report(enriched: pd.DataFrame, summary: pd.DataFrame, output_dir: Path) -> None:
    lines = [
        "# Market Cap and Company History Context Backtest",
        "",
        "This overlays SEC companyfacts context on exploratory Yahoo return results. "
        "Market cap uses latest shares available before the signal multiplied by "
        "entry price. Long-history features use annual companyfacts available before "
        "the signal date.",
        "",
        "## Summary",
        "",
        "| Variant | N | Tickers | Avg Return | Median Return | Avg Excess | Median Excess | Beat Rate | Avg Mkt Cap | Avg Buy/MktCap |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"| {row.variant} | {row.n} | {getattr(row, 'tickers', 'n/a')} | "
            f"{pct(getattr(row, 'avg_return', None))} | {pct(getattr(row, 'median_return', None))} | "
            f"{pct(getattr(row, 'avg_excess', None))} | {pct(getattr(row, 'median_excess', None))} | "
            f"{pct(getattr(row, 'beat_rate', None))} | {money(getattr(row, 'avg_market_cap', None))} | "
            f"{pct(getattr(row, 'avg_purchase_to_market_cap', None))} |"
        )
    lines.extend(["", "## Best Context-Qualified Ex-TKO Mid/Large Signals", ""])
    subset = enriched[
        (enriched["signal_type"] == "purchase")
        & (enriched["ticker"] != "TKO")
        & enriched["market_cap_bucket"].isin(["mid", "large"])
        & (enriched["positive_revenue_cagr_5y"] == True)
    ].copy()
    lines.extend(["| Ticker | Signal | Mkt Cap | Buy/MktCap | Revenue CAGR | Net Return | Excess |", "| --- | --- | ---: | ---: | ---: | ---: | ---: |"])
    for row in subset.sort_values("excess_return", ascending=False).head(20).itertuples(index=False):
        lines.append(
            f"| {row.ticker} | {pd.Timestamp(row.signal_datetime).date()} | {money(row.market_cap)} | "
            f"{pct(row.purchase_to_market_cap)} | {pct(row.revenue_cagr_5y)} | "
            f"{pct(row.net_return)} | {pct(row.excess_return)} |"
        )
    lines.extend(["", "## Worst Context-Qualified Ex-TKO Mid/Large Signals", ""])
    lines.extend(["| Ticker | Signal | Mkt Cap | Buy/MktCap | Revenue CAGR | Net Return | Excess |", "| --- | --- | ---: | ---: | ---: | ---: | ---: |"])
    for row in subset.sort_values("excess_return").head(20).itertuples(index=False):
        lines.append(
            f"| {row.ticker} | {pd.Timestamp(row.signal_datetime).date()} | {money(row.market_cap)} | "
            f"{pct(row.purchase_to_market_cap)} | {pct(row.revenue_cagr_5y)} | "
            f"{pct(row.net_return)} | {pct(row.excess_return)} |"
        )
    (output_dir / "context_backtest_report.md").write_text("\n".join(lines), encoding="utf-8")


def run(signal_dir: Path, returns_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    returns = pd.read_csv(returns_dir / "signal_returns.csv", parse_dates=["signal_datetime"])
    ticker_cik = build_ticker_cik_map(signal_dir)
    enriched = overlay_context(returns, ticker_cik, output_dir / "companyfacts_cache")
    summary = build_summary(enriched)
    enriched.to_csv(output_dir / "context_enriched_returns.csv", index=False)
    summary.to_csv(output_dir / "context_summary.csv", index=False)
    write_report(enriched, summary, output_dir)
    print((output_dir / "context_backtest_report.md").read_text())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest market-cap/company-history context filters.")
    parser.add_argument("--signal-dir", type=Path, default=Path("data/results/sec_signals_5y_top25"))
    parser.add_argument("--returns-dir", type=Path, default=Path("data/results/sec_signals_5y_top25_returns"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/context_backtest"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.signal_dir, args.returns_dir, args.output_dir)
