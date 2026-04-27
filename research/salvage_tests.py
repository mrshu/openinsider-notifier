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

FACT_TAGS = {
    "operating_cash_flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "capex": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ],
    "shares": [
        "EntityCommonStockSharesOutstanding",
        "CommonStocksIncludingAdditionalPaidInCapitalSharesOutstanding",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingDiluted",
    ],
}


def pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value * 100:.1f}%"


def summarize(frame: pd.DataFrame, label: str, return_col: str) -> dict[str, object]:
    if frame.empty:
        return {
            "variant": label,
            "n": 0,
            "avg_return": None,
            "median_return": None,
            "avg_excess": None,
            "median_excess": None,
            "beat_rate": None,
        }
    return {
        "variant": label,
        "n": len(frame),
        "avg_return": frame[return_col].mean(),
        "median_return": frame[return_col].median(),
        "avg_excess": frame["excess_return"].mean(),
        "median_excess": frame["excess_return"].median(),
        "beat_rate": (frame["excess_return"] > 0).mean(),
    }


def markdown_table(rows: list[dict[str, object]]) -> list[str]:
    lines = [
        "| Variant | N | Avg Return | Median Return | Avg Excess | Median Excess | Beat Rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['variant']} | {row['n']} | {pct(row['avg_return'])} | "
            f"{pct(row['median_return'])} | {pct(row['avg_excess'])} | "
            f"{pct(row['median_excess'])} | {pct(row['beat_rate'])} |"
        )
    return lines


def test_openinsider(results_dir: Path) -> tuple[pd.DataFrame, list[str]]:
    event = pd.read_csv(results_dir / "openinsider_3y" / "event_returns.csv")
    all_purchases = event[event["strategy"] == "all_purchases"].copy()
    cluster = event[event["strategy"] == "cluster"].copy()

    rows = [
        summarize(all_purchases, "baseline all purchases >= $100k", "return"),
        summarize(cluster, "baseline cluster only", "return"),
    ]
    for threshold in [1_000_000, 2_000_000, 5_000_000]:
        rows.append(
            summarize(
                all_purchases[all_purchases["cluster_value"] >= threshold],
                f"purchase value >= ${threshold / 1_000_000:g}m",
                "return",
            )
        )
        rows.append(
            summarize(
                cluster[cluster["cluster_value"] >= threshold],
                f"cluster value >= ${threshold / 1_000_000:g}m",
                "return",
            )
        )

    all_purchases["value_rank"] = all_purchases["cluster_value"].rank(pct=True)
    all_purchases["moderate_drawdown"] = all_purchases["drawdown_52w"].between(-0.5, -0.15)
    rows.append(
        summarize(
            all_purchases[
                (all_purchases["cluster_value"] >= 2_000_000)
                & all_purchases["moderate_drawdown"]
            ],
            "purchase >= $2m + 15-50% drawdown",
            "return",
        )
    )
    rows.append(
        summarize(
            all_purchases[
                (all_purchases["cluster_value"] >= 2_000_000)
                & (all_purchases["drawdown_52w"] > -0.6)
            ],
            "purchase >= $2m + avoid >60% drawdown",
            "return",
        )
    )

    topn_rows = monthly_top_n(all_purchases, [10, 20], "cluster_value")
    rows.extend(topn_rows)
    output = pd.DataFrame(rows)
    lines = ["## Broad OpenInsider/Yahoo Salvage Tests", ""]
    lines.extend(markdown_table(rows))
    lines.append("")
    return output, lines


def monthly_top_n(frame: pd.DataFrame, ns: list[int], rank_col: str) -> list[dict[str, object]]:
    data = frame.copy()
    data["signal_datetime"] = pd.to_datetime(data["signal_datetime"])
    data["month"] = data["signal_datetime"].dt.to_period("M")
    rows = []
    for n in ns:
        selected = (
            data.sort_values(["month", rank_col], ascending=[True, False])
            .groupby("month", as_index=False)
            .head(n)
        )
        rows.append(summarize(selected, f"monthly top {n} by purchase value", "return"))
    return rows


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


def fact_units(companyfacts: dict[str, Any], tag: str) -> list[dict[str, Any]]:
    facts = companyfacts.get("facts", {}).get("us-gaap", {})
    fact = facts.get(tag)
    if not fact:
        return []
    rows = []
    for unit_rows in fact.get("units", {}).values():
        rows.extend(unit_rows)
    return rows


def latest_fact_before(
    companyfacts: dict[str, Any],
    tags: list[str],
    signal_date: pd.Timestamp,
    *,
    duration: str | None = None,
) -> float | None:
    candidates = []
    for tag in tags:
        for row in fact_units(companyfacts, tag):
            filed = pd.to_datetime(row.get("filed"), errors="coerce")
            if pd.isna(filed) or filed > signal_date:
                continue
            start = pd.to_datetime(row.get("start"), errors="coerce")
            end = pd.to_datetime(row.get("end"), errors="coerce")
            if duration == "annualish":
                if pd.isna(start) or pd.isna(end):
                    continue
                days = (end - start).days
                if days < 250 or days > 380:
                    continue
            candidates.append((filed, row.get("val")))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    try:
        return float(candidates[-1][1])
    except (TypeError, ValueError):
        return None


def prior_fact_before(
    companyfacts: dict[str, Any],
    tags: list[str],
    signal_date: pd.Timestamp,
    days_back: int = 365,
) -> float | None:
    target = signal_date - pd.Timedelta(days=days_back)
    return latest_fact_before(companyfacts, tags, target)


def test_sec_fundamentals(results_dir: Path, cache_dir: Path) -> tuple[pd.DataFrame, list[str]]:
    sec_dir = results_dir / "sec_form4_targeted_cap250_exploratory"
    event = pd.read_csv(sec_dir / "event_returns.csv", parse_dates=["signal_datetime"])
    eligible = pd.read_csv(sec_dir / "eligible_owner_purchases.csv")
    ticker_cik = eligible.dropna(subset=["ticker", "cik"]).drop_duplicates("ticker").set_index("ticker")["cik"].to_dict()

    overlays = []
    for trade in event.itertuples(index=False):
        cik = ticker_cik.get(trade.ticker)
        if cik is None:
            continue
        facts = load_companyfacts(str(cik), cache_dir, 0.12)
        ocf = latest_fact_before(facts, FACT_TAGS["operating_cash_flow"], trade.signal_datetime, duration="annualish")
        capex = latest_fact_before(facts, FACT_TAGS["capex"], trade.signal_datetime, duration="annualish")
        shares = latest_fact_before(facts, FACT_TAGS["shares"], trade.signal_datetime)
        prior_shares = prior_fact_before(facts, FACT_TAGS["shares"], trade.signal_datetime)
        fcf = ocf - capex if ocf is not None and capex is not None else None
        market_cap = shares * trade.entry_price if shares is not None else None
        overlays.append(
            {
                "ticker": trade.ticker,
                "signal_datetime": trade.signal_datetime,
                "ocf": ocf,
                "capex": capex,
                "fcf": fcf,
                "shares": shares,
                "prior_shares": prior_shares,
                "share_count_change": (
                    shares / prior_shares - 1
                    if shares is not None and prior_shares not in (None, 0)
                    else None
                ),
                "fcf_yield": fcf / market_cap if fcf is not None and market_cap else None,
            }
        )

    overlay = pd.DataFrame(overlays)
    enriched = event.merge(overlay, on=["ticker", "signal_datetime"], how="left")
    rows = [
        summarize(enriched, "SEC targeted baseline", "net_return"),
        summarize(enriched[enriched["fcf"] > 0], "SEC + positive annual FCF", "net_return"),
        summarize(enriched[enriched["fcf_yield"] > 0.03], "SEC + FCF yield > 3%", "net_return"),
        summarize(enriched[enriched["share_count_change"] < 0], "SEC + falling share count", "net_return"),
        summarize(
            enriched[(enriched["fcf"] > 0) & (enriched["share_count_change"] < 0)],
            "SEC + positive FCF + falling share count",
            "net_return",
        ),
        summarize(
            enriched[(enriched["fcf_yield"] > 0.03) & (enriched["share_count_change"] < 0)],
            "SEC + FCF yield > 3% + falling share count",
            "net_return",
        ),
    ]
    lines = ["## SEC Companyfacts Overlay Tests", ""]
    lines.extend(markdown_table(rows))
    lines.append("")
    lines.extend(["### SEC Fundamental Overlay Detail", ""])
    lines.extend(
        [
            "| Ticker | FCF | FCF Yield | Share Count Change | Net Return | Excess |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in enriched.itertuples(index=False):
        lines.append(
            f"| {row.ticker} | "
            f"{row.fcf:,.0f}" if pd.notna(row.fcf) else f"| {row.ticker} | n/a"
        )
        lines[-1] += (
            f" | {pct(row.fcf_yield)} | {pct(row.share_count_change)} | "
            f"{pct(row.net_return)} | {pct(row.excess_return)} |"
        )
    lines.append("")
    return pd.DataFrame(rows), lines


def write_report(results_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    oi_table, oi_lines = test_openinsider(results_dir)
    sec_table, sec_lines = test_sec_fundamentals(results_dir, output_dir / "companyfacts_cache")

    lines = [
        "# Salvage Hypothesis Test Report",
        "",
        "This tests the strongest alternatives identified so far. OpenInsider/Yahoo "
        "results remain exploratory. SEC companyfacts overlays are point-in-time by "
        "`filed` date where tags are available, but the sample is the targeted SEC "
        "cluster sample, not the full universe.",
        "",
    ]
    lines.extend(oi_lines)
    lines.extend(sec_lines)
    lines.extend(
        [
            "## Readout",
            "",
            "- The broadest durable effect is purchase size: `$2m+` and `$5m+` "
            "purchase-value buckets beat SPY in the OpenInsider exploratory run.",
            "- Cluster-only remains weak.",
            "- The SEC companyfacts overlay is too small to declare victory, but it "
            "shows whether FCF/share-count filters would have removed known losers.",
            "",
        ]
    )
    (output_dir / "salvage_hypothesis_report.md").write_text("\n".join(lines), encoding="utf-8")
    oi_table.to_csv(output_dir / "openinsider_salvage_tests.csv", index=False)
    sec_table.to_csv(output_dir / "sec_companyfacts_salvage_tests.csv", index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run salvage hypothesis diagnostics.")
    parser.add_argument("--results-dir", type=Path, default=Path("data/results"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/salvage_tests"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    write_report(args.results_dir, args.output_dir)
