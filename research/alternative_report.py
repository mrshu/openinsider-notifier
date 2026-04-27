from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path

import pandas as pd


def pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value * 100:.1f}%"


def summarize(frame: pd.DataFrame, label: str, return_col: str = "return") -> dict[str, object]:
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


def table_md(rows: list[dict[str, object]]) -> list[str]:
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


def openinsider_tables(results_dir: Path) -> tuple[list[str], pd.DataFrame]:
    base = pd.read_csv(results_dir / "openinsider_3y" / "event_returns.csv")
    hold378 = pd.read_csv(results_dir / "openinsider_3y_hold378" / "event_returns.csv")
    hold504 = pd.read_csv(results_dir / "openinsider_3y_hold504" / "event_returns.csv")

    all_purchases = base[base["strategy"] == "all_purchases"].copy()
    cluster = base[base["strategy"] == "cluster"].copy()

    rows = [
        summarize(all_purchases, "all purchases >= $100k"),
        summarize(cluster, "cluster only"),
        summarize(hold378[hold378["strategy"] == "cluster"], "cluster 18-month hold"),
        summarize(hold504[hold504["strategy"] == "cluster"], "cluster 2-year hold"),
    ]
    for threshold in [500_000, 1_000_000, 2_000_000, 5_000_000]:
        rows.append(
            summarize(
                all_purchases[all_purchases["cluster_value"] >= threshold],
                f"all purchases >= ${threshold / 1_000_000:g}m",
            )
        )

    all_purchases["value_bucket"] = pd.cut(
        all_purchases["cluster_value"],
        [0, 200_000, 500_000, 1_000_000, 2_000_000, float("inf")],
        labels=["100-200k", "200-500k", "500k-1m", "1m-2m", "2m+"],
    )
    for bucket, group in all_purchases.groupby("value_bucket", observed=True):
        rows.append(summarize(group, f"value bucket {bucket}"))

    all_purchases["drawdown_bucket"] = pd.cut(
        all_purchases["drawdown_52w"],
        [-1, -0.6, -0.45, -0.3, -0.15, 0.25],
        labels=["<-60%", "-60:-45%", "-45:-30%", "-30:-15%", ">-15%"],
    )
    for bucket, group in all_purchases.groupby("drawdown_bucket", observed=True):
        rows.append(summarize(group, f"52w drawdown {bucket}"))

    output = pd.DataFrame(rows)
    lines = ["## OpenInsider/Yahoo Diagnostic Tables", ""]
    lines.extend(table_md(rows))
    lines.append("")
    return lines, output


def parse_owner_blob(value: object) -> list[dict[str, object]]:
    if pd.isna(value):
        return []
    text = str(value)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            return []
    return parsed if isinstance(parsed, list) else []


def title_has_high_info_role(owners: list[dict[str, object]]) -> bool:
    needles = ("CEO", "CHIEF EXECUTIVE", "CFO", "CHIEF FINANCIAL", "CHAIR", "PRESIDENT")
    for owner in owners:
        title = str(owner.get("officer_title") or "").upper()
        if any(needle in title for needle in needles):
            return True
    return False


def owners_have_ten_percent(owners: list[dict[str, object]]) -> bool:
    return any(bool(owner.get("is_ten_percent_owner")) for owner in owners)


def sec_tables(results_dir: Path) -> tuple[list[str], pd.DataFrame]:
    sec_dir = results_dir / "sec_form4_targeted_cap250_exploratory"
    event = pd.read_csv(sec_dir / "event_returns.csv", parse_dates=["signal_datetime"])
    eligible = pd.read_csv(sec_dir / "eligible_owner_purchases.csv", parse_dates=["filing_datetime"])

    if event.empty:
        return ["## SEC XML Targeted Diagnostic Tables", "", "No completed SEC trades."], pd.DataFrame()

    event["entry_premium_to_vwap"] = event["entry_price"] / event["insider_vwap"] - 1
    rows = [
        summarize(event, "SEC targeted clusters", return_col="net_return"),
    ]
    for threshold in [250_000, 500_000, 1_000_000, 2_000_000, 5_000_000]:
        rows.append(
            summarize(
                event[event["cluster_value"] >= threshold],
                f"SEC cluster value >= ${threshold / 1_000_000:g}m",
                return_col="net_return",
            )
        )

    event["vwap_bucket"] = pd.cut(
        event["entry_premium_to_vwap"],
        [-1, -0.05, 0, 0.05, 0.15, float("inf")],
        labels=["<-5%", "-5:0%", "0:5%", "5:15%", ">15%"],
    )
    for bucket, group in event.groupby("vwap_bucket", observed=True):
        rows.append(summarize(group, f"entry premium to insider VWAP {bucket}", return_col="net_return"))

    role_rows = []
    for trade in event.itertuples(index=False):
        window = eligible[
            (eligible["ticker"] == trade.ticker)
            & (eligible["filing_datetime"] >= pd.Timestamp(trade.cluster_start_datetime))
            & (eligible["filing_datetime"] <= trade.signal_datetime)
        ].copy()
        owners = []
        for value in window["reporting_owners"]:
            owners.extend(parse_owner_blob(value))
        role_rows.append(
            {
                "ticker": trade.ticker,
                "signal_datetime": trade.signal_datetime,
                "high_info_role": title_has_high_info_role(owners),
                "has_ten_percent_owner": owners_have_ten_percent(owners),
            }
        )
    roles = pd.DataFrame(role_rows)
    enriched = event.merge(roles, on=["ticker", "signal_datetime"], how="left")
    rows.append(summarize(enriched[enriched["high_info_role"]], "SEC clusters with CEO/CFO/Chair/President", return_col="net_return"))
    rows.append(summarize(enriched[~enriched["high_info_role"]], "SEC clusters without high-info role", return_col="net_return"))
    rows.append(summarize(enriched[enriched["has_ten_percent_owner"]], "SEC clusters with 10% owner also eligible", return_col="net_return"))
    rows.append(summarize(enriched[~enriched["has_ten_percent_owner"]], "SEC clusters without 10% owner", return_col="net_return"))

    output = pd.DataFrame(rows)
    lines = ["## SEC XML Targeted Diagnostic Tables", ""]
    lines.extend(table_md(rows))
    lines.append("")
    lines.extend(["### SEC Completed Trades", ""])
    lines.extend(
        [
            "| Ticker | Signal | Cluster Value | Entry/VWAP | Net Return | SPY Return | Excess |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for trade in event.sort_values("excess_return", ascending=False).itertuples(index=False):
        lines.append(
            f"| {trade.ticker} | {trade.signal_datetime.date()} | "
            f"${trade.cluster_value:,.0f} | {trade.entry_premium_to_vwap:.1%} | "
            f"{trade.net_return:.1%} | {trade.benchmark_return:.1%} | "
            f"{trade.excess_return:.1%} |"
        )
    lines.append("")
    return lines, output


def write_report(results_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    oi_lines, oi_table = openinsider_tables(results_dir)
    sec_lines, sec_table = sec_tables(results_dir)

    lines = [
        "# Alternative Signal Diagnostics",
        "",
        "This report tests salvage hypotheses on existing exploratory artifacts. "
        "It is not a production-valid backtest because the free-data runs use "
        "Yahoo prices and current/screen-derived universes.",
        "",
        "## Executive Readout",
        "",
        "- Raw clusters do not work as a standalone 366-day/1-year signal.",
        "- Larger insider purchase value is the clearest positive hint.",
        "- The SEC targeted sample has positive median trade return but negative excess return.",
        "- Entry near or below insider VWAP does not rescue the tested SEC sample by itself.",
        "- The most defensible next combination remains cluster + FCF/value + share-count reduction.",
        "",
    ]
    lines.extend(oi_lines)
    lines.extend(sec_lines)
    lines.extend(
        [
            "## What Looks Worth Pursuing",
            "",
            "1. `cluster + large normalized purchase size`: strongest observed monotonic hint.",
            "2. `cluster + FCF/value/no-distress`: best economic explanation for filtering losers.",
            "3. `cluster + shareholder yield/share-count reduction`: strongest capital-allocation thesis.",
            "4. `monthly top-N ranking sleeve`: better research frame than buying every cluster.",
            "5. `sector/size-relative benchmarks`: required before judging excess return fairly.",
            "",
            "## What Does Not Look Promising",
            "",
            "- Longer fixed holds: 18-month and 2-year holds worsened versus SPY.",
            "- Cluster-only entry: both OpenInsider and SEC targeted samples underperformed.",
            "- Deep drawdown alone: catches falling knives as often as overreactions.",
            "",
        ]
    )
    (output_dir / "alternative_signal_report.md").write_text("\n".join(lines), encoding="utf-8")
    oi_table.to_csv(output_dir / "openinsider_alternative_diagnostics.csv", index=False)
    sec_table.to_csv(output_dir / "sec_alternative_diagnostics.csv", index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate alternative signal diagnostics.")
    parser.add_argument("--results-dir", type=Path, default=Path("data/results"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/alternative_diagnostics"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    write_report(args.results_dir, args.output_dir)
