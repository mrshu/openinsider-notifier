from __future__ import annotations

import argparse
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
            "avg_cluster_value": None,
        }
    return {
        "variant": label,
        "n": len(frame),
        "avg_return": frame[return_col].mean(),
        "median_return": frame[return_col].median(),
        "avg_excess": frame["excess_return"].mean(),
        "median_excess": frame["excess_return"].median(),
        "beat_rate": (frame["excess_return"] > 0).mean(),
        "avg_cluster_value": frame["cluster_value"].mean(),
    }


def markdown_table(rows: list[dict[str, object]]) -> list[str]:
    lines = [
        "| Variant | N | Avg Return | Median Return | Avg Excess | Median Excess | Beat Rate | Avg Value |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        avg_value = row["avg_cluster_value"]
        value_text = "n/a" if avg_value is None or pd.isna(avg_value) else f"${avg_value:,.0f}"
        lines.append(
            f"| {row['variant']} | {row['n']} | {pct(row['avg_return'])} | "
            f"{pct(row['median_return'])} | {pct(row['avg_excess'])} | "
            f"{pct(row['median_excess'])} | {pct(row['beat_rate'])} | {value_text} |"
        )
    return lines


def build_openinsider_episode_features(results_dir: Path) -> pd.DataFrame:
    event = pd.read_csv(results_dir / "openinsider_3y" / "event_returns.csv", parse_dates=["signal_datetime"])
    all_purchases = event[event["strategy"] == "all_purchases"].copy()
    clusters = event[event["strategy"] == "cluster"].copy()

    cluster_keys = set(zip(clusters["ticker"], clusters["signal_datetime"].astype(str)))
    all_purchases["is_cluster_signal"] = [
        (ticker, str(signal_datetime)) in cluster_keys
        for ticker, signal_datetime in zip(all_purchases["ticker"], all_purchases["signal_datetime"])
    ]
    all_purchases["signal_month"] = all_purchases["signal_datetime"].dt.to_period("M").astype(str)
    all_purchases["purchase_value_rank"] = all_purchases["cluster_value"].rank(pct=True)
    all_purchases["value_bucket"] = pd.qcut(
        all_purchases["cluster_value"], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"], duplicates="drop"
    )
    all_purchases["large_1m"] = all_purchases["cluster_value"] >= 1_000_000
    all_purchases["large_2m"] = all_purchases["cluster_value"] >= 2_000_000
    all_purchases["large_5m"] = all_purchases["cluster_value"] >= 5_000_000
    all_purchases["avoid_extreme_drawdown"] = all_purchases["drawdown_52w"] > -0.60
    all_purchases["moderate_or_less_drawdown"] = all_purchases["drawdown_52w"] > -0.50
    return all_purchases


def openinsider_intensity_report(features: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    rows = [
        summarize(features, "all purchase episodes"),
        summarize(features[features["is_cluster_signal"]], "cluster episodes only"),
        summarize(features[~features["is_cluster_signal"]], "single/non-cluster purchase episodes"),
    ]
    for bucket, group in features.groupby("value_bucket", observed=True):
        rows.append(summarize(group, f"purchase value quintile {bucket}"))

    for threshold in [1_000_000, 2_000_000, 5_000_000]:
        rows.append(summarize(features[features["cluster_value"] >= threshold], f"purchase value >= ${threshold / 1_000_000:g}m"))
        rows.append(
            summarize(
                features[(features["cluster_value"] >= threshold) & features["is_cluster_signal"]],
                f"cluster + value >= ${threshold / 1_000_000:g}m",
            )
        )

    rows.append(
        summarize(
            features[features["large_2m"] & features["avoid_extreme_drawdown"]],
            "value >= $2m + avoid >60% drawdown",
        )
    )
    rows.append(
        summarize(
            features[features["large_2m"] & features["moderate_or_less_drawdown"]],
            "value >= $2m + avoid >50% drawdown",
        )
    )

    ranked = []
    for n in [5, 10, 20]:
        ranked.append(
            features.sort_values(["signal_month", "cluster_value"], ascending=[True, False])
            .groupby("signal_month", as_index=False)
            .head(n)
        )
        rows.append(summarize(ranked[-1], f"monthly top {n} by purchase value"))

    table = pd.DataFrame(rows)
    lines = ["## OpenInsider Purchase-Intensity Tests", ""]
    lines.extend(markdown_table(rows))
    lines.append("")
    return table, lines


def sec_intensity_report(results_dir: Path) -> tuple[pd.DataFrame, list[str]]:
    path = results_dir / "sec_form4_targeted_cap250_exploratory" / "event_returns.csv"
    if not path.exists():
        return pd.DataFrame(), ["## SEC Purchase-Intensity Tests", "", "No SEC event returns found.", ""]

    sec = pd.read_csv(path, parse_dates=["signal_datetime"])
    if sec.empty:
        return pd.DataFrame(), ["## SEC Purchase-Intensity Tests", "", "No SEC completed trades found.", ""]
    sec["return"] = sec["net_return"]
    sec["entry_premium_to_vwap"] = sec["entry_price"] / sec["insider_vwap"] - 1
    sec["value_bucket"] = pd.qcut(
        sec["cluster_value"], 3, labels=["low", "mid", "high"], duplicates="drop"
    )
    rows = [summarize(sec, "SEC targeted cluster baseline")]
    for bucket, group in sec.groupby("value_bucket", observed=True):
        rows.append(summarize(group, f"SEC cluster value tercile {bucket}"))
    rows.append(summarize(sec[sec["cluster_value"] >= 1_000_000], "SEC cluster value >= $1m"))
    rows.append(summarize(sec[sec["cluster_value"] >= 2_000_000], "SEC cluster value >= $2m"))
    rows.append(summarize(sec[sec["entry_premium_to_vwap"] <= 0], "SEC entry <= insider VWAP"))
    rows.append(summarize(sec[(sec["cluster_value"] >= 2_000_000) & (sec["entry_premium_to_vwap"] <= 0)], "SEC >= $2m + entry <= VWAP"))

    table = pd.DataFrame(rows)
    lines = ["## SEC Purchase-Intensity Tests", ""]
    lines.extend(markdown_table(rows))
    lines.append("")
    return table, lines


def write_report(results_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    features = build_openinsider_episode_features(results_dir)
    oi_table, oi_lines = openinsider_intensity_report(features)
    sec_table, sec_lines = sec_intensity_report(results_dir)

    features.to_csv(output_dir / "insider_purchase_episodes.csv", index=False)
    oi_table.to_csv(output_dir / "intensity_bucket_report.csv", index=False)
    sec_table.to_csv(output_dir / "sec_intensity_bucket_report.csv", index=False)

    lines = [
        "# Purchase-Intensity Research Report",
        "",
        "This report tests whether large insider purchase intensity is a better "
        "primary signal than cluster status. It uses existing exploratory "
        "OpenInsider/Yahoo and SEC-targeted artifacts, so results are not "
        "production-valid.",
        "",
        "## Executive Readout",
        "",
        "- Purchase value has the clearest monotonic relationship in the broad sample.",
        "- Cluster status does not add obvious value after purchase size in the current data.",
        "- Monthly top-N by raw purchase value does not beat the simple high-threshold buckets.",
        "- The next valid test should normalize purchase size by market cap and ADV.",
        "",
    ]
    lines.extend(oi_lines)
    lines.extend(sec_lines)
    lines.extend(
        [
            "## Proposed Promotion Rule",
            "",
            "Promote purchase intensity to the next research stage only if the full SEC run shows:",
            "",
            "- top intensity bucket has positive median sector/size-adjusted excess return;",
            "- top bucket beats bottom bucket by at least 5 percentage points median excess;",
            "- at least 100 completed trades in the top bucket;",
            "- result survives excluding top 5 winners and all microcaps.",
            "",
        ]
    )
    (output_dir / "purchase_intensity_report.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build purchase-intensity diagnostics.")
    parser.add_argument("--results-dir", type=Path, default=Path("data/results"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/intensity"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    write_report(args.results_dir, args.output_dir)
