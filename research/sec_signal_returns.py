from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import yfinance as yf


@dataclass(frozen=True)
class Config:
    signal_dir: Path
    output_dir: Path
    benchmark: str
    hold_calendar_days: int
    cost_bps_each_side: float


def download_prices(tickers: list[str], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    data = yf.download(
        sorted(set(tickers)),
        start=start.date().isoformat(),
        end=end.date().isoformat(),
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    if data.empty:
        raise ValueError("Yahoo returned no price data")
    data.index = pd.to_datetime(data.index).tz_localize(None)
    return data


def price_panel(data: pd.DataFrame, field: str) -> pd.DataFrame:
    if isinstance(data.columns, pd.MultiIndex):
        return data[field].copy()
    if field in data.columns:
        # Single ticker fallback; caller should normally request multiple tickers.
        return data[[field]]
    raise ValueError(f"Missing price field: {field}")


def first_after(index: pd.DatetimeIndex, timestamp: pd.Timestamp) -> int | None:
    position = index.searchsorted(timestamp.normalize(), side="right")
    return None if position >= len(index) else int(position)


def first_on_or_after(index: pd.DatetimeIndex, timestamp: pd.Timestamp) -> int | None:
    position = index.searchsorted(timestamp.normalize(), side="left")
    return None if position >= len(index) else int(position)


def load_purchase_signals(signal_dir: Path) -> pd.DataFrame:
    path = signal_dir / "eligible_owner_purchases.csv"
    purchases = pd.read_csv(path, parse_dates=["filing_datetime"])
    purchases = purchases.rename(
        columns={
            "filing_datetime": "signal_datetime",
            "purchase_value": "signal_value",
            "price_per_share": "insider_vwap",
        }
    )
    purchases["signal_type"] = "purchase"
    purchases["signal_id"] = (
        purchases["ticker"].astype(str)
        + "|"
        + purchases["accession"].astype(str)
        + "|"
        + purchases["insider_name"].astype(str)
        + "|"
        + purchases["signal_datetime"].astype(str)
    )
    return purchases[
        [
            "signal_id",
            "signal_type",
            "ticker",
            "company_name",
            "signal_datetime",
            "insider_name",
            "shares",
            "signal_value",
            "insider_vwap",
        ]
    ].copy()


def load_cluster_signals(signal_dir: Path) -> pd.DataFrame:
    path = signal_dir / "cluster_signals.csv"
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    clusters = pd.read_csv(path, parse_dates=["signal_datetime"])
    if clusters.empty:
        return clusters
    clusters["signal_type"] = "cluster"
    clusters["signal_id"] = (
        clusters["ticker"].astype(str)
        + "|cluster|"
        + clusters["signal_datetime"].astype(str)
    )
    clusters = clusters.rename(
        columns={
            "cluster_value": "signal_value",
            "cluster_shares": "shares",
        }
    )
    clusters["insider_name"] = clusters.get("insiders", "")
    return clusters[
        [
            "signal_id",
            "signal_type",
            "ticker",
            "company_name",
            "signal_datetime",
            "insider_name",
            "shares",
            "signal_value",
            "insider_vwap",
            "insider_count",
            "event_count",
        ]
    ].copy()


def attach_returns(signals: pd.DataFrame, prices: pd.DataFrame, config: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    opens = price_panel(prices, "Open")
    closes = price_panel(prices, "Close")
    volumes = price_panel(prices, "Volume")
    benchmark = config.benchmark.upper()
    rows = []
    diagnostics = []
    cost_multiplier = (1 - config.cost_bps_each_side / 10_000) ** 2

    for signal in signals.itertuples(index=False):
        ticker = str(signal.ticker).upper()
        if ticker not in opens.columns or ticker not in closes.columns:
            diagnostics.append({"signal_id": signal.signal_id, "ticker": ticker, "reason": "missing_price_column"})
            continue
        if benchmark not in closes.columns:
            diagnostics.append({"signal_id": signal.signal_id, "ticker": ticker, "reason": "missing_benchmark_column"})
            continue

        open_series = opens[ticker].dropna()
        close_series = closes[ticker].dropna()
        benchmark_series = closes[benchmark].dropna()
        entry_pos = first_after(open_series.index, signal.signal_datetime)
        if entry_pos is None:
            diagnostics.append({"signal_id": signal.signal_id, "ticker": ticker, "reason": "missing_entry_price"})
            continue
        entry_date = open_series.index[entry_pos]
        exit_target = entry_date + pd.Timedelta(days=config.hold_calendar_days)
        exit_pos = first_on_or_after(close_series.index, exit_target)
        if exit_pos is None:
            diagnostics.append({"signal_id": signal.signal_id, "ticker": ticker, "reason": "missing_exit_price"})
            continue

        exit_date = close_series.index[exit_pos]
        bench_entry_pos = first_on_or_after(benchmark_series.index, entry_date)
        bench_exit_pos = first_on_or_after(benchmark_series.index, exit_date)
        if bench_entry_pos is None or bench_exit_pos is None:
            diagnostics.append({"signal_id": signal.signal_id, "ticker": ticker, "reason": "missing_benchmark_price"})
            continue

        entry_price = float(open_series.iloc[entry_pos])
        dollar_volume = (closes[ticker] * volumes[ticker]).dropna()
        adv60_window = dollar_volume[dollar_volume.index < entry_date].tail(60)
        adv20_window = dollar_volume[dollar_volume.index < entry_date].tail(20)
        adv60 = float(adv60_window.mean()) if len(adv60_window) >= 20 else pd.NA
        adv20 = float(adv20_window.mean()) if len(adv20_window) >= 10 else pd.NA
        exit_price = float(close_series.iloc[exit_pos])
        gross_return = exit_price / entry_price - 1
        net_return = (1 + gross_return) * cost_multiplier - 1
        benchmark_return = benchmark_series.iloc[bench_exit_pos] / benchmark_series.iloc[bench_entry_pos] - 1
        entry_premium_to_vwap = (
            entry_price / float(signal.insider_vwap) - 1
            if pd.notna(signal.insider_vwap) and float(signal.insider_vwap) != 0
            else pd.NA
        )
        rows.append(
            {
                **signal._asdict(),
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "adv20_dollars": adv20,
                "adv60_dollars": adv60,
                "signal_value_to_adv60": (
                    float(signal.signal_value) / adv60
                    if pd.notna(adv60) and adv60 != 0 and pd.notna(signal.signal_value)
                    else pd.NA
                ),
                "gross_return": gross_return,
                "net_return": net_return,
                "benchmark_return": benchmark_return,
                "excess_return": net_return - benchmark_return,
                "holding_days": (exit_date - entry_date).days,
                "entry_premium_to_vwap": entry_premium_to_vwap,
            }
        )

    return pd.DataFrame(rows), pd.DataFrame(diagnostics)


def summarize(frame: pd.DataFrame, label: str) -> dict[str, object]:
    if frame.empty:
        return {
            "variant": label,
            "n": 0,
            "avg_return": pd.NA,
            "median_return": pd.NA,
            "avg_excess": pd.NA,
            "median_excess": pd.NA,
            "beat_rate": pd.NA,
            "avg_value": pd.NA,
        }
    return {
        "variant": label,
        "n": len(frame),
        "avg_return": frame["net_return"].mean(),
        "median_return": frame["net_return"].median(),
        "avg_excess": frame["excess_return"].mean(),
        "median_excess": frame["excess_return"].median(),
        "beat_rate": (frame["excess_return"] > 0).mean(),
        "avg_value": frame["signal_value"].mean(),
    }


def build_summary(returns: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for signal_type, group in returns.groupby("signal_type"):
        rows.append(summarize(group, f"{signal_type} baseline"))
        for threshold in [1_000_000, 2_000_000, 5_000_000, 10_000_000]:
            rows.append(
                summarize(
                    group[group["signal_value"] >= threshold],
                    f"{signal_type} value >= ${threshold / 1_000_000:g}m",
                )
            )
        if signal_type == "purchase":
            group = group.copy()
            group["value_quintile"] = pd.qcut(
                group["signal_value"],
                5,
                labels=["Q1", "Q2", "Q3", "Q4", "Q5"],
                duplicates="drop",
            )
            for bucket, bucket_group in group.groupby("value_quintile", observed=True):
                rows.append(summarize(bucket_group, f"purchase value quintile {bucket}"))
            liquid = group[group["ticker"] != "TKO"].copy()
            rows.append(summarize(liquid, "purchase ex-TKO baseline"))
            rows.append(summarize(liquid[liquid["signal_value"] >= 1_000_000], "purchase ex-TKO value >= $1m"))
            rows.append(summarize(liquid[liquid["signal_value"] >= 2_000_000], "purchase ex-TKO value >= $2m"))
            if "signal_value_to_adv60" in liquid.columns:
                liquid["signal_value_to_adv60"] = pd.to_numeric(
                    liquid["signal_value_to_adv60"], errors="coerce"
                )
                valid_intensity = liquid[liquid["signal_value_to_adv60"].notna()].copy()
                valid_intensity["adv_intensity_bucket"] = pd.qcut(
                    valid_intensity["signal_value_to_adv60"],
                    5,
                    labels=["Q1", "Q2", "Q3", "Q4", "Q5"],
                    duplicates="drop",
                )
                for bucket, bucket_group in valid_intensity.groupby("adv_intensity_bucket", observed=True):
                    rows.append(summarize(bucket_group, f"purchase ex-TKO value/ADV60 quintile {bucket}"))
                rows.append(
                    summarize(
                        valid_intensity[
                            (valid_intensity["signal_value"] >= 1_000_000)
                            & (valid_intensity["signal_value_to_adv60"] >= 0.05)
                        ],
                        "purchase ex-TKO >= $1m + value/ADV60 >= 5%",
                    )
                )
        if "entry_premium_to_vwap" in group.columns:
            rows.append(
                summarize(
                    group[group["entry_premium_to_vwap"] <= 0],
                    f"{signal_type} entry <= insider VWAP",
                )
            )
            rows.append(
                summarize(
                    group[(group["signal_value"] >= 2_000_000) & (group["entry_premium_to_vwap"] <= 0)],
                    f"{signal_type} value >= $2m + entry <= VWAP",
                )
            )
    return pd.DataFrame(rows)


def pct(value: object) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value) * 100:.1f}%"


def write_report(output_dir: Path, returns: pd.DataFrame, diagnostics: pd.DataFrame, summary: pd.DataFrame, config: Config) -> None:
    lines = [
        "# SEC Signal Return Attachment Report",
        "",
        "This is an exploratory Yahoo-price return attachment to the cached SEC "
        "signal database. It is not production-valid because it does not include "
        "delisting returns or historical ticker mapping.",
        "",
        f"- Hold: {config.hold_calendar_days} calendar days",
        f"- Costs: {config.cost_bps_each_side:g} bps each side",
        f"- Completed signals: {len(returns)}",
        f"- Price diagnostics: {len(diagnostics)}",
        "",
        "## Summary",
        "",
        "| Variant | N | Avg Return | Median Return | Avg Excess | Median Excess | Beat Rate | Avg Value |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        avg_value = "n/a" if pd.isna(row.avg_value) else f"${row.avg_value:,.0f}"
        lines.append(
            f"| {row.variant} | {row.n} | {pct(row.avg_return)} | {pct(row.median_return)} | "
            f"{pct(row.avg_excess)} | {pct(row.median_excess)} | {pct(row.beat_rate)} | {avg_value} |"
        )

    if not returns.empty:
        lines.extend(["", "## Best Completed Signals", ""])
        lines.extend(["| Type | Ticker | Signal | Value | Net Return | SPY Return | Excess |", "| --- | --- | --- | ---: | ---: | ---: | ---: |"])
        for row in returns.sort_values("excess_return", ascending=False).head(20).itertuples(index=False):
            lines.append(
                f"| {row.signal_type} | {row.ticker} | {row.signal_datetime.date()} | "
                f"${row.signal_value:,.0f} | {pct(row.net_return)} | {pct(row.benchmark_return)} | {pct(row.excess_return)} |"
            )
        lines.extend(["", "## Worst Completed Signals", ""])
        lines.extend(["| Type | Ticker | Signal | Value | Net Return | SPY Return | Excess |", "| --- | --- | --- | ---: | ---: | ---: | ---: |"])
        for row in returns.sort_values("excess_return").head(20).itertuples(index=False):
            lines.append(
                f"| {row.signal_type} | {row.ticker} | {row.signal_datetime.date()} | "
                f"${row.signal_value:,.0f} | {pct(row.net_return)} | {pct(row.benchmark_return)} | {pct(row.excess_return)} |"
            )

    (output_dir / "return_report.md").write_text("\n".join(lines), encoding="utf-8")


def run(config: Config) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    purchases = load_purchase_signals(config.signal_dir)
    clusters = load_cluster_signals(config.signal_dir)
    signals = pd.concat([purchases, clusters], ignore_index=True, sort=False)
    signals["signal_datetime"] = pd.to_datetime(signals["signal_datetime"], errors="coerce")
    signals = signals[signals["signal_datetime"].notna()].copy()

    tickers = sorted(set(signals["ticker"].dropna().astype(str).str.upper()) | {config.benchmark.upper()})
    start = signals["signal_datetime"].min() - pd.Timedelta(days=10)
    end = signals["signal_datetime"].max() + pd.Timedelta(days=config.hold_calendar_days + 30)
    prices = download_prices(tickers, start, end)
    returns, diagnostics = attach_returns(signals, prices, config)
    summary = build_summary(returns)

    signals.to_csv(config.output_dir / "signals.csv", index=False)
    returns.to_csv(config.output_dir / "signal_returns.csv", index=False)
    diagnostics.to_csv(config.output_dir / "price_diagnostics.csv", index=False)
    summary.to_csv(config.output_dir / "summary.csv", index=False)
    (config.output_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "created_at": datetime.now(UTC).isoformat(),
                "signal_dir": str(config.signal_dir),
                "benchmark": config.benchmark,
                "hold_calendar_days": config.hold_calendar_days,
                "cost_bps_each_side": config.cost_bps_each_side,
                "label": "exploratory_yahoo_survivorship_risky",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    write_report(config.output_dir, returns, diagnostics, summary, config)
    print((config.output_dir / "return_report.md").read_text())


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Attach exploratory returns to cached SEC signal database.")
    parser.add_argument("--signal-dir", type=Path, default=Path("data/results/sec_signals_5y_top25"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/sec_signals_5y_top25_returns"))
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--hold-calendar-days", type=int, default=366)
    parser.add_argument("--cost-bps-each-side", type=float, default=10)
    args = parser.parse_args()
    return Config(
        signal_dir=args.signal_dir,
        output_dir=args.output_dir,
        benchmark=args.benchmark.upper(),
        hold_calendar_days=args.hold_calendar_days,
        cost_bps_each_side=args.cost_bps_each_side,
    )


if __name__ == "__main__":
    run(parse_args())
