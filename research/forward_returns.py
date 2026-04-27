from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import yfinance as yf


HORIZONS = (30, 90, 180, 366)


def build_forward_returns(episodes: pd.DataFrame, *, benchmark: str = "SPY") -> pd.DataFrame:
    if episodes.empty:
        return pd.DataFrame()
    rows = []
    today = pd.Timestamp.utcnow().normalize().tz_localize(None)
    benchmark_prices = download_prices(benchmark, episodes, today)
    for row in episodes.itertuples(index=False):
        ticker = str(row.ticker)
        prices = download_prices(ticker, episodes[episodes["ticker"] == ticker], today)
        signal_date = pd.to_datetime(row.filing_datetime, errors="coerce")
        if pd.isna(signal_date):
            signal_date = pd.to_datetime(row.source_date, errors="coerce") if hasattr(row, "source_date") else pd.NaT
        if pd.isna(signal_date):
            continue
        signal_date = signal_date.tz_localize(None) if getattr(signal_date, "tzinfo", None) else signal_date
        entry = first_price_on_or_after(prices, signal_date.normalize())
        benchmark_entry = first_price_on_or_after(benchmark_prices, signal_date.normalize())
        result = {
            "episode_key": getattr(row, "episode_key", None),
            "ticker": ticker,
            "issuer_name": getattr(row, "issuer_name", None),
            "filing_datetime": getattr(row, "filing_datetime", None),
            "research_score": getattr(row, "research_score", None),
            "research_tier": getattr(row, "research_tier", None),
            "entry_date": entry[0] if entry else None,
            "entry_price": entry[1] if entry else None,
            "benchmark_entry_price": benchmark_entry[1] if benchmark_entry else None,
        }
        for horizon in HORIZONS:
            target_date = signal_date.normalize() + pd.Timedelta(days=horizon)
            if target_date > today:
                result[f"return_{horizon}d"] = None
                result[f"benchmark_return_{horizon}d"] = None
                result[f"excess_return_{horizon}d"] = None
                continue
            exit_price = first_price_on_or_after(prices, target_date)
            benchmark_exit = first_price_on_or_after(benchmark_prices, target_date)
            stock_return = exit_price[1] / entry[1] - 1 if entry and exit_price else None
            benchmark_return = (
                benchmark_exit[1] / benchmark_entry[1] - 1
                if benchmark_entry and benchmark_exit
                else None
            )
            result[f"return_{horizon}d"] = stock_return
            result[f"benchmark_return_{horizon}d"] = benchmark_return
            result[f"excess_return_{horizon}d"] = (
                stock_return - benchmark_return
                if stock_return is not None and benchmark_return is not None
                else None
            )
        rows.append(result)
    return pd.DataFrame(rows)


def download_prices(ticker: str, episodes: pd.DataFrame, today: pd.Timestamp) -> pd.Series:
    dates = pd.to_datetime(episodes["filing_datetime"], errors="coerce")
    dates = dates.dropna()
    if dates.empty:
        return pd.Series(dtype=float)
    start = dates.min().normalize() - pd.Timedelta(days=5)
    end = today + pd.Timedelta(days=5)
    prices = yf.download(ticker, start=start.date(), end=end.date(), interval="1d", auto_adjust=True, progress=False)
    if prices.empty or "Close" not in prices:
        return pd.Series(dtype=float)
    close = prices["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close.index = pd.to_datetime(close.index).tz_localize(None)
    return close.dropna()


def first_price_on_or_after(prices: pd.Series, target_date: pd.Timestamp) -> tuple[str, float] | None:
    if prices.empty:
        return None
    target_date = pd.Timestamp(target_date).tz_localize(None)
    later = prices[prices.index >= target_date]
    if later.empty:
        return None
    return later.index[0].date().isoformat(), float(later.iloc[0])


def write_report(returns: pd.DataFrame, output_dir: Path) -> None:
    lines = [
        "# Forward Return Audit",
        "",
        f"Created at: `{datetime.now(UTC).isoformat()}`",
        "",
        "Returns use free Yahoo adjusted closes and are for monitoring, not production-grade attribution.",
        "",
    ]
    if returns.empty:
        lines.append("No candidate episodes are available.")
    else:
        lines.extend(
            [
                "| Ticker | Tier | Score | Entry | 30d Excess | 90d Excess | 180d Excess | 366d Excess |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in returns.itertuples(index=False):
            lines.append(
                f"| {row.ticker} | {row.research_tier} | {row.research_score} | {money(row.entry_price)} | "
                f"{pct(getattr(row, 'excess_return_30d'))} | {pct(getattr(row, 'excess_return_90d'))} | "
                f"{pct(getattr(row, 'excess_return_180d'))} | {pct(getattr(row, 'excess_return_366d'))} |"
            )
    (output_dir / "forward_return_report.md").write_text("\n".join(lines), encoding="utf-8")


def pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value) * 100:.1f}%"


def money(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"${float(value):,.2f}"


def run(input_path: Path, output_dir: Path) -> None:
    episodes = pd.read_csv(input_path) if input_path.exists() else pd.DataFrame()
    returns = build_forward_returns(episodes)
    returns.to_csv(output_dir / "candidate_forward_returns.csv", index=False)
    write_report(returns, output_dir)
    print((output_dir / "forward_return_report.md").read_text())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update forward-return audit CSVs for insider alert episodes.")
    parser.add_argument("--input", type=Path, default=Path("data/live/sec_daily/candidate_episode_history.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/live/sec_daily"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.output_dir)
