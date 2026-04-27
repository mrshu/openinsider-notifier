from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yfinance as yf


OFFICER_PATTERNS = (
    "CEO",
    "CFO",
    "COO",
    "COB",
    "CHAIR",
    "PRES",
    "PRESIDENT",
    "DIRECTOR",
    "10%",
)


@dataclass(frozen=True)
class BacktestConfig:
    events_path: Path
    output_dir: Path
    benchmark: str
    min_value: float
    cluster_window_days: int
    hold_days: int
    drawdown_threshold: float
    min_cluster_value: float


def clean_column_name(name: str) -> str:
    return (
        name.replace("\xa0", " ")
        .replace("Δ", "delta_")
        .strip()
        .lower()
        .replace(" ", "_")
    )


def parse_number(value: object) -> float:
    if value is None:
        return math.nan
    text = str(value).strip()
    if not text:
        return math.nan
    is_negative = text.startswith("-")
    text = re.sub(r"[^0-9.]", "", text)
    if not text:
        return math.nan
    parsed = float(text)
    return -parsed if is_negative else parsed


def parse_percent(value: object) -> float:
    parsed = parse_number(value)
    if math.isnan(parsed):
        return math.nan
    return parsed / 100.0


def read_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    with path.open() as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return pd.DataFrame(rows)


def normalize_events(path: Path, min_value: float) -> pd.DataFrame:
    raw = read_jsonl(path)
    raw = raw.rename(columns={col: clean_column_name(col) for col in raw.columns})

    required = {
        "filing_date",
        "trade_date",
        "ticker",
        "company_name",
        "insider_name",
        "title",
        "trade_type",
        "price",
        "qty",
        "value",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        raise ValueError(f"Missing required event columns: {', '.join(missing)}")

    events = pd.DataFrame(
        {
            "filing_datetime": pd.to_datetime(raw["filing_date"], errors="coerce"),
            "trade_date": pd.to_datetime(raw["trade_date"], errors="coerce"),
            "ticker": raw["ticker"].astype(str).str.strip().str.upper(),
            "company_name": raw["company_name"].astype(str).str.strip(),
            "insider_name": raw["insider_name"].astype(str).str.strip(),
            "title": raw["title"].astype(str).str.strip().str.upper(),
            "trade_type": raw["trade_type"].astype(str).str.strip(),
            "price": raw["price"].map(parse_number),
            "qty": raw["qty"].map(parse_number),
            "value": raw["value"].map(parse_number),
            "ownership_change": raw.get("delta_own", pd.Series(index=raw.index)).map(
                parse_percent
            ),
        }
    )

    purchases = events[
        events["trade_type"].str.startswith("P - Purchase", na=False)
        & events["filing_datetime"].notna()
        & events["ticker"].ne("")
        & (events["value"] >= min_value)
    ].copy()
    purchases = purchases.drop_duplicates(
        ["filing_datetime", "ticker", "insider_name", "value", "qty"]
    )
    purchases["is_officer_or_director"] = purchases["title"].map(is_officer_or_director)
    return purchases.sort_values(["ticker", "filing_datetime"]).reset_index(drop=True)


def is_officer_or_director(title: object) -> bool:
    title_text = str(title).upper()
    return any(pattern in title_text for pattern in OFFICER_PATTERNS)


def build_cluster_signals(
    events: pd.DataFrame, window_days: int, min_cluster_value: float
) -> pd.DataFrame:
    signals = []
    for ticker, ticker_events in events.groupby("ticker", sort=False):
        ticker_events = ticker_events.sort_values("filing_datetime").reset_index(drop=True)
        last_signal_date = pd.Timestamp.min

        for _, row in ticker_events.iterrows():
            signal_date = row["filing_datetime"]
            window_start = signal_date - pd.Timedelta(days=window_days)
            window = ticker_events[
                (ticker_events["filing_datetime"] >= window_start)
                & (ticker_events["filing_datetime"] <= signal_date)
            ]
            distinct_insiders = window["insider_name"].nunique()
            cluster_value = window["value"].sum()

            if (
                distinct_insiders >= 2
                and cluster_value >= min_cluster_value
                and signal_date > last_signal_date + pd.Timedelta(days=window_days)
            ):
                signals.append(
                    {
                        "strategy": "cluster",
                        "signal_datetime": signal_date,
                        "ticker": ticker,
                        "company_name": row["company_name"],
                        "insider_count": distinct_insiders,
                        "cluster_value": cluster_value,
                        "event_count": len(window),
                    }
                )
                last_signal_date = signal_date

    return pd.DataFrame(signals)


def build_signals(events: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    signal_frames = [
        events.assign(
            strategy="all_purchases",
            signal_datetime=events["filing_datetime"],
            insider_count=1,
            cluster_value=events["value"],
            event_count=1,
        )[
            [
                "strategy",
                "signal_datetime",
                "ticker",
                "company_name",
                "insider_count",
                "cluster_value",
                "event_count",
            ]
        ],
        events[events["is_officer_or_director"]]
        .assign(
            strategy="officer_director",
            signal_datetime=lambda df: df["filing_datetime"],
            insider_count=1,
            cluster_value=lambda df: df["value"],
            event_count=1,
        )[
            [
                "strategy",
                "signal_datetime",
                "ticker",
                "company_name",
                "insider_count",
                "cluster_value",
                "event_count",
            ]
        ],
        build_cluster_signals(
            events, config.cluster_window_days, config.min_cluster_value
        ),
    ]
    signals = pd.concat(signal_frames, ignore_index=True)
    return signals.sort_values(["strategy", "signal_datetime", "ticker"]).reset_index(
        drop=True
    )


def download_close_prices(
    tickers: list[str], start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    data = yf.download(
        tickers=tickers,
        start=start.date().isoformat(),
        end=end.date().isoformat(),
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    if data.empty:
        raise ValueError("No price data returned from Yahoo Finance")

    if isinstance(data.columns, pd.MultiIndex):
        if "Close" in data.columns.get_level_values(0):
            close = data["Close"]
        else:
            close = data.xs("Close", level=1, axis=1)
    else:
        close = data[["Close"]].rename(columns={"Close": tickers[0]})

    close.index = pd.to_datetime(close.index).tz_localize(None)
    close = close.sort_index()
    return close


def first_index_after(index: pd.DatetimeIndex, date: pd.Timestamp) -> int | None:
    position = index.searchsorted(date.normalize(), side="right")
    if position >= len(index):
        return None
    return int(position)


def add_event_returns(
    signals: pd.DataFrame, prices: pd.DataFrame, config: BacktestConfig
) -> pd.DataFrame:
    benchmark = config.benchmark.upper()
    rows = []

    for signal in signals.itertuples(index=False):
        ticker = signal.ticker
        if ticker not in prices.columns or benchmark not in prices.columns:
            continue

        stock = prices[ticker].dropna()
        bench = prices[benchmark].dropna()
        if stock.empty or bench.empty:
            continue

        entry_pos = first_index_after(stock.index, signal.signal_datetime)
        if entry_pos is None:
            continue
        exit_pos = entry_pos + config.hold_days
        if exit_pos >= len(stock.index):
            continue

        entry_date = stock.index[entry_pos]
        exit_date = stock.index[exit_pos]
        entry_price = stock.iloc[entry_pos]
        exit_price = stock.iloc[exit_pos]

        bench_entry_pos = bench.index.searchsorted(entry_date, side="left")
        bench_exit_pos = bench.index.searchsorted(exit_date, side="left")
        if bench_entry_pos >= len(bench.index) or bench_exit_pos >= len(bench.index):
            continue

        bench_entry = bench.iloc[bench_entry_pos]
        bench_exit = bench.iloc[bench_exit_pos]
        prior = stock.iloc[max(0, entry_pos - 252) : entry_pos]
        drawdown_52w = (
            entry_price / prior.max() - 1.0 if len(prior) >= 30 and prior.max() else math.nan
        )

        stock_return = exit_price / entry_price - 1.0
        benchmark_return = bench_exit / bench_entry - 1.0

        rows.append(
            {
                **signal._asdict(),
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "return": stock_return,
                "benchmark_return": benchmark_return,
                "excess_return": stock_return - benchmark_return,
                "drawdown_52w": drawdown_52w,
            }
        )

    event_returns = pd.DataFrame(rows)
    if event_returns.empty:
        return event_returns

    drawdown = event_returns[
        (event_returns["strategy"] == "cluster")
        & (event_returns["drawdown_52w"] <= -abs(config.drawdown_threshold))
    ].copy()
    drawdown["strategy"] = "cluster_plus_30pct_drawdown"
    return pd.concat([event_returns, drawdown], ignore_index=True)


def summarize_event_returns(event_returns: pd.DataFrame) -> pd.DataFrame:
    if event_returns.empty:
        return pd.DataFrame()

    grouped = event_returns.groupby("strategy", sort=True)
    summary = grouped.agg(
        signals=("ticker", "count"),
        unique_tickers=("ticker", "nunique"),
        avg_return=("return", "mean"),
        median_return=("return", "median"),
        avg_benchmark_return=("benchmark_return", "mean"),
        avg_excess_return=("excess_return", "mean"),
        median_excess_return=("excess_return", "median"),
        win_rate=("return", lambda values: (values > 0).mean()),
        beat_benchmark_rate=("excess_return", lambda values: (values > 0).mean()),
        avg_52w_drawdown=("drawdown_52w", "mean"),
    )
    return summary.reset_index()


def build_daily_portfolio(
    event_returns: pd.DataFrame, prices: pd.DataFrame, benchmark: str
) -> pd.DataFrame:
    if event_returns.empty:
        return pd.DataFrame()

    daily_returns = prices.pct_change(fill_method=None)
    benchmark = benchmark.upper()
    frames = []

    for strategy, trades in event_returns.groupby("strategy"):
        start = trades["entry_date"].min()
        end = trades["exit_date"].max()
        index = daily_returns.loc[start:end].index
        strategy_returns = pd.Series(0.0, index=index)
        active_counts = pd.Series(0, index=index)

        for trade in trades.itertuples(index=False):
            if trade.ticker not in daily_returns.columns:
                continue
            active = daily_returns.loc[
                (daily_returns.index > trade.entry_date)
                & (daily_returns.index <= trade.exit_date),
                trade.ticker,
            ].dropna()
            if active.empty:
                continue
            active_index = active.index
            active_counts.loc[active_index] += 1
            strategy_returns.loc[active_index] += active

        active_mask = active_counts > 0
        strategy_returns.loc[active_mask] = (
            strategy_returns.loc[active_mask] / active_counts.loc[active_mask]
        )
        benchmark_returns = daily_returns.loc[index, benchmark].fillna(0.0)

        frames.append(
            pd.DataFrame(
                {
                    "date": index,
                    "strategy": strategy,
                    "daily_return": strategy_returns.values,
                    "benchmark_daily_return": benchmark_returns.values,
                    "active_positions": active_counts.values,
                }
            )
        )

    return pd.concat(frames, ignore_index=True)


def summarize_portfolio(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()

    rows = []
    for strategy, frame in daily.groupby("strategy"):
        frame = frame.sort_values("date")
        equity = (1.0 + frame["daily_return"]).cumprod()
        benchmark_equity = (1.0 + frame["benchmark_daily_return"]).cumprod()
        years = max((frame["date"].max() - frame["date"].min()).days / 365.25, 1 / 365.25)
        cagr = equity.iloc[-1] ** (1.0 / years) - 1.0
        benchmark_cagr = benchmark_equity.iloc[-1] ** (1.0 / years) - 1.0
        drawdown = equity / equity.cummax() - 1.0

        rows.append(
            {
                "strategy": strategy,
                "start": frame["date"].min(),
                "end": frame["date"].max(),
                "cagr": cagr,
                "benchmark_cagr": benchmark_cagr,
                "excess_cagr": cagr - benchmark_cagr,
                "max_drawdown": drawdown.min(),
                "avg_active_positions": frame["active_positions"].mean(),
                "days_in_market": (frame["active_positions"] > 0).mean(),
            }
        )

    return pd.DataFrame(rows)


def write_outputs(
    events: pd.DataFrame,
    signals: pd.DataFrame,
    event_returns: pd.DataFrame,
    event_summary: pd.DataFrame,
    daily_portfolio: pd.DataFrame,
    portfolio_summary: pd.DataFrame,
    output_dir: Path,
    events_path: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    events.to_csv(output_dir / "normalized_events.csv", index=False)
    signals.to_csv(output_dir / "signals.csv", index=False)
    event_returns.to_csv(output_dir / "event_returns.csv", index=False)
    event_summary.to_csv(output_dir / "event_summary.csv", index=False)
    daily_portfolio.to_csv(output_dir / "daily_portfolio.csv", index=False)
    portfolio_summary.to_csv(output_dir / "portfolio_summary.csv", index=False)
    (output_dir / "report.md").write_text(
        build_markdown_report(event_summary, portfolio_summary, events_path),
        encoding="utf-8",
    )


def format_pct(value: object) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value) * 100:.1f}%"


def build_markdown_report(
    event_summary: pd.DataFrame, portfolio_summary: pd.DataFrame, events_path: Path
) -> str:
    lines = [
        "# Free-data insider backtest report",
        "",
        f"This report is generated from the OpenInsider-style events in "
        f"`{events_path}` and Yahoo Finance adjusted daily close prices.",
        "",
        "Important limitations:",
        "",
        "- Yahoo Finance can omit delisted or renamed tickers, which can bias "
        "results upward.",
        "- The test uses first adjusted close after filing date as entry, not "
        "intraday execution.",
        "- The current implementation does not include point-in-time valuation, "
        "buyback, or sector-neutral controls.",
        "",
    ]

    if event_summary.empty:
        lines.extend(["No event returns were generated.", ""])
        return "\n".join(lines)

    lines.extend(
        [
            "## Event Study",
            "",
            "| Strategy | Signals | Avg Return | Avg SPY Return | Avg Excess | "
            "Beat SPY | Win Rate |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in event_summary.sort_values("strategy").itertuples(index=False):
        lines.append(
            f"| {row.strategy} | {row.signals} | {format_pct(row.avg_return)} | "
            f"{format_pct(row.avg_benchmark_return)} | "
            f"{format_pct(row.avg_excess_return)} | "
            f"{format_pct(row.beat_benchmark_rate)} | {format_pct(row.win_rate)} |"
        )

    lines.extend(["", "## Rolling Portfolio", ""])
    if portfolio_summary.empty:
        lines.extend(["No portfolio results were generated.", ""])
    else:
        lines.extend(
            [
                "| Strategy | Start | End | CAGR | SPY CAGR | Excess CAGR | "
                "Max Drawdown | Avg Active Positions |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in portfolio_summary.sort_values("strategy").itertuples(index=False):
            lines.append(
                f"| {row.strategy} | {row.start.date()} | {row.end.date()} | "
                f"{format_pct(row.cagr)} | {format_pct(row.benchmark_cagr)} | "
                f"{format_pct(row.excess_cagr)} | {format_pct(row.max_drawdown)} | "
                f"{row.avg_active_positions:.1f} |"
            )

    best_event = event_summary.sort_values("avg_excess_return", ascending=False).iloc[0]
    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"The best event-study variant by average excess return was "
            f"`{best_event['strategy']}`, at "
            f"{format_pct(best_event['avg_excess_return'])} versus SPY.",
        ]
    )
    if best_event["avg_excess_return"] <= 0:
        lines.append(
            "None of the tested variants showed positive average excess return "
            "in this captured dataset."
        )
    else:
        lines.append(
            "At least one tested variant showed positive average excess return "
            "in this captured dataset."
        )

    lines.append("")
    return "\n".join(lines)


def run(config: BacktestConfig) -> None:
    events = normalize_events(config.events_path, config.min_value)
    signals = build_signals(events, config)

    tickers = sorted(set(signals["ticker"]) | {config.benchmark.upper()})
    start = signals["signal_datetime"].min() - pd.Timedelta(days=400)
    end = signals["signal_datetime"].max() + pd.Timedelta(days=config.hold_days * 2 + 30)
    prices = download_close_prices(tickers, start, end)

    event_returns = add_event_returns(signals, prices, config)
    event_summary = summarize_event_returns(event_returns)
    daily_portfolio = build_daily_portfolio(event_returns, prices, config.benchmark)
    portfolio_summary = summarize_portfolio(daily_portfolio)

    write_outputs(
        events,
        signals,
        event_returns,
        event_summary,
        daily_portfolio,
        portfolio_summary,
        config.output_dir,
        config.events_path,
    )

    print("\nEvent summary")
    print(event_summary.to_string(index=False))
    print("\nPortfolio summary")
    print(portfolio_summary.to_string(index=False))
    print(f"\nWrote CSV and Markdown outputs to {config.output_dir}")


def parse_args() -> BacktestConfig:
    parser = argparse.ArgumentParser(
        description="Backtest insider purchase signals using only free data sources."
    )
    parser.add_argument("--events", type=Path, default=Path("trades.jsonl"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/free_backtest"))
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--min-value", type=float, default=100_000)
    parser.add_argument("--cluster-window-days", type=int, default=7)
    parser.add_argument("--hold-days", type=int, default=252)
    parser.add_argument("--drawdown-threshold", type=float, default=0.30)
    parser.add_argument("--min-cluster-value", type=float, default=250_000)
    args = parser.parse_args()
    return BacktestConfig(
        events_path=args.events,
        output_dir=args.output_dir,
        benchmark=args.benchmark,
        min_value=args.min_value,
        cluster_window_days=args.cluster_window_days,
        hold_days=args.hold_days,
        drawdown_threshold=args.drawdown_threshold,
        min_cluster_value=args.min_cluster_value,
    )


if __name__ == "__main__":
    run(parse_args())
