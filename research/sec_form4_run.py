from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yfinance as yf

from research.form4_ingest import parse_form4_xml
from research.portfolio import build_cluster_signals, suppress_active_signals


SEC_HEADERS = {
    "User-Agent": "openinsider-notifier research contact@example.com",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass(frozen=True)
class Config:
    tickers: list[str] | None
    tickers_from: Path
    output_dir: Path
    start: pd.Timestamp
    end: pd.Timestamp
    min_purchase_value: float
    min_cluster_value: float
    cluster_window_trading_days: int
    hold_calendar_days: int
    max_positions: int
    max_position_weight: float
    sector_cap: float
    benchmark: str
    sec_sleep_seconds: float
    max_tickers: int | None
    max_filings_per_ticker: int | None


def read_tickers_from_openinsider(path: Path) -> list[str]:
    rows = []
    with path.open() as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    frame = pd.DataFrame(rows)
    if "Ticker" not in frame.columns:
        raise ValueError(f"{path} must contain an OpenInsider `Ticker` column")
    counts = frame["Ticker"].dropna().astype(str).str.upper().value_counts()
    return counts.index.tolist()


def sec_get_json(url: str, sleep_seconds: float) -> dict[str, Any]:
    time.sleep(sleep_seconds)
    response = requests.get(url, headers=SEC_HEADERS, timeout=60)
    response.raise_for_status()
    return response.json()


def sec_get_text(url: str, sleep_seconds: float) -> str:
    time.sleep(sleep_seconds)
    response = requests.get(url, headers=SEC_HEADERS, timeout=60)
    response.raise_for_status()
    return response.text


def load_current_ticker_map(sleep_seconds: float) -> pd.DataFrame:
    data = sec_get_json("https://www.sec.gov/files/company_tickers.json", sleep_seconds)
    rows = []
    for item in data.values():
        rows.append(
            {
                "ticker": str(item["ticker"]).upper(),
                "title": item["title"],
                "cik": str(item["cik_str"]).zfill(10),
            }
        )
    return pd.DataFrame(rows)


def submissions_for_cik(cik: str, sleep_seconds: float) -> dict[str, Any]:
    return sec_get_json(f"https://data.sec.gov/submissions/CIK{cik}.json", sleep_seconds)


def recent_filings(
    submission: dict[str, Any],
    ticker: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    max_filings: int | None,
) -> list[dict[str, Any]]:
    recent = submission.get("filings", {}).get("recent", {})
    rows = []
    for idx, form in enumerate(recent.get("form", [])):
        if form not in {"4", "4/A"}:
            continue
        accepted_at = pd.to_datetime(recent["acceptanceDateTime"][idx], errors="coerce")
        if pd.isna(accepted_at):
            continue
        accepted_at = accepted_at.tz_localize(None) if accepted_at.tzinfo else accepted_at
        if accepted_at < start or accepted_at > end:
            continue
        accession = recent["accessionNumber"][idx]
        primary_doc = recent["primaryDocument"][idx]
        cik = str(submission["cik"]).zfill(10)
        accession_path = accession.replace("-", "")
        rows.append(
            {
                "ticker": ticker,
                "cik": cik,
                "company_name": submission.get("name"),
                "form_type": form,
                "amendment_flag": form == "4/A",
                "accepted_at": accepted_at,
                "accession": accession,
                "primary_document": primary_doc,
                "source_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_path}/{primary_doc}",
            }
        )
        if max_filings is not None and len(rows) >= max_filings:
            break
    return rows


def find_xml_url(filing: dict[str, Any], sleep_seconds: float) -> str:
    primary = str(filing["primary_document"]).lower()
    if primary.endswith(".xml") and "/" not in primary:
        return str(filing["source_url"])

    cik = str(int(filing["cik"]))
    accession_path = str(filing["accession"]).replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/index.json"
    index = sec_get_json(index_url, sleep_seconds)
    for item in index.get("directory", {}).get("item", []):
        name = item.get("name", "")
        if name.lower().endswith(".xml"):
            return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/{name}"
    return str(filing["source_url"])


def download_and_parse_form4(tickers: list[str], config: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    ticker_map = load_current_ticker_map(config.sec_sleep_seconds)
    selected_tickers = tickers[: config.max_tickers] if config.max_tickers is not None else tickers
    mapped = ticker_map[ticker_map["ticker"].isin(selected_tickers)].copy()
    if config.max_tickers is not None:
        mapped = mapped.head(config.max_tickers)
    missing = sorted(set(selected_tickers) - set(mapped["ticker"]))
    manifest_rows = []
    records = []

    for count, row in enumerate(mapped.itertuples(index=False), start=1):
        print(f"SEC {count}/{len(mapped)} {row.ticker}", flush=True)
        try:
            submission = submissions_for_cik(row.cik, config.sec_sleep_seconds)
        except requests.HTTPError as exc:
            manifest_rows.append({"ticker": row.ticker, "cik": row.cik, "status": f"submission_error:{exc}"})
            continue

        filings = recent_filings(
            submission,
            row.ticker,
            config.start,
            config.end,
            config.max_filings_per_ticker,
        )
        print(f"  filings={len(filings)}", flush=True)
        for filing in filings:
            try:
                xml_url = find_xml_url(filing, config.sec_sleep_seconds)
                xml_text = sec_get_text(xml_url, config.sec_sleep_seconds)
                parsed = parse_form4_xml(
                    xml_text,
                    accepted_at=str(filing["accepted_at"]),
                    accession=filing["accession"],
                    cik=filing["cik"],
                    ticker=filing["ticker"],
                )
                sha = hashlib.sha256(xml_text.encode()).hexdigest()
                manifest_rows.append(
                    {
                        **filing,
                        "xml_url": xml_url,
                        "sha256": sha,
                        "status": "parsed",
                        "parsed_records": len(parsed),
                    }
                )
                records.extend(parsed)
            except Exception as exc:  # noqa: BLE001 - diagnostics artifact needs exact failures
                manifest_rows.append(
                    {
                        **filing,
                        "xml_url": filing["source_url"],
                        "status": f"parse_error:{type(exc).__name__}:{exc}",
                        "parsed_records": 0,
                    }
                )

    for ticker in missing:
        manifest_rows.append({"ticker": ticker, "status": "missing_current_sec_ticker_map"})

    return pd.DataFrame(records), pd.DataFrame(manifest_rows)


def serialize_value(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return json.dumps(value, default=serialize_value)
    return value


def build_eligible_purchases(raw: pd.DataFrame, config: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    if raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    frame = raw.copy()
    for col in ["shares", "price_per_share", "purchase_value"]:
        frame[col] = frame[col].map(lambda value: float(value) if isinstance(value, Decimal) else value)
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["filing_datetime"] = pd.to_datetime(frame["accepted_at"], errors="coerce")
    frame["insider_name"] = frame["reporting_owners"].map(owner_names)
    frame["company_name"] = frame["issuer_name"]

    keep = (
        (frame["eligible_insider"] == True)  # noqa: E712
        & (frame["purchase_value"] >= config.min_purchase_value)
        & frame["filing_datetime"].notna()
    )
    eligible = frame[keep].copy()
    dropped = frame[~keep].copy()
    dropped["drop_reason"] = dropped.apply(drop_reason, axis=1, config=config)
    return eligible, dropped


def owner_names(owners: object) -> str:
    if not isinstance(owners, list):
        return ""
    names = [str(owner.get("name")) for owner in owners if owner.get("name")]
    return "; ".join(sorted(set(names)))


def drop_reason(row: pd.Series, config: Config) -> str:
    if row.get("eligible_insider") is not True:
        return "ineligible_owner"
    if pd.isna(row.get("purchase_value")):
        return "missing_purchase_value"
    if float(row["purchase_value"]) < config.min_purchase_value:
        return "below_min_purchase_value"
    if pd.isna(row.get("filing_datetime")):
        return "missing_filing_datetime"
    return "unknown"


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
        raise ValueError("Yahoo returned no prices")
    data.index = pd.to_datetime(data.index).tz_localize(None)
    return data


def price_panel(data: pd.DataFrame, field: str, single_ticker: str | None = None) -> pd.DataFrame:
    if isinstance(data.columns, pd.MultiIndex):
        return data[field].copy()
    if field in data.columns and single_ticker:
        return data[[field]].rename(columns={field: single_ticker})
    raise ValueError(f"Price field {field} missing")


def first_trading_index_after(index: pd.DatetimeIndex, timestamp: pd.Timestamp) -> int | None:
    position = index.searchsorted(timestamp.normalize(), side="right")
    if position >= len(index):
        return None
    return int(position)


def first_trading_index_on_or_after(index: pd.DatetimeIndex, timestamp: pd.Timestamp) -> int | None:
    position = index.searchsorted(timestamp.normalize(), side="left")
    if position >= len(index):
        return None
    return int(position)


def build_trades(
    signals: pd.DataFrame,
    price_data: pd.DataFrame,
    benchmark: str,
    hold_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if signals.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    tickers = sorted(set(signals["ticker"]) | {benchmark})
    opens = price_panel(price_data, "Open")
    closes = price_panel(price_data, "Close")
    rows = []
    diagnostics = []

    for signal in signals.itertuples(index=False):
        ticker = signal.ticker
        if ticker not in opens.columns or ticker not in closes.columns:
            diagnostics.append({"ticker": ticker, "reason": "missing_price_column"})
            continue
        open_series = opens[ticker].dropna()
        close_series = closes[ticker].dropna()
        bench_close = closes[benchmark].dropna()
        entry_pos = first_trading_index_after(open_series.index, signal.signal_datetime)
        if entry_pos is None:
            diagnostics.append({"ticker": ticker, "reason": "missing_entry_price"})
            continue
        entry_date = open_series.index[entry_pos]
        entry_price = float(open_series.iloc[entry_pos])
        if getattr(signal, "insider_vwap", None) and entry_price > float(signal.insider_vwap) * 1.15:
            diagnostics.append({"ticker": ticker, "reason": "entry_price_gt_115pct_insider_vwap"})
            continue
        target_exit = entry_date + pd.Timedelta(days=hold_days)
        exit_pos = first_trading_index_on_or_after(close_series.index, target_exit)
        if exit_pos is None:
            diagnostics.append({"ticker": ticker, "reason": "missing_exit_price"})
            continue
        exit_date = close_series.index[exit_pos]
        exit_price = float(close_series.iloc[exit_pos])
        bench_entry_pos = first_trading_index_on_or_after(bench_close.index, entry_date)
        bench_exit_pos = first_trading_index_on_or_after(bench_close.index, exit_date)
        if bench_entry_pos is None or bench_exit_pos is None:
            diagnostics.append({"ticker": ticker, "reason": "missing_benchmark_price"})
            continue
        gross_return = exit_price / entry_price - 1
        net_return = (1 + gross_return) * (1 - 0.001) * (1 - 0.001) - 1
        benchmark_return = bench_close.iloc[bench_exit_pos] / bench_close.iloc[bench_entry_pos] - 1
        rows.append(
            {
                **signal._asdict(),
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "gross_return": gross_return,
                "net_return": net_return,
                "benchmark_return": benchmark_return,
                "excess_return": net_return - benchmark_return,
                "holding_days": (exit_date - entry_date).days,
            }
        )

    trades = pd.DataFrame(rows)
    positions = trades.copy()
    event_returns = trades.copy()
    return trades, positions, pd.DataFrame(diagnostics), event_returns


def summary_metrics(event_returns: pd.DataFrame) -> pd.DataFrame:
    if event_returns.empty:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "strategy": "sec_cluster_366d",
                "trades": len(event_returns),
                "unique_tickers": event_returns["ticker"].nunique(),
                "mean_trade_return": event_returns["net_return"].mean(),
                "median_trade_return": event_returns["net_return"].median(),
                "mean_benchmark_return": event_returns["benchmark_return"].mean(),
                "mean_excess_return": event_returns["excess_return"].mean(),
                "median_excess_return": event_returns["excess_return"].median(),
                "win_rate": (event_returns["net_return"] > 0).mean(),
                "beat_benchmark_rate": (event_returns["excess_return"] > 0).mean(),
                "average_holding_period": event_returns["holding_days"].mean(),
            }
        ]
    )


def write_diagnostics(
    config: Config,
    manifest: pd.DataFrame,
    raw: pd.DataFrame,
    eligible: pd.DataFrame,
    dropped: pd.DataFrame,
    signals: pd.DataFrame,
    filtered_signals: pd.DataFrame,
    price_diag: pd.DataFrame,
    event_returns: pd.DataFrame,
    metrics: pd.DataFrame,
) -> None:
    lines = [
        "# SEC Form 4 Exploratory Diagnostics",
        "",
        "Label: `exploratory only`.",
        "",
        "This run uses official SEC Form 4 XML for signal construction, but it "
        "uses current SEC ticker mapping and Yahoo Finance prices. It is "
        "`SURVIVORSHIP-BIASED` and not production-valid.",
        "",
        f"Ticker cap: `{config.max_tickers}`. Filing cap per ticker: "
        f"`{config.max_filings_per_ticker}`.",
        "",
        "## Funnel",
        "",
        f"- Filings discovered: {len(manifest)}",
        f"- Filings parsed: {(manifest.get('status') == 'parsed').sum() if 'status' in manifest else 0}",
        f"- Raw P/A transactions parsed: {len(raw)}",
        f"- Eligible owner purchases: {len(eligible)}",
        f"- Dropped transactions: {len(dropped)}",
        f"- Cluster signals: {len(signals)}",
        f"- Active-suppressed signals: {len(filtered_signals)}",
        f"- Completed trades: {len(event_returns)}",
        f"- Price diagnostic rows: {len(price_diag)}",
        "",
    ]
    if not metrics.empty:
        row = metrics.iloc[0]
        lines.extend(
            [
                "## Main Result",
                "",
                f"- Mean net trade return: {row['mean_trade_return']:.1%}",
                f"- Median net trade return: {row['median_trade_return']:.1%}",
                f"- Mean SPY return over matched holds: {row['mean_benchmark_return']:.1%}",
                f"- Mean excess return: {row['mean_excess_return']:.1%}",
                f"- Beat SPY rate: {row['beat_benchmark_rate']:.1%}",
                f"- Win rate: {row['win_rate']:.1%}",
                "",
            ]
        )
    lines.extend(
        [
            "## Bias-Control Review",
            "",
            "- Filing acceptance timestamps are used for signal timing.",
            "- Transaction dates are not used for entry timing.",
            "- Passive 10% owners are excluded unless also officers/directors.",
            "- Duplicate active issuer signals are suppressed.",
            "- 366-calendar-day minimum hold is enforced for completed trades.",
            "- Yahoo missing/delisted cases are recorded in price diagnostics.",
            "- Delisting returns are not validly modeled in this exploratory run.",
            "",
        ]
    )
    (config.output_dir / "diagnostics.md").write_text("\n".join(lines), encoding="utf-8")


def run(config: Config) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    tickers = (
        [ticker.upper() for ticker in config.tickers]
        if config.tickers
        else read_tickers_from_openinsider(config.tickers_from)
    )
    raw, manifest = download_and_parse_form4(tickers, config)

    raw_for_csv = raw.map(serialize_value) if not raw.empty else raw
    raw_for_csv.to_csv(config.output_dir / "raw_form4_transactions.csv", index=False)
    manifest.to_csv(config.output_dir / "filings_manifest.csv", index=False)

    eligible, dropped = build_eligible_purchases(raw, config)
    eligible.map(serialize_value).to_csv(config.output_dir / "eligible_owner_purchases.csv", index=False)
    dropped.map(serialize_value).to_csv(config.output_dir / "dropped_transactions.csv", index=False)

    if eligible.empty:
        signals = pd.DataFrame(
            columns=[
                "ticker",
                "signal_datetime",
                "cluster_start_datetime",
                "insider_count",
                "event_count",
                "cluster_value",
                "cluster_shares",
                "insider_vwap",
                "insiders",
                "company_name",
            ]
        )
    else:
        signals = build_cluster_signals(
            eligible,
            config.cluster_window_trading_days,
            ticker_col="ticker",
            insider_col="insider_name",
            filing_col="filing_datetime",
            company_col="company_name",
            value_col="purchase_value",
            shares_col="shares",
            min_cluster_value=config.min_cluster_value,
        )
    signals.to_csv(config.output_dir / "cluster_signals.csv", index=False)
    filtered_signals = suppress_active_signals(
        signals,
        hold_days=config.hold_calendar_days,
        ticker_col="ticker",
        signal_col="signal_datetime",
    )
    filtered_signals.to_csv(config.output_dir / "cluster_signals_filtered.csv", index=False)

    if filtered_signals.empty:
        price_diag = pd.DataFrame()
        trades = positions = event_returns = pd.DataFrame()
    else:
        price_start = filtered_signals["signal_datetime"].min() - pd.Timedelta(days=10)
        price_end = filtered_signals["signal_datetime"].max() + pd.Timedelta(days=config.hold_calendar_days + 30)
        price_data = download_prices(
            sorted(set(filtered_signals["ticker"]) | {config.benchmark}),
            price_start,
            price_end,
        )
        trades, positions, price_diag, event_returns = build_trades(
            filtered_signals, price_data, config.benchmark, config.hold_calendar_days
        )

    trades.to_csv(config.output_dir / "trades.csv", index=False)
    positions.to_csv(config.output_dir / "positions.csv", index=False)
    pd.DataFrame().to_csv(config.output_dir / "daily_portfolio_returns.csv", index=False)
    event_returns.to_csv(config.output_dir / "event_returns.csv", index=False)
    price_diag.to_csv(config.output_dir / "price_match_diagnostics.csv", index=False)
    metrics = summary_metrics(event_returns)
    metrics.to_csv(config.output_dir / "summary_metrics.csv", index=False)
    write_diagnostics(
        config,
        manifest,
        raw,
        eligible,
        dropped,
        signals,
        filtered_signals,
        price_diag,
        event_returns,
        metrics,
    )
    run_manifest = {
        "label": "exploratory only",
        "data_mode": "free",
        "survivorship_biased": True,
        "sec_source": "sec_xml",
        "price_source": "yfinance",
        "created_at": datetime.now(UTC).isoformat(),
        "parameters": {
            "cluster_window_trading_days": config.cluster_window_trading_days,
            "min_purchase_value": config.min_purchase_value,
            "min_cluster_value": config.min_cluster_value,
            "hold_calendar_days": config.hold_calendar_days,
            "max_tickers": config.max_tickers,
            "max_filings_per_ticker": config.max_filings_per_ticker,
        },
    }
    (config.output_dir / "run_manifest.json").write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")

    print((config.output_dir / "diagnostics.md").read_text())


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Run exploratory SEC Form 4 cluster research.")
    parser.add_argument("--tickers", nargs="*", default=None)
    parser.add_argument("--tickers-from", type=Path, default=Path("data/raw/openinsider_3y.jsonl"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/sec_form4_exploratory"))
    parser.add_argument("--start", default="2023-04-25")
    parser.add_argument("--end", default="2026-04-25")
    parser.add_argument("--min-purchase-value", type=float, default=100_000)
    parser.add_argument("--min-cluster-value", type=float, default=250_000)
    parser.add_argument("--cluster-window-trading-days", type=int, default=5)
    parser.add_argument("--hold-calendar-days", type=int, default=366)
    parser.add_argument("--max-positions", type=int, default=30)
    parser.add_argument("--max-position-weight", type=float, default=0.05)
    parser.add_argument("--sector-cap", type=float, default=0.20)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--sec-sleep-seconds", type=float, default=0.12)
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--max-filings-per-ticker", type=int, default=None)
    args = parser.parse_args()
    return Config(
        tickers=args.tickers,
        tickers_from=args.tickers_from,
        output_dir=args.output_dir,
        start=pd.Timestamp(args.start),
        end=pd.Timestamp(args.end),
        min_purchase_value=args.min_purchase_value,
        min_cluster_value=args.min_cluster_value,
        cluster_window_trading_days=args.cluster_window_trading_days,
        hold_calendar_days=args.hold_calendar_days,
        max_positions=args.max_positions,
        max_position_weight=args.max_position_weight,
        sector_cap=args.sector_cap,
        benchmark=args.benchmark.upper(),
        sec_sleep_seconds=args.sec_sleep_seconds,
        max_tickers=args.max_tickers,
        max_filings_per_ticker=args.max_filings_per_ticker,
    )


if __name__ == "__main__":
    run(parse_args())
