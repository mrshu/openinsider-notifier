from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from research.form4_ingest import parse_form4_xml
from research.portfolio import build_cluster_signals


SEC_HEADERS = {
    "User-Agent": "openinsider-notifier research contact@example.com",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass(frozen=True)
class Config:
    output_dir: Path
    cache_dir: Path
    start: pd.Timestamp
    end: pd.Timestamp
    tickers_from: Path | None
    tickers: list[str] | None
    max_tickers: int | None
    max_filings_per_ticker: int | None
    sleep_seconds: float
    min_purchase_value: float
    min_cluster_value: float
    cluster_window_trading_days: int


def sec_get(url: str, sleep_seconds: float) -> requests.Response:
    time.sleep(sleep_seconds)
    response = requests.get(url, headers=SEC_HEADERS, timeout=60)
    response.raise_for_status()
    return response


def cached_json(path: Path, url: str, sleep_seconds: float) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return json.loads(path.read_text())
    response = sec_get(url, sleep_seconds)
    path.write_text(response.text, encoding="utf-8")
    return response.json()


def cached_text(path: Path, url: str, sleep_seconds: float) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path.read_text(encoding="utf-8")
    response = sec_get(url, sleep_seconds)
    path.write_text(response.text, encoding="utf-8")
    return response.text


def read_tickers_from_openinsider(path: Path) -> list[str]:
    rows = []
    with path.open() as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    frame = pd.DataFrame(rows)
    if "Ticker" not in frame.columns:
        raise ValueError(f"{path} must contain an OpenInsider `Ticker` column")
    return frame["Ticker"].dropna().astype(str).str.upper().value_counts().index.tolist()


def load_ticker_map(cache_dir: Path, sleep_seconds: float) -> pd.DataFrame:
    data = cached_json(
        cache_dir / "company_tickers.json",
        "https://www.sec.gov/files/company_tickers.json",
        sleep_seconds,
    )
    rows = []
    for item in data.values():
        rows.append(
            {
                "ticker": str(item["ticker"]).upper(),
                "company_title": item["title"],
                "cik": str(item["cik_str"]).zfill(10),
            }
        )
    return pd.DataFrame(rows)


def load_submission(cik: str, cache_dir: Path, sleep_seconds: float) -> dict[str, Any]:
    return cached_json(
        cache_dir / "submissions" / f"CIK{cik}.json",
        f"https://data.sec.gov/submissions/CIK{cik}.json",
        sleep_seconds,
    )


def recent_form4_filings(
    submission: dict[str, Any],
    ticker: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    max_filings: int | None,
) -> list[dict[str, Any]]:
    recent = submission.get("filings", {}).get("recent", {})
    rows = []
    forms = recent.get("form", [])
    for idx, form in enumerate(forms):
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
                "archive_base_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_path}",
            }
        )
        if max_filings is not None and len(rows) >= max_filings:
            break
    return rows


def find_ownership_xml_url(filing: dict[str, Any], cache_dir: Path, sleep_seconds: float) -> str:
    primary = str(filing["primary_document"])
    if primary.lower().endswith(".xml") and "/" not in primary:
        return f"{filing['archive_base_url']}/{primary}"

    index_url = f"{filing['archive_base_url']}/index.json"
    index_path = cache_dir / "filing_indexes" / str(filing["cik"]) / f"{filing['accession']}.json"
    index = cached_json(index_path, index_url, sleep_seconds)
    xml_names = [
        item.get("name", "")
        for item in index.get("directory", {}).get("item", [])
        if item.get("name", "").lower().endswith(".xml")
    ]
    if not xml_names:
        return f"{filing['archive_base_url']}/{primary}"
    # Ownership filings usually have one actual XML document; avoid xsl paths.
    preferred = [name for name in xml_names if "/" not in name]
    return f"{filing['archive_base_url']}/{(preferred or xml_names)[0]}"


def parse_filing(filing: dict[str, Any], cache_dir: Path, sleep_seconds: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        xml_url = find_ownership_xml_url(filing, cache_dir, sleep_seconds)
        xml_path = cache_dir / "form4_xml" / str(filing["cik"]) / f"{filing['accession']}.xml"
        xml_text = cached_text(xml_path, xml_url, sleep_seconds)
        records = parse_form4_xml(
            xml_text,
            accepted_at=str(filing["accepted_at"]),
            accession=filing["accession"],
            cik=filing["cik"],
            ticker=filing["ticker"],
        )
        return records, {
            **filing,
            "xml_url": xml_url,
            "sha256": hashlib.sha256(xml_text.encode()).hexdigest(),
            "status": "parsed",
            "parsed_records": len(records),
        }
    except Exception as exc:  # noqa: BLE001 - manifest must preserve exact failure
        return [], {
            **filing,
            "xml_url": None,
            "sha256": None,
            "status": f"error:{type(exc).__name__}:{exc}",
            "parsed_records": 0,
        }


def owner_names(owners: object) -> str:
    if not isinstance(owners, list):
        return ""
    names = [str(owner.get("name")) for owner in owners if owner.get("name")]
    return "; ".join(sorted(set(names)))


def serialize_value(value: object) -> object:
    if isinstance(value, list):
        return json.dumps(value)
    return value


def build_eligible(raw: pd.DataFrame, min_purchase_value: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    if raw.empty:
        return pd.DataFrame(), pd.DataFrame()
    frame = raw.copy()
    for col in ["shares", "price_per_share", "purchase_value"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["filing_datetime"] = pd.to_datetime(frame["accepted_at"], errors="coerce")
    frame["insider_name"] = frame["reporting_owners"].map(owner_names)
    frame["company_name"] = frame["issuer_name"]

    keep = (
        (frame["eligible_insider"] == True)  # noqa: E712
        & (frame["purchase_value"] >= min_purchase_value)
        & frame["filing_datetime"].notna()
        & frame["ticker"].notna()
        & frame["insider_name"].ne("")
    )
    eligible = frame[keep].copy()
    dropped = frame[~keep].copy()
    dropped["drop_reason"] = "unknown"
    dropped.loc[dropped["eligible_insider"] != True, "drop_reason"] = "ineligible_owner"  # noqa: E712
    dropped.loc[dropped["purchase_value"] < min_purchase_value, "drop_reason"] = "below_min_purchase_value"
    dropped.loc[dropped["purchase_value"].isna(), "drop_reason"] = "missing_purchase_value"
    return eligible, dropped


def run(config: Config) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    tickers = (
        [ticker.upper() for ticker in config.tickers]
        if config.tickers
        else read_tickers_from_openinsider(config.tickers_from)
    )
    if config.max_tickers is not None:
        tickers = tickers[: config.max_tickers]

    ticker_map = load_ticker_map(config.cache_dir, config.sleep_seconds)
    mapped = ticker_map[ticker_map["ticker"].isin(tickers)].copy()
    missing_tickers = sorted(set(tickers) - set(mapped["ticker"]))

    filings = []
    ticker_manifest = []
    for count, row in enumerate(mapped.itertuples(index=False), start=1):
        print(f"{count}/{len(mapped)} {row.ticker}", flush=True)
        try:
            submission = load_submission(row.cik, config.cache_dir, config.sleep_seconds)
            ticker_filings = recent_form4_filings(
                submission,
                row.ticker,
                config.start,
                config.end,
                config.max_filings_per_ticker,
            )
            filings.extend(ticker_filings)
            ticker_manifest.append(
                {
                    "ticker": row.ticker,
                    "cik": row.cik,
                    "status": "ok",
                    "filings": len(ticker_filings),
                }
            )
        except Exception as exc:  # noqa: BLE001
            ticker_manifest.append(
                {
                    "ticker": row.ticker,
                    "cik": row.cik,
                    "status": f"error:{type(exc).__name__}:{exc}",
                    "filings": 0,
                }
            )
    for ticker in missing_tickers:
        ticker_manifest.append(
            {
                "ticker": ticker,
                "cik": None,
                "status": "missing_current_sec_ticker_map",
                "filings": 0,
            }
        )

    records = []
    filing_manifest = []

    def write_partial() -> None:
        raw_partial = pd.DataFrame(records)
        raw_for_csv = raw_partial.map(serialize_value) if not raw_partial.empty else raw_partial
        raw_for_csv.to_csv(config.output_dir / "raw_form4_transactions.partial.csv", index=False)
        pd.DataFrame(filing_manifest).to_csv(config.output_dir / "filings_manifest.partial.csv", index=False)

    for count, filing in enumerate(filings, start=1):
        if count % 100 == 0:
            print(f"parsed filings {count}/{len(filings)}", flush=True)
        parsed, manifest_row = parse_filing(filing, config.cache_dir, config.sleep_seconds)
        records.extend(parsed)
        filing_manifest.append(manifest_row)
        if count % 500 == 0:
            write_partial()
    write_partial()

    raw = pd.DataFrame(records)
    raw_for_csv = raw.map(serialize_value) if not raw.empty else raw
    raw_for_csv.to_csv(config.output_dir / "raw_form4_transactions.csv", index=False)
    pd.DataFrame(ticker_manifest).to_csv(config.output_dir / "ticker_manifest.csv", index=False)
    pd.DataFrame(filing_manifest).to_csv(config.output_dir / "filings_manifest.csv", index=False)

    eligible, dropped = build_eligible(raw, config.min_purchase_value)
    eligible.map(serialize_value).to_csv(config.output_dir / "eligible_owner_purchases.csv", index=False)
    dropped.map(serialize_value).to_csv(config.output_dir / "dropped_transactions.csv", index=False)

    if eligible.empty:
        clusters = pd.DataFrame()
    else:
        clusters = build_cluster_signals(
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
    clusters.to_csv(config.output_dir / "cluster_signals.csv", index=False)

    episode_features = eligible.copy()
    if not episode_features.empty:
        episode_features["purchase_value_rank"] = episode_features["purchase_value"].rank(pct=True)
    episode_features.map(serialize_value).to_csv(config.output_dir / "purchase_intensity_episodes.csv", index=False)

    diagnostics = [
        "# SEC Signal Database Diagnostics",
        "",
        f"Created at: `{datetime.now(UTC).isoformat()}`",
        "",
        "This is a signal database only. It does not make return claims and does not solve delisting or historical ticker mapping.",
        "",
        f"- Tickers requested: {len(tickers)}",
        f"- Tickers mapped with current SEC map: {len(mapped)}",
        f"- Missing current ticker map: {len(missing_tickers)}",
        f"- Form 4/4-A filings discovered: {len(filings)}",
        f"- Filing records parsed: {(pd.DataFrame(filing_manifest).get('status') == 'parsed').sum() if filing_manifest else 0}",
        f"- Raw P/A transactions parsed: {len(raw)}",
        f"- Eligible owner purchases: {len(eligible)}",
        f"- Dropped transactions: {len(dropped)}",
        f"- Cluster signals: {len(clusters)}",
        "",
    ]
    (config.output_dir / "diagnostics.md").write_text("\n".join(diagnostics), encoding="utf-8")
    (config.output_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "created_at": datetime.now(UTC).isoformat(),
                "mode": "signal_database_only",
                "start": str(config.start.date()),
                "end": str(config.end.date()),
                "max_tickers": config.max_tickers,
                "max_filings_per_ticker": config.max_filings_per_ticker,
                "min_purchase_value": config.min_purchase_value,
                "min_cluster_value": config.min_cluster_value,
                "cluster_window_trading_days": config.cluster_window_trading_days,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print((config.output_dir / "diagnostics.md").read_text())


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Build cached SEC Form 4 signal database.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/sec_signals"))
    parser.add_argument("--cache-dir", type=Path, default=Path("data/cache/sec"))
    parser.add_argument("--start", default="2021-04-25")
    parser.add_argument("--end", default="2026-04-25")
    parser.add_argument("--tickers-from", type=Path, default=Path("data/raw/openinsider_3y.jsonl"))
    parser.add_argument("--tickers", nargs="*", default=None)
    parser.add_argument("--max-tickers", type=int, default=50)
    parser.add_argument("--max-filings-per-ticker", type=int, default=500)
    parser.add_argument("--sleep-seconds", type=float, default=0.12)
    parser.add_argument("--min-purchase-value", type=float, default=100_000)
    parser.add_argument("--min-cluster-value", type=float, default=250_000)
    parser.add_argument("--cluster-window-trading-days", type=int, default=5)
    args = parser.parse_args()
    return Config(
        output_dir=args.output_dir,
        cache_dir=args.cache_dir,
        start=pd.Timestamp(args.start),
        end=pd.Timestamp(args.end),
        tickers_from=args.tickers_from,
        tickers=args.tickers,
        max_tickers=args.max_tickers,
        max_filings_per_ticker=args.max_filings_per_ticker,
        sleep_seconds=args.sleep_seconds,
        min_purchase_value=args.min_purchase_value,
        min_cluster_value=args.min_cluster_value,
        cluster_window_trading_days=args.cluster_window_trading_days,
    )


if __name__ == "__main__":
    run(parse_args())
