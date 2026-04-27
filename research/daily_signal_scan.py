from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree

import pandas as pd
import requests
import yfinance as yf

from discord_send import discord_send
from research.alerting import apply_scores, format_alert_message, format_daily_digest
from matrix_send import matrix_send
from research.sec_signal_database import SEC_HEADERS, build_eligible, parse_filing, serialize_value


ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


@dataclass(frozen=True)
class ScanConfig:
    output_dir: Path
    cache_dir: Path
    state_file: Path
    lookback_hours: int
    max_entries: int
    sleep_seconds: float
    min_purchase_value: float
    min_adv60_ratio: float
    alert_adv60_ratio: float
    max_price_premium: float
    notify: bool
    notify_discord: bool


def sec_get_text(url: str, sleep_seconds: float) -> str:
    time.sleep(sleep_seconds)
    response = requests.get(url, headers=SEC_HEADERS, timeout=60)
    response.raise_for_status()
    return response.text


def parse_atom_entries(atom_text: str) -> list[dict[str, Any]]:
    root = ElementTree.fromstring(atom_text)
    entries = []
    for entry in root.findall("atom:entry", ATOM_NS):
        title = _atom_text(entry, "title")
        link = entry.find("atom:link", ATOM_NS)
        href = link.attrib.get("href") if link is not None else ""
        updated = pd.to_datetime(_atom_text(entry, "updated"), utc=True, errors="coerce")
        category = entry.find("atom:category", ATOM_NS)
        form_type = category.attrib.get("term") if category is not None else ""
        accession = ""
        summary = _atom_text(entry, "summary")
        match = re.search(r"AccNo:\s*</?[^>]*>\s*([0-9-]+)|AccNo:\s*([0-9-]+)", summary)
        if match:
            accession = next(group for group in match.groups() if group)
        if not accession:
            accession = _atom_text(entry, "id").rsplit("=", maxsplit=1)[-1].rsplit(":", maxsplit=1)[-1]

        archive_base_url, cik = archive_base_from_index_url(href)
        entries.append(
            {
                "title": title,
                "form_type": form_type,
                "accepted_at": updated.tz_convert(None) if not pd.isna(updated) else pd.NaT,
                "accession": accession,
                "cik": cik,
                "company_name": title,
                "ticker": None,
                "primary_document": "",
                "archive_base_url": archive_base_url,
                "source_url": href,
                "amendment_flag": form_type == "4/A",
            }
        )
    return entries


def archive_base_from_index_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 6:
        return url.rsplit("/", maxsplit=1)[0], ""
    cik = str(int(parts[3])).zfill(10)
    base_path = "/" + "/".join(parts[:-1])
    return f"{parsed.scheme}://{parsed.netloc}{base_path}", cik


def fetch_recent_form4_feed(config: ScanConfig) -> pd.DataFrame:
    cutoff = pd.Timestamp(datetime.now(UTC) - timedelta(hours=config.lookback_hours)).tz_convert(None)
    rows = []
    start = 0
    page_size = 100
    while len(rows) < config.max_entries:
        url = (
            "https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcurrent&type=4&owner=include&count={page_size}&start={start}&output=atom"
        )
        page_rows = parse_atom_entries(sec_get_text(url, config.sleep_seconds))
        if not page_rows:
            break
        rows.extend(row for row in page_rows if row["form_type"] in {"4", "4/A"})
        oldest = min((row["accepted_at"] for row in page_rows if not pd.isna(row["accepted_at"])), default=pd.NaT)
        if pd.notna(oldest) and oldest < cutoff:
            break
        start += page_size
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame = frame[frame["accepted_at"] >= cutoff].copy()
    return frame.drop_duplicates("accession").head(config.max_entries)


def record_key(row: pd.Series) -> str:
    owners = row.get("reporting_owners")
    if isinstance(owners, list):
        owner_value = json.dumps(owners, sort_keys=True)
    else:
        owner_value = str(owners)
    value = "|".join(
        str(row.get(col, ""))
        for col in ["accession", "issuer_cik", "ticker", "transaction_date", "security_title", "shares", "price_per_share"]
    )
    return hashlib.sha256(f"{value}|{owner_value}".encode()).hexdigest()


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def append_jsonl_unique(path: Path, rows: list[dict[str, Any]], key_col: str) -> list[dict[str, Any]]:
    existing = read_jsonl(path)
    seen = {str(row.get(key_col)) for row in existing}
    appended = []
    for row in rows:
        key = str(row.get(key_col))
        if key in seen:
            continue
        existing.append(row)
        appended.append(row)
        seen.add(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in existing:
            handle.write(json.dumps(json_ready(row), sort_keys=True) + "\n")
    return appended


def json_ready(row: dict[str, Any]) -> dict[str, Any]:
    cleaned = {}
    for key, value in row.items():
        if pd.isna(value) if not isinstance(value, (list, dict)) else False:
            cleaned[key] = None
        elif isinstance(value, pd.Timestamp):
            cleaned[key] = value.isoformat()
        else:
            cleaned[key] = value
    return cleaned


def append_unique(existing: pd.DataFrame, new: pd.DataFrame, key_col: str) -> pd.DataFrame:
    if existing.empty:
        return new.copy()
    if new.empty:
        return existing.copy()
    combined = pd.concat([existing, new], ignore_index=True)
    return combined.drop_duplicates(key_col, keep="last")


def enrich_with_market_data(eligible: pd.DataFrame) -> pd.DataFrame:
    if eligible.empty:
        return eligible
    enriched = eligible.copy()
    tickers = sorted(ticker for ticker in enriched["ticker"].dropna().astype(str).str.upper().unique() if ticker)
    market_rows = {}
    for ticker in tickers:
        try:
            prices = yf.download(ticker, period="1y", interval="1d", auto_adjust=True, progress=False)
            if prices.empty:
                continue
            close_series = price_series(prices, "Close")
            volume_series = price_series(prices, "Volume")
            close = _last_scalar(close_series)
            high_52w = _last_scalar(close_series.max()) if close_series is not None else None
            dollar_volume = (close_series * volume_series).tail(60).mean() if close_series is not None and volume_series is not None else None
            market_cap = None
            try:
                market_cap = yf.Ticker(ticker).fast_info.get("market_cap")
            except Exception:  # noqa: BLE001 - market cap is auxiliary
                market_cap = None
            market_rows[ticker] = {
                "latest_close": close,
                "high_52w": high_52w,
                "drawdown_from_52w_high": close / high_52w - 1 if close and high_52w else None,
                "adv60_dollars": _last_scalar(dollar_volume),
                "current_market_cap": market_cap,
            }
        except Exception:  # noqa: BLE001 - preserve candidate rows even when Yahoo fails
            continue

    for col in ["latest_close", "high_52w", "drawdown_from_52w_high", "adv60_dollars", "current_market_cap"]:
        enriched[col] = enriched["ticker"].map(lambda ticker: market_rows.get(str(ticker).upper(), {}).get(col))
    enriched["signal_value_to_adv60"] = enriched["purchase_value"] / enriched["adv60_dollars"]
    enriched["current_price_premium_to_insider_vwap"] = enriched["latest_close"] / enriched["price_per_share"] - 1
    enriched["purchase_to_market_cap"] = enriched["purchase_value"] / enriched["current_market_cap"]
    return enriched


def price_series(prices: pd.DataFrame, column: str) -> pd.Series | None:
    if column not in prices:
        return None
    values = prices[column]
    if isinstance(values, pd.DataFrame):
        if values.empty:
            return None
        values = values.iloc[:, 0]
    return values.dropna()


def _last_scalar(value: Any) -> float | None:
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return None
        value = value.iloc[:, 0]
    if isinstance(value, pd.Series):
        if value.empty:
            return None
        value = value.iloc[-1]
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def score_candidates(enriched: pd.DataFrame, config: ScanConfig) -> pd.DataFrame:
    if enriched.empty:
        return enriched
    candidates = enriched.copy()
    candidates["passes_adv60_monitor"] = candidates["signal_value_to_adv60"] >= config.min_adv60_ratio
    candidates["passes_adv60_alert"] = candidates["signal_value_to_adv60"] >= config.alert_adv60_ratio
    candidates["passes_price_premium"] = (
        candidates["current_price_premium_to_insider_vwap"].notna()
        & (candidates["current_price_premium_to_insider_vwap"] <= config.max_price_premium)
    )
    candidates["monitor_candidate"] = (
        candidates["passes_price_premium"]
        & (
            candidates["passes_adv60_monitor"]
            | (candidates["purchase_value"] >= 1_000_000)
        )
    )
    candidates["alert_candidate"] = candidates["monitor_candidate"] & (
        candidates["passes_adv60_alert"] | (candidates["purchase_value"] >= 2_000_000)
    )
    return candidates.sort_values(["alert_candidate", "signal_value_to_adv60", "purchase_value"], ascending=False)


def build_candidate_episodes(scored: pd.DataFrame, config: ScanConfig) -> pd.DataFrame:
    if scored.empty:
        return scored.copy()
    candidates = scored[scored["monitor_candidate"] == True].copy()
    if candidates.empty:
        return candidates

    group_cols = ["accession", "ticker", "issuer_name", "insider_name", "filing_datetime"]
    episodes = (
        candidates.groupby(group_cols, dropna=False)
        .agg(
            cik=("cik", "first"),
            purchase_value=("purchase_value", "sum"),
            shares=("shares", "sum"),
            latest_close=("latest_close", "first"),
            high_52w=("high_52w", "first"),
            drawdown_from_52w_high=("drawdown_from_52w_high", "first"),
            adv60_dollars=("adv60_dollars", "first"),
            current_market_cap=("current_market_cap", "first"),
            transaction_rows=("record_key", "count"),
        )
        .reset_index()
    )
    episodes["insider_vwap"] = episodes["purchase_value"] / episodes["shares"]
    episodes["signal_value_to_adv60"] = episodes["purchase_value"] / episodes["adv60_dollars"]
    episodes["current_price_premium_to_insider_vwap"] = episodes["latest_close"] / episodes["insider_vwap"] - 1
    episodes["purchase_to_market_cap"] = episodes["purchase_value"] / episodes["current_market_cap"]
    episodes["passes_adv60_monitor"] = episodes["signal_value_to_adv60"] >= config.min_adv60_ratio
    episodes["passes_adv60_alert"] = episodes["signal_value_to_adv60"] >= config.alert_adv60_ratio
    episodes["passes_price_premium"] = (
        episodes["current_price_premium_to_insider_vwap"].notna()
        & (episodes["current_price_premium_to_insider_vwap"] <= config.max_price_premium)
    )
    episodes["monitor_candidate"] = (
        episodes["passes_price_premium"]
        & (
            episodes["passes_adv60_monitor"]
            | (episodes["purchase_value"] >= 1_000_000)
        )
    )
    episodes["alert_candidate"] = episodes["monitor_candidate"] & (
        episodes["passes_adv60_alert"] | (episodes["purchase_value"] >= 2_000_000)
    )
    episodes["episode_key"] = episodes.apply(
        lambda row: hashlib.sha256(
            f"{row['accession']}|{row['ticker']}|{row['insider_name']}|{row['filing_datetime']}".encode()
        ).hexdigest(),
        axis=1,
    )
    return episodes.sort_values(["alert_candidate", "signal_value_to_adv60", "purchase_value"], ascending=False)


def format_candidate_message(row: pd.Series) -> str:
    return "\n".join(
        [
            f"Insider buy candidate: {row.get('ticker')} / {row.get('issuer_name')}",
            f"Insider: {row.get('insider_name')}",
            f"Filed: {row.get('filing_datetime')}",
            f"Value: ${float(row.get('purchase_value', 0)):,.0f}",
            f"Value/ADV60: {float(row.get('signal_value_to_adv60', 0)) * 100:.1f}%",
            f"Price premium to insider VWAP: {float(row.get('current_price_premium_to_insider_vwap', 0)) * 100:.1f}%",
            f"Accession: {row.get('accession')}",
            f"https://www.sec.gov/Archives/edgar/data/{int(row.get('cik'))}/{str(row.get('accession')).replace('-', '')}/",
            f"https://finance.yahoo.com/quote/{row.get('ticker')}",
        ]
    )


def run(config: ScanConfig) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    filings = fetch_recent_form4_feed(config)
    filings.to_csv(config.output_dir / "latest_form4_filings.csv", index=False)

    records = []
    manifest = []
    for filing in filings.to_dict("records"):
        parsed, manifest_row = parse_filing(filing, config.cache_dir, config.sleep_seconds)
        records.extend(parsed)
        manifest.append(manifest_row)

    raw = pd.DataFrame(records)
    if not raw.empty:
        raw["ticker"] = raw["ticker"].fillna(raw["issuer_trading_symbol"]).astype(str).str.upper()
        raw["record_key"] = raw.apply(record_key, axis=1)
    raw_for_csv = raw.map(serialize_value) if not raw.empty else raw
    raw_for_csv.to_csv(config.output_dir / "raw_form4_transactions_latest.csv", index=False)
    pd.DataFrame(manifest).to_csv(config.output_dir / "filings_manifest_latest.csv", index=False)

    eligible, dropped = build_eligible(raw, config.min_purchase_value)
    if not eligible.empty:
        eligible["record_key"] = eligible.apply(record_key, axis=1)
    eligible = enrich_with_market_data(eligible)
    scored = score_candidates(eligible, config)
    scored = apply_scores(scored)
    latest_episodes = build_candidate_episodes(scored, config)
    latest_episodes = apply_scores(latest_episodes)

    existing_history = load_csv(config.output_dir / "eligible_purchase_history.csv")
    history = append_unique(existing_history, scored, "record_key")
    history.map(serialize_value).to_csv(config.output_dir / "eligible_purchase_history.csv", index=False)

    latest_candidates = scored[scored.get("monitor_candidate", False) == True].copy() if not scored.empty else scored
    existing_candidates = load_csv(config.output_dir / "candidate_history.csv")
    candidate_history = append_unique(existing_candidates, latest_candidates, "record_key")
    candidate_history = apply_scores(candidate_history)
    candidate_history.map(serialize_value).to_csv(config.output_dir / "candidate_history.csv", index=False)
    latest_candidates.map(serialize_value).to_csv(config.output_dir / "watchlist_candidates_latest.csv", index=False)

    existing_episodes = load_csv(config.output_dir / "candidate_episode_history.csv")
    episode_history = append_unique(existing_episodes, latest_episodes, "episode_key")
    episode_history = apply_scores(episode_history)
    episode_history.map(serialize_value).to_csv(config.output_dir / "candidate_episode_history.csv", index=False)
    latest_episodes.map(serialize_value).to_csv(config.output_dir / "watchlist_episodes_latest.csv", index=False)
    dropped.map(serialize_value).to_csv(config.output_dir / "dropped_transactions_latest.csv", index=False)

    previous_episode_keys = set(existing_episodes.get("episode_key", pd.Series(dtype=str)).astype(str))
    new_notifications = latest_episodes[
        (latest_episodes.get("research_tier", "").isin(["ALERT", "WATCH"]))
        & ~latest_episodes["episode_key"].astype(str).isin(previous_episode_keys)
    ].copy() if not latest_episodes.empty else latest_episodes
    if config.notify and not new_notifications.empty:
        loop = asyncio.get_event_loop()
        for _, row in new_notifications.iterrows():
            loop.run_until_complete(matrix_send(format_alert_message(row)))
    if config.notify_discord and not new_notifications.empty:
        for _, row in new_notifications.iterrows():
            discord_send(format_alert_message(row))

    alert_rows = []
    new_alert_rows = new_notifications.iterrows() if not new_notifications.empty else []
    for _, row in new_alert_rows:
        payload = row.to_dict()
        payload["alert_key"] = f"{payload.get('episode_key')}|{payload.get('research_tier')}|{payload.get('research_score')}"
        payload["alert_created_at"] = datetime.now(UTC).isoformat()
        alert_rows.append(payload)
    appended_alerts = append_jsonl_unique(config.output_dir / "alert_history.jsonl", alert_rows, "alert_key")

    alert_messages = []
    alert_rows = (
        latest_episodes[latest_episodes["research_tier"].isin(["ALERT", "WATCH"])].iterrows()
        if not latest_episodes.empty
        else []
    )
    for _, row in alert_rows:
        alert_messages.append(format_alert_message(row))
    (config.output_dir / "alert_messages_latest.md").write_text("\n\n---\n\n".join(alert_messages), encoding="utf-8")
    (config.output_dir / "daily_digest.md").write_text(
        format_daily_digest(
            latest_episodes,
            lookback_hours=config.lookback_hours,
            filings=len(filings),
            raw_transactions=len(raw),
        ),
        encoding="utf-8",
    )
    append_jsonl_unique(
        config.output_dir / "daily_runs.jsonl",
        [
            {
                "run_key": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "created_at": datetime.now(UTC).isoformat(),
                "lookback_hours": config.lookback_hours,
                "filings": len(filings),
                "raw_transactions": len(raw),
                "eligible_purchases": len(eligible),
                "monitor_candidates": len(latest_candidates),
                "monitor_episodes": len(latest_episodes),
                "new_notifications": len(appended_alerts),
            }
        ],
        "run_key",
    )

    diagnostics = [
        "# Daily SEC Insider Signal Scan",
        "",
        f"Created at: `{datetime.now(UTC).isoformat()}`",
        f"- Lookback hours: {config.lookback_hours}",
        f"- Form 4/4-A filings discovered: {len(filings)}",
        f"- Raw P/A transactions parsed: {len(raw)}",
        f"- Eligible purchases >= ${config.min_purchase_value:,.0f}: {len(eligible)}",
        f"- Monitor candidates: {len(latest_candidates)}",
        f"- Monitor episodes: {len(latest_episodes)}",
        f"- ALERT-tier episodes: {(latest_episodes.get('research_tier', pd.Series(dtype=str)) == 'ALERT').sum() if not latest_episodes.empty else 0}",
        f"- WATCH-tier episodes: {(latest_episodes.get('research_tier', pd.Series(dtype=str)) == 'WATCH').sum() if not latest_episodes.empty else 0}",
        f"- New ALERT/WATCH notifications: {len(new_notifications)}",
        f"- Historical eligible purchases: {len(history)}",
        f"- Historical candidates: {len(candidate_history)}",
        f"- Historical candidate episodes: {len(episode_history)}",
        "",
        "Monitor rule: eligible Form 4 code-P acquisition by officer/director, "
        f"value >= ${config.min_purchase_value:,.0f}, current price no more than "
        f"{config.max_price_premium:.0%} above insider VWAP, and either value/ADV60 "
        f">= {config.min_adv60_ratio:.0%} or value >= $1,000,000.",
    ]
    (config.output_dir / "daily_diagnostics.md").write_text("\n".join(diagnostics), encoding="utf-8")
    config.state_file.parent.mkdir(parents=True, exist_ok=True)
    config.state_file.write_text(
        json.dumps(
            {
                "last_run_at": datetime.now(UTC).isoformat(),
                "latest_accessions": sorted(filings.get("accession", pd.Series(dtype=str)).astype(str).tolist()),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print("\n".join(diagnostics))


def _atom_text(entry: ElementTree.Element, tag: str) -> str:
    child = entry.find(f"atom:{tag}", ATOM_NS)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def parse_args() -> ScanConfig:
    parser = argparse.ArgumentParser(description="Run a daily SEC Form 4 insider-buy signal scan.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/live/sec_daily"))
    parser.add_argument("--cache-dir", type=Path, default=Path("data/cache/sec"))
    parser.add_argument("--state-file", type=Path, default=Path("data/live/sec_daily/state.json"))
    parser.add_argument("--lookback-hours", type=int, default=72)
    parser.add_argument("--max-entries", type=int, default=500)
    parser.add_argument("--sleep-seconds", type=float, default=0.12)
    parser.add_argument("--min-purchase-value", type=float, default=100_000)
    parser.add_argument("--min-adv60-ratio", type=float, default=0.02)
    parser.add_argument("--alert-adv60-ratio", type=float, default=0.05)
    parser.add_argument("--max-price-premium", type=float, default=0.15)
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--notify-discord", action="store_true")
    args = parser.parse_args()
    return ScanConfig(
        output_dir=args.output_dir,
        cache_dir=args.cache_dir,
        state_file=args.state_file,
        lookback_hours=args.lookback_hours,
        max_entries=args.max_entries,
        sleep_seconds=args.sleep_seconds,
        min_purchase_value=args.min_purchase_value,
        min_adv60_ratio=args.min_adv60_ratio,
        alert_adv60_ratio=args.alert_adv60_ratio,
        max_price_premium=args.max_price_premium,
        notify=args.notify,
        notify_discord=args.notify_discord,
    )


if __name__ == "__main__":
    run(parse_args())
