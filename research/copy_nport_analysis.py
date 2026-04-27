from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import pandas as pd
import requests

from research.sec_signal_database import SEC_HEADERS


RBB_CIK = "0001618627"
COPY_SERIES_NAME = "Tweedy, Browne Insider + Value ETF"


@dataclass(frozen=True)
class Config:
    output_dir: Path
    cache_dir: Path
    sleep_seconds: float


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


def cached_json(path: Path, url: str, sleep_seconds: float) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return json.loads(path.read_text())
    data = sec_get_json(url, sleep_seconds)
    path.write_text(json.dumps(data), encoding="utf-8")
    return data


def cached_text(path: Path, url: str, sleep_seconds: float) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path.read_text(encoding="utf-8")
    text = sec_get_text(url, sleep_seconds)
    path.write_text(text, encoding="utf-8")
    return text


def local_name(tag: str) -> str:
    return tag.rsplit("}", maxsplit=1)[-1]


def child_text(element: ElementTree.Element | None, tag: str) -> str | None:
    if element is None:
        return None
    for candidate in element.iter():
        if local_name(candidate.tag) == tag:
            return candidate.text.strip() if candidate.text else None
    return None


def direct_child_text(element: ElementTree.Element, tag: str) -> str | None:
    for child in list(element):
        if local_name(child.tag) == tag:
            return child.text.strip() if child.text else None
    return None


def to_float(value: str | None) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def nport_filings(cache_dir: Path, sleep_seconds: float) -> pd.DataFrame:
    submission = cached_json(
        cache_dir / "submissions" / f"CIK{RBB_CIK}.json",
        f"https://data.sec.gov/submissions/CIK{RBB_CIK}.json",
        sleep_seconds,
    )
    recent = submission.get("filings", {}).get("recent", {})
    rows = []
    for idx, form in enumerate(recent.get("form", [])):
        if not str(form).startswith("NPORT-P"):
            continue
        accession = recent["accessionNumber"][idx]
        accession_path = accession.replace("-", "")
        xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(RBB_CIK)}/{accession_path}/primary_doc.xml"
        xml_text = cached_text(cache_dir / "copy_nport_xml" / f"{accession}.xml", xml_url, sleep_seconds)
        root = ElementTree.fromstring(xml_text)
        series_name = child_text(root, "seriesName")
        if series_name != COPY_SERIES_NAME:
            continue
        rows.append(
            {
                "form": form,
                "filing_date": recent["filingDate"][idx],
                "report_date": recent.get("reportDate", [""])[idx],
                "accession": accession,
                "xml_url": xml_url,
                "series_name": series_name,
                "is_amendment": str(form).endswith("/A"),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["filing_date"] = pd.to_datetime(frame["filing_date"])
    frame["report_date"] = pd.to_datetime(frame["report_date"])
    frame = frame.sort_values(["report_date", "filing_date", "is_amendment", "accession"])
    return frame.drop_duplicates("report_date", keep="last")


def parse_holdings(xml_text: str, filing: pd.Series) -> pd.DataFrame:
    root = ElementTree.fromstring(xml_text)
    rows = []
    for investment in root.iter():
        if local_name(investment.tag) != "invstOrSec":
            continue
        identifiers = next((child for child in list(investment) if local_name(child.tag) == "identifiers"), None)
        rows.append(
            {
                "report_date": filing["report_date"],
                "filing_date": filing["filing_date"],
                "accession": filing["accession"],
                "name": direct_child_text(investment, "name"),
                "title": direct_child_text(investment, "title"),
                "lei": direct_child_text(investment, "lei"),
                "cusip": direct_child_text(investment, "cusip"),
                "isin": child_text(identifiers, "isin"),
                "ticker": child_text(identifiers, "ticker"),
                "balance": to_float(direct_child_text(investment, "balance")),
                "units": direct_child_text(investment, "units"),
                "currency": direct_child_text(investment, "curCd"),
                "value_usd": to_float(direct_child_text(investment, "valUSD")),
                "pct_value": to_float(direct_child_text(investment, "pctVal")),
                "payoff_profile": direct_child_text(investment, "payoffProfile"),
                "asset_category": direct_child_text(investment, "assetCat"),
                "issuer_category": direct_child_text(investment, "issuerCat"),
                "country": direct_child_text(investment, "invCountry"),
                "fair_value_level": direct_child_text(investment, "fairValLevel"),
                "restricted": direct_child_text(investment, "isRestrictedSec"),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["holding_key"] = frame.apply(holding_key, axis=1)
    return frame


def holding_key(row: pd.Series) -> str:
    for column in ["isin", "cusip", "lei"]:
        value = row.get(column)
        if isinstance(value, str) and value and value != "N/A":
            return f"{column}:{value}"
    return f"name:{row.get('name')}|country:{row.get('country')}|asset:{row.get('asset_category')}"


def infer_changes(holdings: pd.DataFrame) -> pd.DataFrame:
    if holdings.empty:
        return holdings
    frames = []
    dates = sorted(holdings["report_date"].dropna().unique())
    for idx, date in enumerate(dates):
        current = holdings[holdings["report_date"] == date].copy()
        previous = (
            holdings[holdings["report_date"] == dates[idx - 1]].copy()
            if idx > 0
            else pd.DataFrame(columns=current.columns)
        )
        merged = current.merge(
            previous[["holding_key", "balance", "value_usd", "pct_value"]],
            on="holding_key",
            how="left",
            suffixes=("", "_prev"),
        )
        merged["change_type"] = "held"
        merged.loc[merged["balance_prev"].isna(), "change_type"] = "new"
        merged["balance_change"] = merged["balance"] - merged["balance_prev"]
        merged["value_change_usd"] = merged["value_usd"] - merged["value_usd_prev"]
        merged["pct_value_change"] = merged["pct_value"] - merged["pct_value_prev"]
        merged.loc[(merged["change_type"] == "held") & (merged["pct_value_change"] > 0.15), "change_type"] = "active_add"
        merged.loc[(merged["change_type"] == "held") & (merged["pct_value_change"] < -0.15), "change_type"] = "active_trim"
        frames.append(merged)

        if idx > 0:
            exits = previous[~previous["holding_key"].isin(set(current["holding_key"]))].copy()
            exits["report_date"] = date
            exits["change_type"] = "exit"
            exits["balance_prev"] = exits["balance"]
            exits["value_usd_prev"] = exits["value_usd"]
            exits["pct_value_prev"] = exits["pct_value"]
            exits["balance"] = 0.0
            exits["value_usd"] = 0.0
            exits["pct_value"] = 0.0
            exits["balance_change"] = -exits["balance_prev"]
            exits["value_change_usd"] = -exits["value_usd_prev"]
            exits["pct_value_change"] = -exits["pct_value_prev"]
            frames.append(exits)
    return pd.concat(frames, ignore_index=True)


def summarize(holdings: pd.DataFrame, changes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    by_date = (
        holdings.groupby("report_date")
        .agg(
            holdings=("holding_key", "nunique"),
            equities=("asset_category", lambda values: (values == "EC").sum()),
            preferred=("asset_category", lambda values: (values == "EP").sum()),
            cash_like=("asset_category", lambda values: (values == "STIV").sum()),
            total_value_usd=("value_usd", "sum"),
            top10_weight=("pct_value", lambda values: values.sort_values(ascending=False).head(10).sum()),
            median_weight=("pct_value", "median"),
        )
        .reset_index()
    )
    by_country = (
        holdings[holdings["asset_category"].isin(["EC", "EP"])]
        .groupby(["report_date", "country"])
        .agg(weight=("pct_value", "sum"), holdings=("holding_key", "nunique"))
        .reset_index()
        .sort_values(["report_date", "weight"], ascending=[True, False])
    )
    by_change = (
        changes.groupby(["report_date", "change_type"])
        .agg(count=("holding_key", "nunique"), value_usd=("value_usd", "sum"), value_change_usd=("value_change_usd", "sum"))
        .reset_index()
    )
    return by_date, by_country, by_change


def write_report(
    filings: pd.DataFrame,
    holdings: pd.DataFrame,
    changes: pd.DataFrame,
    by_date: pd.DataFrame,
    by_country: pd.DataFrame,
    by_change: pd.DataFrame,
    output_dir: Path,
) -> None:
    lines = [
        "# COPY N-PORT Holdings Inference",
        "",
        f"Created at: `{datetime.now(UTC).isoformat()}`",
        "",
        "This uses official SEC N-PORT filings for RBB Fund Trust series "
        "`Tweedy, Browne Insider + Value ETF`. These are delayed portfolio "
        "snapshots, not actual order tickets.",
        "",
        "## Filing Snapshots",
        "",
        "| Report Date | Filing Date | Form | Accession | Holdings | Value |",
        "| --- | --- | --- | --- | ---: | ---: |",
    ]
    counts = holdings.groupby("report_date").agg(holdings=("holding_key", "nunique"), value_usd=("value_usd", "sum"))
    for row in filings.sort_values("report_date").itertuples(index=False):
        count = counts.loc[row.report_date]
        lines.append(
            f"| {row.report_date.date()} | {row.filing_date.date()} | {row.form} | "
            f"`{row.accession}` | {int(count.holdings)} | ${count.value_usd:,.0f} |"
        )
    lines.extend(["", "## Portfolio Shape", ""])
    lines.extend(["| Report Date | Holdings | Equities | Total Value | Top 10 Weight | Median Weight |", "| --- | ---: | ---: | ---: | ---: | ---: |"])
    for row in by_date.itertuples(index=False):
        lines.append(
            f"| {row.report_date.date()} | {row.holdings} | {row.equities} | "
            f"${row.total_value_usd:,.0f} | {row.top10_weight:.1f}% | {row.median_weight:.2f}% |"
        )
    lines.extend(["", "## Change Counts", ""])
    pivot = by_change.pivot(index="report_date", columns="change_type", values="count").fillna(0).astype(int)
    lines.append("| Report Date | New | Active Adds | Active Trims | Exits | Held |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for date, row in pivot.iterrows():
        lines.append(
            f"| {pd.Timestamp(date).date()} | {row.get('new', 0)} | {row.get('active_add', 0)} | "
            f"{row.get('active_trim', 0)} | {row.get('exit', 0)} | {row.get('held', 0)} |"
        )
    lines.extend(["", "## Top Countries By Snapshot", ""])
    lines.append("| Report Date | Country | Weight | Holdings |")
    lines.append("| --- | --- | ---: | ---: |")
    for row in by_country.groupby("report_date").head(8).itertuples(index=False):
        lines.append(f"| {row.report_date.date()} | {row.country} | {row.weight:.1f}% | {row.holdings} |")
    lines.extend(["", "## Largest New Positions", ""])
    lines.append("| Report Date | Name | Country | Asset | Value | Weight |")
    lines.append("| --- | --- | --- | --- | ---: | ---: |")
    for row in (
        changes[changes["change_type"] == "new"]
        .sort_values(["report_date", "value_usd"], ascending=[True, False])
        .groupby("report_date")
        .head(15)
        .itertuples(index=False)
    ):
        lines.append(
            f"| {row.report_date.date()} | {row.name} | {row.country} | {row.asset_category} | "
            f"${row.value_usd:,.0f} | {row.pct_value:.2f}% |"
        )
    lines.extend(["", "## Largest Exits", ""])
    lines.append("| Report Date | Name | Country | Prior Value | Prior Weight |")
    lines.append("| --- | --- | --- | ---: | ---: |")
    exits = changes[changes["change_type"] == "exit"].copy()
    for row in exits.sort_values(["report_date", "value_usd_prev"], ascending=[True, False]).groupby("report_date").head(15).itertuples(index=False):
        lines.append(
            f"| {row.report_date.date()} | {row.name} | {row.country} | "
            f"${row.value_usd_prev:,.0f} | {row.pct_value_prev:.2f}% |"
        )
    lines.extend(["", "## Largest Active Weight Increases", ""])
    lines.append("| Report Date | Name | Country | Weight Change | Weight |")
    lines.append("| --- | --- | --- | ---: | ---: |")
    active_adds = changes[changes["change_type"] == "active_add"].copy()
    for row in active_adds.sort_values(["report_date", "pct_value_change"], ascending=[True, False]).groupby("report_date").head(15).itertuples(index=False):
        lines.append(
            f"| {row.report_date.date()} | {row.name} | {row.country} | "
            f"{row.pct_value_change:.2f}% | {row.pct_value:.2f}% |"
        )
    lines.extend(["", "## Largest Active Weight Decreases", ""])
    lines.append("| Report Date | Name | Country | Weight Change | Weight |")
    lines.append("| --- | --- | --- | ---: | ---: |")
    active_trims = changes[changes["change_type"] == "active_trim"].copy()
    for row in active_trims.sort_values(["report_date", "pct_value_change"], ascending=[True, True]).groupby("report_date").head(15).itertuples(index=False):
        lines.append(
            f"| {row.report_date.date()} | {row.name} | {row.country} | "
            f"{row.pct_value_change:.2f}% | {row.pct_value:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## Inferred Strategy Notes",
            "",
            "- The official archive shows a broad global value portfolio rather than a concentrated insider-buy clone.",
            "- Position weights are generally small; the median reported holding weight is well under 1%.",
            "- The portfolio has meaningful non-U.S. exposure and uses both common and preferred equity categories.",
            "- Large additions/exits between N-PORT snapshots can identify behavior, but not exact order timing.",
            "- Because public N-PORT is quarterly-delayed, daily holdings should be archived going forward for better inference.",
        ]
    )
    (output_dir / "copy_nport_report.md").write_text("\n".join(lines), encoding="utf-8")


def run(config: Config) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    filings = nport_filings(config.cache_dir, config.sleep_seconds)
    holding_frames = []
    for filing in filings.itertuples(index=False):
        xml_text = cached_text(
            config.cache_dir / "copy_nport_xml" / f"{filing.accession}.xml",
            filing.xml_url,
            config.sleep_seconds,
        )
        holding_frames.append(parse_holdings(xml_text, pd.Series(filing._asdict())))
    holdings = pd.concat(holding_frames, ignore_index=True) if holding_frames else pd.DataFrame()
    changes = infer_changes(holdings)
    by_date, by_country, by_change = summarize(holdings, changes)

    filings.to_csv(config.output_dir / "copy_nport_filings.csv", index=False)
    holdings.to_csv(config.output_dir / "copy_nport_holdings.csv", index=False)
    changes.to_csv(config.output_dir / "copy_nport_changes.csv", index=False)
    by_date.to_csv(config.output_dir / "copy_nport_summary_by_date.csv", index=False)
    by_country.to_csv(config.output_dir / "copy_nport_summary_by_country.csv", index=False)
    by_change.to_csv(config.output_dir / "copy_nport_summary_by_change.csv", index=False)
    write_report(filings, holdings, changes, by_date, by_country, by_change, config.output_dir)
    print((config.output_dir / "copy_nport_report.md").read_text())


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Infer COPY ETF behavior from official N-PORT holdings.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/results/copy_nport"))
    parser.add_argument("--cache-dir", type=Path, default=Path("data/cache/sec"))
    parser.add_argument("--sleep-seconds", type=float, default=0.12)
    args = parser.parse_args()
    return Config(output_dir=args.output_dir, cache_dir=args.cache_dir, sleep_seconds=args.sleep_seconds)


if __name__ == "__main__":
    run(parse_args())
