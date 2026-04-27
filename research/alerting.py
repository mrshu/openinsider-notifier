from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class ScoreResult:
    score: int
    tier: str
    reasons: list[str]
    caveats: list[str]


def score_insider_candidate(row: pd.Series) -> ScoreResult:
    score = 0
    reasons: list[str] = []
    caveats: list[str] = []

    purchase_value = number(row.get("purchase_value"))
    value_to_adv60 = number(row.get("signal_value_to_adv60"))
    premium = number(row.get("current_price_premium_to_insider_vwap"))
    drawdown = number(row.get("drawdown_from_52w_high"))
    market_cap = number(row.get("current_market_cap"))
    transaction_rows = number(row.get("transaction_rows"), default=1)

    if purchase_value >= 2_000_000:
        score += 3
        reasons.append(f"purchase value >= $2m (${purchase_value:,.0f})")
    elif purchase_value >= 1_000_000:
        score += 2
        reasons.append(f"purchase value >= $1m (${purchase_value:,.0f})")
    elif purchase_value >= 100_000:
        score += 1
        reasons.append(f"purchase value >= $100k (${purchase_value:,.0f})")

    if value_to_adv60 >= 0.05:
        score += 3
        reasons.append(f"purchase value / ADV60 >= 5% ({value_to_adv60:.1%})")
    elif value_to_adv60 >= 0.02:
        score += 2
        reasons.append(f"purchase value / ADV60 >= 2% ({value_to_adv60:.1%})")
    elif pd.isna(value_to_adv60):
        caveats.append("ADV60 unavailable")

    if premium <= 0:
        score += 1
        reasons.append(f"current price below insider VWAP ({premium:.1%})")
    elif premium <= 0.15:
        score += 1
        reasons.append(f"current price within 15% of insider VWAP ({premium:.1%})")
    else:
        score -= 3
        caveats.append(f"current price more than 15% above insider VWAP ({premium:.1%})")

    if drawdown <= -0.30:
        score += 2
        reasons.append(f"stock down at least 30% from 52w high ({drawdown:.1%})")
    elif drawdown <= -0.15:
        score += 1
        reasons.append(f"stock down at least 15% from 52w high ({drawdown:.1%})")
    elif pd.isna(drawdown):
        caveats.append("52w drawdown unavailable")

    if market_cap and market_cap < 300_000_000:
        score -= 2
        caveats.append(f"market cap below $300m (${market_cap:,.0f})")
    elif pd.isna(market_cap):
        caveats.append("market cap unavailable")

    if transaction_rows >= 2:
        score += 1
        reasons.append(f"multiple purchase rows in filing ({transaction_rows:.0f})")

    if not reasons:
        reasons.append("eligible insider purchase, but no strong secondary features")

    if score >= 7:
        tier = "ALERT"
    elif score >= 4:
        tier = "WATCH"
    else:
        tier = "ARCHIVE"
    return ScoreResult(score=score, tier=tier, reasons=reasons, caveats=caveats)


def apply_scores(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    scored = frame.copy()
    results = scored.apply(score_insider_candidate, axis=1)
    scored["research_score"] = [result.score for result in results]
    scored["research_tier"] = [result.tier for result in results]
    scored["score_reasons"] = [json.dumps(result.reasons) for result in results]
    scored["score_caveats"] = [json.dumps(result.caveats) for result in results]
    return scored.sort_values(["research_score", "signal_value_to_adv60", "purchase_value"], ascending=False)


def format_alert_message(row: pd.Series) -> str:
    reasons = parse_json_list(row.get("score_reasons"))
    caveats = parse_json_list(row.get("score_caveats"))
    ticker = display(row.get("ticker"))
    issuer = display(row.get("issuer_name"))
    lines = [
        f"[{display(row.get('research_tier'))}] {ticker} - {issuer}",
        "",
        "Insider signal:",
        f"- Insider: {display(row.get('insider_name'))}",
        f"- Filed: {display(row.get('filing_datetime'))}",
        f"- Purchase value: {money(row.get('purchase_value'))}",
        f"- Insider VWAP: {money(row.get('insider_vwap'))}",
        f"- Latest price: {money(row.get('latest_close'))}",
        f"- Premium to insider VWAP: {pct(row.get('current_price_premium_to_insider_vwap'))}",
        "",
        "Why flagged:",
        *[f"- {reason}" for reason in reasons],
        "",
        "Context:",
        f"- Purchase / ADV60: {pct(row.get('signal_value_to_adv60'))}",
        f"- 52w drawdown: {pct(row.get('drawdown_from_52w_high'))}",
        f"- Market cap: {money(row.get('current_market_cap'))}",
        f"- Purchase / market cap: {pct(row.get('purchase_to_market_cap'))}",
        f"- Score: {display(row.get('research_score'))}",
    ]
    if caveats:
        lines.extend(["", "Caveats:", *[f"- {caveat}" for caveat in caveats]])
    lines.extend(
        [
            "",
            "Links:",
            sec_archive_url(row),
            f"https://finance.yahoo.com/quote/{ticker}",
            f"https://finviz.com/quote.ashx?t={ticker}",
        ]
    )
    return "\n".join(lines)


def format_daily_digest(episodes: pd.DataFrame, *, lookback_hours: int, filings: int, raw_transactions: int) -> str:
    lines = [
        "# Daily Insider Signal Digest",
        "",
        f"- Lookback hours: {lookback_hours}",
        f"- Form 4/4-A filings discovered: {filings}",
        f"- Raw P/A transactions parsed: {raw_transactions}",
    ]
    if episodes.empty:
        lines.extend(["- Alert candidates: 0", "- Watch candidates: 0", "", "No monitor candidates passed the current filters."])
        return "\n".join(lines)

    counts = episodes["research_tier"].value_counts().to_dict()
    lines.extend(
        [
            f"- Alert candidates: {counts.get('ALERT', 0)}",
            f"- Watch candidates: {counts.get('WATCH', 0)}",
            f"- Archived candidates: {counts.get('ARCHIVE', 0)}",
            "",
            "## Top Candidates",
            "",
            "| Tier | Ticker | Company | Score | Value | Value/ADV60 | Premium | Drawdown |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in episodes.sort_values("research_score", ascending=False).head(20).itertuples(index=False):
        lines.append(
            f"| {row.research_tier} | {row.ticker} | {row.issuer_name} | {row.research_score} | "
            f"{money(row.purchase_value)} | {pct(row.signal_value_to_adv60)} | "
            f"{pct(row.current_price_premium_to_insider_vwap)} | {pct(getattr(row, 'drawdown_from_52w_high', None))} |"
        )
    return "\n".join(lines)


def parse_json_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return [value]
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return [str(parsed)]


def sec_archive_url(row: pd.Series) -> str:
    cik = str(row.get("cik", "")).strip()
    accession = str(row.get("accession", "")).strip()
    if not cik or not accession:
        return "SEC filing: n/a"
    try:
        cik_path = str(int(float(cik)))
    except ValueError:
        cik_path = cik.lstrip("0") or cik
    return f"https://www.sec.gov/Archives/edgar/data/{cik_path}/{accession.replace('-', '')}/"


def number(value: object, default: float = float("nan")) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def display(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return str(value)


def money(value: object) -> str:
    value_float = number(value)
    if pd.isna(value_float):
        return "n/a"
    return f"${value_float:,.2f}" if abs(value_float) < 1_000 else f"${value_float:,.0f}"


def pct(value: object) -> str:
    value_float = number(value)
    if pd.isna(value_float):
        return "n/a"
    return f"{value_float * 100:.1f}%"
