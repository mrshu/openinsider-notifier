from __future__ import annotations

import pandas as pd

from research.alerting import apply_scores, format_alert_message, format_daily_digest


def test_apply_scores_assigns_alert_for_large_liquidity_intense_purchase() -> None:
    frame = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "issuer_name": "AAA Inc",
                "purchase_value": 2_500_000,
                "signal_value_to_adv60": 0.08,
                "current_price_premium_to_insider_vwap": 0.05,
                "drawdown_from_52w_high": -0.35,
                "current_market_cap": 1_000_000_000,
                "transaction_rows": 2,
            }
        ]
    )

    scored = apply_scores(frame)

    assert scored.iloc[0]["research_tier"] == "ALERT"
    assert scored.iloc[0]["research_score"] >= 7
    assert "purchase value / ADV60" in scored.iloc[0]["score_reasons"]


def test_apply_scores_penalizes_price_chasing() -> None:
    frame = pd.DataFrame(
        [
            {
                "ticker": "BBB",
                "issuer_name": "BBB Inc",
                "purchase_value": 2_500_000,
                "signal_value_to_adv60": 0.08,
                "current_price_premium_to_insider_vwap": 0.35,
                "drawdown_from_52w_high": 0.0,
                "current_market_cap": 1_000_000_000,
                "transaction_rows": 1,
            }
        ]
    )

    scored = apply_scores(frame)

    assert scored.iloc[0]["research_tier"] != "ALERT"
    assert "more than 15%" in scored.iloc[0]["score_caveats"]


def test_format_alert_message_contains_decision_context() -> None:
    scored = apply_scores(
        pd.DataFrame(
            [
                {
                    "ticker": "AAA",
                    "issuer_name": "AAA Inc",
                    "insider_name": "Jane Doe",
                    "filing_datetime": "2026-04-27 20:00:00",
                    "purchase_value": 2_500_000,
                    "insider_vwap": 10,
                    "latest_close": 10.5,
                    "signal_value_to_adv60": 0.08,
                    "current_price_premium_to_insider_vwap": 0.05,
                    "drawdown_from_52w_high": -0.35,
                    "current_market_cap": 1_000_000_000,
                    "purchase_to_market_cap": 0.0025,
                    "transaction_rows": 2,
                    "cik": "0000123456",
                    "accession": "0000000000-26-000001",
                }
            ]
        )
    )

    message = format_alert_message(scored.iloc[0])

    assert "[ALERT] AAA - AAA Inc" in message
    assert "Purchase / ADV60" in message
    assert "SEC filing" not in message
    assert "https://www.sec.gov/Archives" not in message
    assert "http://www.openinsider.com/AAA" in message


def test_format_daily_digest_summarizes_tiers() -> None:
    scored = apply_scores(
        pd.DataFrame(
            [
                {
                    "ticker": "AAA",
                    "issuer_name": "AAA Inc",
                    "purchase_value": 2_500_000,
                    "signal_value_to_adv60": 0.08,
                    "current_price_premium_to_insider_vwap": 0.05,
                    "drawdown_from_52w_high": -0.35,
                    "current_market_cap": 1_000_000_000,
                    "transaction_rows": 2,
                }
            ]
        )
    )

    digest = format_daily_digest(scored, lookback_hours=72, filings=10, raw_transactions=3)

    assert "Alert candidates: 1" in digest
    assert "| ALERT | AAA |" in digest
