from __future__ import annotations

import pandas as pd
import pytest

from research.portfolio import build_cluster_signals, suppress_active_signals


def test_cluster_date_uses_latest_required_filing_timestamp_without_trade_lookahead():
    events = pd.DataFrame(
        [
            {
                "ticker": "abc",
                "insider_name": "First Insider",
                "filing_datetime": "2024-01-08 15:30",
                "trade_date": "2023-12-15",
                "company_name": "ABC Corp",
            },
            {
                "ticker": "abc",
                "insider_name": "Second Insider",
                "filing_datetime": "2024-01-10 09:45",
                "trade_date": "2023-12-01",
                "company_name": "ABC Corp",
            },
        ]
    )

    signals = build_cluster_signals(events, window_trading_days=3)

    assert len(signals) == 1
    assert signals.loc[0, "signal_datetime"] == pd.Timestamp("2024-01-10 09:45")
    assert signals.loc[0, "cluster_start_datetime"] == pd.Timestamp(
        "2024-01-08 15:30"
    )
    assert signals.loc[0, "insider_count"] == 2
    assert signals.loc[0, "insiders"] == ("First Insider", "Second Insider")


def test_cluster_window_counts_trading_days_not_calendar_days():
    events = pd.DataFrame(
        [
            {
                "ticker": "abc",
                "insider_name": "Friday Insider",
                "filing_datetime": "2024-01-05 16:00",
            },
            {
                "ticker": "abc",
                "insider_name": "Monday Insider",
                "filing_datetime": "2024-01-08 09:30",
            },
        ]
    )

    signals = build_cluster_signals(events, window_trading_days=2)

    assert len(signals) == 1
    assert signals.loc[0, "signal_datetime"] == pd.Timestamp("2024-01-08 09:30")


def test_duplicate_active_signal_is_suppressed_until_exit():
    signals = pd.DataFrame(
        [
            {"ticker": "ABC", "signal_datetime": "2024-01-02 10:00"},
            {"ticker": "ABC", "signal_datetime": "2024-07-01 10:00"},
            {"ticker": "XYZ", "signal_datetime": "2024-07-01 10:00"},
        ]
    )

    kept = suppress_active_signals(signals)

    assert kept["ticker"].tolist() == ["ABC", "XYZ"]
    assert kept.loc[0, "entry_datetime"] == pd.Timestamp("2024-01-02 10:00")
    assert kept.loc[0, "exit_datetime"] == pd.Timestamp("2025-01-02 10:00")


def test_minimum_366_calendar_day_hold_is_enforced():
    with pytest.raises(ValueError, match="366 calendar days"):
        suppress_active_signals(
            pd.DataFrame([{"ticker": "ABC", "signal_datetime": "2024-01-02"}]),
            hold_days=365,
        )

    signals = pd.DataFrame(
        [
            {"ticker": "ABC", "signal_datetime": "2024-01-02"},
            {"ticker": "ABC", "signal_datetime": "2025-01-01"},
            {"ticker": "ABC", "signal_datetime": "2025-01-02"},
        ]
    )

    kept = suppress_active_signals(signals, hold_days=366)

    assert kept["signal_datetime"].tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2025-01-02"),
    ]
