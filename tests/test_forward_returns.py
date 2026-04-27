from __future__ import annotations

import pandas as pd

from research.forward_returns import first_price_on_or_after


def test_first_price_on_or_after_skips_non_trading_days() -> None:
    prices = pd.Series(
        [10.0, 12.0],
        index=pd.to_datetime(["2026-04-24", "2026-04-27"]),
    )

    found = first_price_on_or_after(prices, pd.Timestamp("2026-04-25"))

    assert found == ("2026-04-27", 12.0)


def test_first_price_on_or_after_returns_none_when_no_price_available() -> None:
    prices = pd.Series([10.0], index=pd.to_datetime(["2026-04-24"]))

    assert first_price_on_or_after(prices, pd.Timestamp("2026-04-25")) is None
