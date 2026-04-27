from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class PortfolioSelection:
    """Equal-weight target for one selected signal."""

    ticker: str
    signal_datetime: pd.Timestamp
    weight: float
    sector: str | None = None


def build_cluster_signals(
    events: pd.DataFrame,
    window_trading_days: int,
    *,
    ticker_col: str = "ticker",
    insider_col: str = "insider_name",
    filing_col: str = "filing_datetime",
    company_col: str = "company_name",
    value_col: str = "purchase_value",
    shares_col: str = "shares",
    min_distinct_insiders: int = 2,
    min_cluster_value: float | None = None,
) -> pd.DataFrame:
    """Build issuer cluster signals using only public filing timestamps.

    A cluster date is the latest public filing timestamp required to satisfy
    the distinct-insider threshold inside the trailing trading-day window.
    Transaction dates are deliberately ignored to avoid lookahead.
    """

    if window_trading_days < 1:
        raise ValueError("window_trading_days must be at least 1")
    if min_distinct_insiders < 2:
        raise ValueError("min_distinct_insiders must be at least 2")

    required = {ticker_col, insider_col, filing_col}
    missing = sorted(required - set(events.columns))
    if missing:
        raise ValueError(f"Missing required event columns: {', '.join(missing)}")

    normalized = events.copy()
    normalized[filing_col] = pd.to_datetime(normalized[filing_col], errors="coerce")
    normalized = normalized[
        normalized[filing_col].notna()
        & normalized[ticker_col].notna()
        & normalized[insider_col].notna()
    ].copy()
    normalized[ticker_col] = normalized[ticker_col].astype(str).str.upper()
    normalized[insider_col] = normalized[insider_col].astype(str)
    normalized = normalized.sort_values([ticker_col, filing_col, insider_col])

    signals: list[dict[str, object]] = []
    for ticker, ticker_events in normalized.groupby(ticker_col, sort=False):
        ticker_events = ticker_events.reset_index(drop=True)

        for position, row in ticker_events.iterrows():
            signal_datetime = row[filing_col]
            window_start = _trading_window_start(
                signal_datetime, window_trading_days
            )
            window = ticker_events[
                (ticker_events[filing_col] >= window_start)
                & (ticker_events[filing_col] <= signal_datetime)
            ]
            distinct_insiders = window[insider_col].nunique()

            prior_window = ticker_events.iloc[:position]
            prior_window = prior_window[
                (prior_window[filing_col] >= window_start)
                & (prior_window[filing_col] <= signal_datetime)
            ]
            prior_distinct_insiders = prior_window[insider_col].nunique()

            if (
                distinct_insiders < min_distinct_insiders
                or prior_distinct_insiders >= min_distinct_insiders
            ):
                continue

            cluster_value = (
                pd.to_numeric(window[value_col], errors="coerce").sum()
                if value_col in window.columns
                else None
            )
            cluster_shares = (
                pd.to_numeric(window[shares_col], errors="coerce").sum()
                if shares_col in window.columns
                else None
            )
            if (
                min_cluster_value is not None
                and cluster_value is not None
                and cluster_value < min_cluster_value
            ):
                continue

            signals.append(
                {
                    "ticker": ticker,
                    "signal_datetime": signal_datetime,
                    "cluster_start_datetime": window[filing_col].min(),
                    "insider_count": distinct_insiders,
                    "event_count": len(window),
                    "cluster_value": cluster_value,
                    "cluster_shares": cluster_shares,
                    "insider_vwap": (
                        cluster_value / cluster_shares
                        if cluster_value is not None and cluster_shares
                        else None
                    ),
                    "insiders": tuple(sorted(window[insider_col].unique())),
                    "company_name": row[company_col]
                    if company_col in normalized.columns
                    else None,
                }
            )

    columns = [
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
    return pd.DataFrame(signals, columns=columns).sort_values(
        ["signal_datetime", "ticker"], ignore_index=True
    )


def suppress_active_signals(
    signals: pd.DataFrame,
    *,
    hold_days: int = 366,
    ticker_col: str = "ticker",
    signal_col: str = "signal_datetime",
) -> pd.DataFrame:
    """Keep one active signal per issuer until its calendar-day exit."""

    if hold_days < 366:
        raise ValueError("hold_days must be at least 366 calendar days")

    required = {ticker_col, signal_col}
    missing = sorted(required - set(signals.columns))
    if missing:
        raise ValueError(f"Missing required signal columns: {', '.join(missing)}")

    ordered = signals.copy()
    ordered[signal_col] = pd.to_datetime(ordered[signal_col], errors="coerce")
    ordered = ordered[ordered[signal_col].notna()].sort_values(
        [ticker_col, signal_col]
    )

    kept_rows = []
    for _, ticker_signals in ordered.groupby(ticker_col, sort=False):
        active_until = pd.Timestamp.min

        for _, row in ticker_signals.iterrows():
            signal_datetime = row[signal_col]
            if signal_datetime < active_until:
                continue

            output = row.to_dict()
            output["entry_datetime"] = signal_datetime
            output["exit_datetime"] = signal_datetime + pd.Timedelta(days=hold_days)
            kept_rows.append(output)
            active_until = output["exit_datetime"]

    if not kept_rows:
        return ordered.assign(entry_datetime=pd.NaT, exit_datetime=pd.NaT).iloc[0:0]

    return pd.DataFrame(kept_rows).sort_values(
        [signal_col, ticker_col], ignore_index=True
    )


def equal_weight_targets(
    signals: pd.DataFrame,
    *,
    max_positions: int,
    sector_cap: float | None = None,
    ticker_col: str = "ticker",
    signal_col: str = "signal_datetime",
    sector_col: str = "sector",
) -> list[PortfolioSelection]:
    """Select deterministic equal-weight targets with an optional sector cap."""

    if max_positions < 1:
        raise ValueError("max_positions must be at least 1")
    if sector_cap is not None and not 0 < sector_cap <= 1:
        raise ValueError("sector_cap must be greater than 0 and no more than 1")

    ordered = signals.copy()
    ordered[signal_col] = pd.to_datetime(ordered[signal_col], errors="coerce")
    ordered = ordered[ordered[signal_col].notna()].sort_values(
        [signal_col, ticker_col]
    )

    sector_counts: dict[str, int] = {}
    selected: list[pd.Series] = []
    for _, row in ordered.iterrows():
        if len(selected) >= max_positions:
            break

        sector = row[sector_col] if sector_col in ordered.columns else None
        if sector_cap is not None and sector is not None:
            candidate_count = sector_counts.get(str(sector), 0) + 1
            candidate_weight = 1 / max_positions
            if candidate_count * candidate_weight > sector_cap:
                continue
            sector_counts[str(sector)] = candidate_count

        selected.append(row)

    if not selected:
        return []

    weight = 1 / len(selected)
    return [
        PortfolioSelection(
            ticker=str(row[ticker_col]),
            signal_datetime=row[signal_col],
            weight=weight,
            sector=str(row[sector_col])
            if sector_col in ordered.columns and pd.notna(row[sector_col])
            else None,
        )
        for row in selected
    ]


def _trading_window_start(
    end_datetime: pd.Timestamp, window_trading_days: int
) -> pd.Timestamp:
    end_date = end_datetime.normalize()
    offset = pd.offsets.BusinessDay(window_trading_days - 1)
    return end_date - offset
