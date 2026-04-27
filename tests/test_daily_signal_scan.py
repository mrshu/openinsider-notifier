from __future__ import annotations

import pandas as pd

from research.daily_signal_scan import (
    ScanConfig,
    append_jsonl_unique,
    archive_base_from_index_url,
    parse_atom_entries,
    read_jsonl,
    score_candidates,
)


def test_parse_atom_entries_extracts_accession_and_archive_base() -> None:
    atom = """<?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>4 - Example Owner (0001530803) (Reporting)</title>
    <link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/1530803/000090266426002148/0000902664-26-002148-index.htm"/>
    <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2026-04-24 &lt;b&gt;AccNo:&lt;/b&gt; 0000902664-26-002148 &lt;b&gt;Size:&lt;/b&gt; 12 KB</summary>
    <updated>2026-04-24T21:35:59-04:00</updated>
    <category scheme="https://www.sec.gov/" label="form type" term="4"/>
    <id>urn:tag:sec.gov,2008:accession-number=0000902664-26-002148</id>
  </entry>
</feed>"""

    rows = parse_atom_entries(atom)

    assert rows[0]["accession"] == "0000902664-26-002148"
    assert rows[0]["cik"] == "0001530803"
    assert rows[0]["archive_base_url"] == "https://www.sec.gov/Archives/edgar/data/1530803/000090266426002148"
    assert rows[0]["form_type"] == "4"


def test_archive_base_from_index_url_handles_sec_paths() -> None:
    base, cik = archive_base_from_index_url(
        "https://www.sec.gov/Archives/edgar/data/1530803/000090266426002148/0000902664-26-002148-index.htm"
    )

    assert base.endswith("/Archives/edgar/data/1530803/000090266426002148")
    assert cik == "0001530803"


def test_score_candidates_requires_liquidity_or_large_value_and_price_discipline(tmp_path) -> None:
    config = ScanConfig(
        output_dir=tmp_path,
        cache_dir=tmp_path,
        state_file=tmp_path / "state.json",
        lookback_hours=72,
        max_entries=100,
        sleep_seconds=0,
        min_purchase_value=100_000,
        min_adv60_ratio=0.02,
        alert_adv60_ratio=0.05,
        max_price_premium=0.15,
        notify=False,
        notify_discord=False,
    )
    frame = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "purchase_value": 200_000,
                "signal_value_to_adv60": 0.03,
                "current_price_premium_to_insider_vwap": 0.10,
            },
            {
                "ticker": "BBB",
                "purchase_value": 2_500_000,
                "signal_value_to_adv60": 0.01,
                "current_price_premium_to_insider_vwap": 0.05,
            },
            {
                "ticker": "CCC",
                "purchase_value": 2_500_000,
                "signal_value_to_adv60": 0.10,
                "current_price_premium_to_insider_vwap": 0.25,
            },
        ]
    )

    scored = score_candidates(frame, config).set_index("ticker")

    assert scored.loc["AAA", "monitor_candidate"]
    assert scored.loc["BBB", "alert_candidate"]
    assert not scored.loc["CCC", "monitor_candidate"]


def test_score_candidates_does_not_pass_missing_price_gate(tmp_path) -> None:
    config = ScanConfig(
        output_dir=tmp_path,
        cache_dir=tmp_path,
        state_file=tmp_path / "state.json",
        lookback_hours=72,
        max_entries=100,
        sleep_seconds=0,
        min_purchase_value=100_000,
        min_adv60_ratio=0.02,
        alert_adv60_ratio=0.05,
        max_price_premium=0.15,
        notify=False,
        notify_discord=False,
    )
    frame = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "purchase_value": 2_000_000,
                "signal_value_to_adv60": 0.10,
                "current_price_premium_to_insider_vwap": pd.NA,
            }
        ]
    )

    scored = score_candidates(frame, config)

    assert not scored.iloc[0]["passes_price_premium"]
    assert not scored.iloc[0]["monitor_candidate"]


def test_append_jsonl_unique_dedupes_by_key(tmp_path) -> None:
    path = tmp_path / "alerts.jsonl"

    appended = append_jsonl_unique(path, [{"alert_key": "a", "ticker": "AAA"}], "alert_key")
    appended_again = append_jsonl_unique(path, [{"alert_key": "a", "ticker": "AAA"}], "alert_key")

    assert len(appended) == 1
    assert appended_again == []
    assert read_jsonl(path) == [{"alert_key": "a", "ticker": "AAA"}]
