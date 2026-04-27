from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE_URL = "http://www.openinsider.com/screener"


def build_params(days: int, page: int, min_value_thousands: int, rows_per_page: int) -> dict:
    return {
        "s": "",
        "o": "",
        "pl": 50,
        "ph": "",
        "ll": "",
        "lh": "",
        "fd": days,
        "fdr": "",
        "td": 0,
        "tdr": "",
        "fdlyl": "",
        "fdlyh": "",
        "daysago": "",
        "xp": 1,
        "vl": min_value_thousands,
        "vh": "",
        "ocl": "",
        "och": "",
        "sic1": -1,
        "sicl": 100,
        "sich": 9999,
        "isofficer": 1,
        "iscob": 1,
        "isceo": 1,
        "ispres": 1,
        "iscoo": 1,
        "iscfo": 1,
        "isgc": 1,
        "isvp": 1,
        "grp": 0,
        "nfl": "",
        "nfh": "",
        "nil": "",
        "nih": "",
        "nol": "",
        "noh": "",
        "v2l": "",
        "v2h": "",
        "oc2l": "",
        "oc2h": "",
        "sortcol": 0,
        "cnt": rows_per_page,
        "page": page,
    }


def fetch_table(params: dict) -> pd.DataFrame:
    response = requests.get(
        f"{BASE_URL}?{urlencode(params)}",
        headers={"User-Agent": "Mozilla/5.0 openinsider-notifier backtest"},
        timeout=60,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="tinytable")
    if table is None:
        return pd.DataFrame()

    headers = [header.text.strip() for header in table.find_all("th")]
    rows = [
        [col.text.strip() for col in row.find_all("td")]
        for row in table.find_all("tr")[1:]
    ]
    return pd.DataFrame(rows, columns=headers)


def download_history(
    output: Path,
    days: int,
    min_value_thousands: int,
    rows_per_page: int,
    max_pages: int,
    sleep_seconds: float,
) -> pd.DataFrame:
    frames = []
    for page in range(1, max_pages + 1):
        params = build_params(days, page, min_value_thousands, rows_per_page)
        frame = fetch_table(params)
        print(f"page={page} rows={len(frame)}")
        if frame.empty:
            break
        frames.append(frame)
        if len(frame) < rows_per_page:
            break
        time.sleep(sleep_seconds)

    if not frames:
        raise RuntimeError("No OpenInsider rows were downloaded")

    combined = pd.concat(frames, ignore_index=True).drop_duplicates()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as handle:
        for _, row in combined.iterrows():
            handle.write(json.dumps(row.to_dict()) + "\n")
    return combined


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download OpenInsider screener rows for free-data backtests."
    )
    parser.add_argument("--output", type=Path, default=Path("data/raw/openinsider_3y.jsonl"))
    parser.add_argument("--days", type=int, default=1095)
    parser.add_argument("--min-value-thousands", type=int, default=100)
    parser.add_argument("--rows-per-page", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    data = download_history(
        output=args.output,
        days=args.days,
        min_value_thousands=args.min_value_thousands,
        rows_per_page=args.rows_per_page,
        max_pages=args.max_pages,
        sleep_seconds=args.sleep_seconds,
    )
    print(f"downloaded_rows={len(data)} output={args.output}")
