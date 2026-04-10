import json
import time
from pathlib import Path

import pandas as pd
import requests

INPUT_PATH = Path("data/processed/candlestick_sample_markets.csv")
OUTPUT_DIR = Path("data/raw/candlesticks_sample")

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/historical/markets/{ticker}/candlesticks"
PERIOD_INTERVAL = 60


def to_unix_timestamp(datetime_value):
    return int(pd.Timestamp(datetime_value).timestamp())


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)
    df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce", utc=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    success_count = 0
    fail_count = 0

    for i, row in df.iterrows():
        ticker = row["ticker"]
        open_time = row["open_time"]
        close_time = row["close_time"]

        if pd.isna(open_time) or pd.isna(close_time):
            print(f"Skipping {ticker}: missing open_time or close_time")
            fail_count += 1
            continue

        start_ts = to_unix_timestamp(open_time)
        end_ts = to_unix_timestamp(close_time)

        url = BASE_URL.format(ticker=ticker)
        params = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": PERIOD_INTERVAL
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            output_path = OUTPUT_DIR / f"{ticker}.json"
            with open(output_path, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=2)

            success_count += 1
            print(f"[{i + 1}/{len(df)}] Saved candlesticks for {ticker}")

        except Exception as error:
            fail_count += 1
            print(f"[{i + 1}/{len(df)}] Failed for {ticker}: {error}")

        time.sleep(0.1)

    print()
    print("Finished fetching candlesticks.")
    print("Success count:", success_count)
    print("Fail count:", fail_count)
    print(f"Saved files to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
