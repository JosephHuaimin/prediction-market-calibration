import json
from pathlib import Path

import pandas as pd

SAMPLE_PATH = Path("data/processed/candlestick_sample_markets.csv")
CANDLE_DIR = Path("data/raw/candlesticks_sample")
OUTPUT_PATH = Path("data/processed/preclose_probabilities_sample.csv")


def main():
    df = pd.read_csv(SAMPLE_PATH, low_memory=False)

    rows = []

    for _, row in df.iterrows():
        ticker = row["ticker"]
        json_path = CANDLE_DIR / f"{ticker}.json"

        if not json_path.exists():
            continue

        with open(json_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        candlesticks = data.get("candlesticks", [])
        if len(candlesticks) == 0:
            continue

        valid_candles = []

        for candle in candlesticks:
            end_period_ts = candle.get("end_period_ts")

            price_block = candle.get("price", {})
            close_dollars = price_block.get("close_dollars")
            if close_dollars is None or close_dollars == "":
                close_dollars = price_block.get("close")

            if end_period_ts is None:
                continue

            if close_dollars is None or close_dollars == "":
                continue

            try:
                end_period_ts = int(end_period_ts)
                close_dollars = float(close_dollars)
            except Exception:
                continue

            valid_candles.append((end_period_ts, close_dollars))

        if len(valid_candles) == 0:
            continue

        valid_candles.sort(key=lambda x: x[0])
        last_end_ts, preclose_prob = valid_candles[-1]

        rows.append({
            "ticker": ticker,
            "title": row.get("title"),
            "result": row.get("result"),
            "outcome": row.get("outcome"),
            "volume_fp": row.get("volume_fp"),
            "open_time": row.get("open_time"),
            "close_time": row.get("close_time"),
            "preclose_candle_end_ts": last_end_ts,
            "preclose_prob": preclose_prob,
            "num_valid_candles": len(valid_candles)
        })

    out_df = pd.DataFrame(rows)
    out_df.to_csv(OUTPUT_PATH, index=False)

    print("Done extracting pre-close probabilities.")
    print("Rows extracted:", len(out_df))
    print()

    if len(out_df) > 0:
        print("Probability summary:")
        print(out_df["preclose_prob"].describe())
        print()

        print("First 10 rows:")
        print(out_df[["ticker", "preclose_prob", "outcome", "num_valid_candles"]].head(10))
        print()

    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
