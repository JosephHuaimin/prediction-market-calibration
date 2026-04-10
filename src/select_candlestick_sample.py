from pathlib import Path

import pandas as pd

INPUT_PATH = Path("data/processed/kalshi_binary_resolved_markets.csv")
OUTPUT_PATH = Path("data/processed/candlestick_sample_markets.csv")

SAMPLE_SIZE = 200


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)

    df["volume_fp"] = pd.to_numeric(df["volume_fp"], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")
    df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")

    sample_df = df[
        (df["volume_fp"] > 0) &
        (df["open_time"].notna()) &
        (df["close_time"].notna())
    ].copy()

    sample_df = sample_df.sort_values("volume_fp", ascending=False)
    sample_df = sample_df.head(SAMPLE_SIZE)

    sample_df.to_csv(OUTPUT_PATH, index=False)

    print("Done selecting candlestick sample.")
    print("Rows in original binary dataset:", len(df))
    print("Rows with volume > 0 and both open_time/close_time:", len(sample_df))
    print()
    print("Top sample preview:")
    print(sample_df[["ticker", "title", "volume_fp", "close_time"]].head(10))
    print()
    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
