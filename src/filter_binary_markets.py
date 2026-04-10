from pathlib import Path

import pandas as pd

INPUT_PATH = Path("data/processed/kalshi_market_metadata.csv")
OUTPUT_PATH = Path("data/processed/kalshi_binary_resolved_markets.csv")


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)

    filtered_df = df[
        (df["market_type"] == "binary") &
        (df["result"].isin(["yes", "no"])) &
        (df["outcome"].notna())
    ].copy()

    filtered_df.to_csv(OUTPUT_PATH, index=False)

    print("Done filtering binary resolved markets.")
    print("Original rows:", len(df))
    print("Filtered rows:", len(filtered_df))
    print()

    print("Result counts after filtering:")
    print(filtered_df["result"].value_counts(dropna=False))
    print()

    print("Saved to:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
