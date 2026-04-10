from pathlib import Path

import pandas as pd

INPUT_PATH = Path("data/processed/kalshi_binary_resolved_markets.csv")


def summarize_column(df, column_name):
    print(f"--- {column_name} ---")
    print("Missing values:", df[column_name].isna().sum())
    print("Min:", df[column_name].min())
    print("Max:", df[column_name].max())
    print("Mean:", df[column_name].mean())
    print()

    print("Value counts for exact 0 and exact 1:")
    print("== 0:", (df[column_name] == 0).sum())
    print("== 1:", (df[column_name] == 1).sum())
    print()

    print("First 10 unique values:")
    print(sorted(df[column_name].dropna().unique())[:10])
    print()

    print("Last 10 unique values:")
    print(sorted(df[column_name].dropna().unique())[-10:])
    print()
    print()


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)

    columns_to_check = [
        "last_price_dollars",
        "previous_price_dollars",
        "yes_bid_dollars",
        "yes_ask_dollars"
    ]

    print("Rows in filtered dataset:", len(df))
    print()

    for column in columns_to_check:
        summarize_column(df, column)


if __name__ == "__main__":
    main()
