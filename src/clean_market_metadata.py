import json
from pathlib import Path

import pandas as pd

INPUT_PATH = Path("data/raw/kalshi_historical_markets_page1.json")
OUTPUT_PATH = Path("data/processed/kalshi_market_metadata.csv")

with open(INPUT_PATH, "r", encoding="utf-8") as file:
    data = json.load(file)

markets = data.get("markets", [])

rows = []

for market in markets:
    result_text = market.get("result")
    outcome = None

    if result_text == "yes":
        outcome = 1
    elif result_text == "no":
        outcome = 0

    row = {
        "ticker": market.get("ticker"),
        "title": market.get("title"),
        "event_ticker": market.get("event_ticker"),
        "market_type": market.get("market_type"),
        "status": market.get("status"),
        "result": result_text,
        "created_time": market.get("created_time"),
        "open_time": market.get("open_time"),
        "close_time": market.get("close_time"),
        "updated_time": market.get("updated_time"),
        "settlement_ts": market.get("settlement_ts"),
        "last_price_dollars": market.get("last_price_dollars"),
        "previous_price_dollars": market.get("previous_price_dollars"),
        "yes_bid_dollars": market.get("yes_bid_dollars"),
        "yes_ask_dollars": market.get("yes_ask_dollars"),
        "volume_fp": market.get("volume_fp"),
        "volume_24h_fp": market.get("volume_24h_fp"),
        "liquidity_dollars": market.get("liquidity_dollars"),
        "open_interest_fp": market.get("open_interest_fp"),
        "outcome": outcome
    }

    rows.append(row)

df = pd.DataFrame(rows)

numeric_columns = [
    "last_price_dollars",
    "previous_price_dollars",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "volume_fp",
    "volume_24h_fp",
    "liquidity_dollars",
    "open_interest_fp"
]

for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors="coerce")

time_columns = [
    "created_time",
    "open_time",
    "close_time",
    "updated_time",
    "settlement_ts"
]

for column in time_columns:
    df[column] = pd.to_datetime(df[column], errors="coerce")

df["is_resolved"] = df["outcome"].notna()

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)

print("Done cleaning market metadata.")
print("Total rows:", len(df))
print("Resolved rows:", int(df["is_resolved"].sum()))
print()

print("Result counts:")
print(df["result"].value_counts(dropna=False))
print()

print("Preview:")
print(df[["ticker", "title", "status", "result", "outcome", "last_price_dollars"]].head(10))
print()

print(f"Saved to: {OUTPUT_PATH}")
