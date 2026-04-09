import json
from pathlib import Path

INPUT_PATH = Path("data/raw/kalshi_historical_markets_page1.json")


def main():
    with open(INPUT_PATH, "r", encoding="utf-8") as file:
        data = json.load(file)

    print("Top-level keys:")
    print(list(data.keys()))
    print()

    markets = data.get("markets", [])
    print("Number of markets in this file:")
    print(len(markets))
    print()

    if len(markets) > 0:
        print("Keys in the first market:")
        print(list(markets[0].keys()))
        print()
        print("First market preview:")
        print(markets[0])


if __name__ == "__main__":
    main()
