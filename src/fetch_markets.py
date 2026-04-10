import json
from pathlib import Path

import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/historical/markets"
OUTPUT_PATH = Path("data/raw/kalshi_historical_markets_sample.json")
MAX_PAGES = 50


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    all_markets = []
    cursor = None
    page_number = 1

    while page_number <= MAX_PAGES:
        params = {
            "limit": 1000
        }

        if cursor:
            params["cursor"] = cursor

        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        markets = data.get("markets", [])
        cursor = data.get("cursor")

        all_markets.extend(markets)

        print(f"Page {page_number}: downloaded {len(markets)} markets")

        if not cursor:
            break

        page_number = page_number + 1

    output_data = {
        "markets": all_markets
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
        json.dump(output_data, file, indent=2)

    print()
    print("Download complete.")
    print("Total markets downloaded:", len(all_markets))
    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
