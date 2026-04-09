import json
from pathlib import Path

import requests

URL = "https://api.elections.kalshi.com/trade-api/v2/historical/markets"
OUTPUT_PATH = Path("data/raw/kalshi_historical_markets_page1.json")


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print("Download complete.")
    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
