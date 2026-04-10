from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

INPUT_PATH = Path("data/processed/preclose_probabilities_sample.csv")
OUTPUT_PATH = Path("results/preclose_probability_histogram_sample.png")


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)
    df["preclose_prob"] = pd.to_numeric(df["preclose_prob"], errors="coerce")
    df = df[df["preclose_prob"].notna()].copy()

    plt.figure(figsize=(8, 6))
    plt.hist(df["preclose_prob"], bins=10, range=(0, 1), edgecolor="black")
    plt.xlabel("Pre-close Probability")
    plt.ylabel("Market Count")
    plt.title("Pre-close Probability Distribution (Sample)")
    plt.xlim(0, 1)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=300)
    plt.close()

    print("Done plotting pre-close probability histogram.")
    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
