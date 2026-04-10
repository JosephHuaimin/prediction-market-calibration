from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

INPUT_PATH = Path("data/processed/calibration_table_sample.csv")
OUTPUT_PATH = Path("results/calibration_curve_sample.png")


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)

    df = df[df["market_count"] > 0].copy()

    plt.figure(figsize=(8, 6))
    plt.scatter(df["avg_predicted_prob"], df["actual_yes_rate"], s=60)
    plt.plot([0, 1], [0, 1], linestyle="--")

    for _, row in df.iterrows():
        plt.annotate(
            int(row["market_count"]),
            (row["avg_predicted_prob"], row["actual_yes_rate"]),
            textcoords="offset points",
            xytext=(5, 5)
        )

    plt.xlabel("Average Predicted Probability")
    plt.ylabel("Actual Yes Rate")
    plt.title("Calibration Curve (Sample)")
    plt.xlim(0, 1)
    plt.ylim(0, 1)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=300)
    plt.close()

    print("Done plotting calibration curve.")
    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
