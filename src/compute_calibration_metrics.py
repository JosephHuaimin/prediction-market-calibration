from pathlib import Path

import pandas as pd

INPUT_PATH = Path("data/processed/preclose_probabilities_sample.csv")
OUTPUT_PATH = Path("data/processed/calibration_table_sample.csv")


def assign_bin(probability):
    if probability < 0.1:
        return "[0.0, 0.1)"
    elif probability < 0.2:
        return "[0.1, 0.2)"
    elif probability < 0.3:
        return "[0.2, 0.3)"
    elif probability < 0.4:
        return "[0.3, 0.4)"
    elif probability < 0.5:
        return "[0.4, 0.5)"
    elif probability < 0.6:
        return "[0.5, 0.6)"
    elif probability < 0.7:
        return "[0.6, 0.7)"
    elif probability < 0.8:
        return "[0.7, 0.8)"
    elif probability < 0.9:
        return "[0.8, 0.9)"
    else:
        return "[0.9, 1.0]"


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)

    df["outcome"] = pd.to_numeric(df["outcome"], errors="coerce")
    df["preclose_prob"] = pd.to_numeric(df["preclose_prob"], errors="coerce")

    df = df[df["outcome"].notna() & df["preclose_prob"].notna()].copy()

    df["squared_error"] = (df["preclose_prob"] - df["outcome"]) ** 2
    brier_score = df["squared_error"].mean()

    df["prob_bin"] = df["preclose_prob"].apply(assign_bin)

    calibration_table = (
        df.groupby("prob_bin", as_index=False)
        .agg(
            market_count=("ticker", "count"),
            avg_predicted_prob=("preclose_prob", "mean"),
            actual_yes_rate=("outcome", "mean"),
            avg_squared_error=("squared_error", "mean")
        )
    )

    bin_order = [
        "[0.0, 0.1)",
        "[0.1, 0.2)",
        "[0.2, 0.3)",
        "[0.3, 0.4)",
        "[0.4, 0.5)",
        "[0.5, 0.6)",
        "[0.6, 0.7)",
        "[0.7, 0.8)",
        "[0.8, 0.9)",
        "[0.9, 1.0]"
    ]

    calibration_table["prob_bin"] = pd.Categorical(
        calibration_table["prob_bin"],
        categories=bin_order,
        ordered=True
    )

    calibration_table = calibration_table.sort_values("prob_bin")
    calibration_table.to_csv(OUTPUT_PATH, index=False)

    print("Done computing calibration metrics.")
    print("Number of usable markets:", len(df))
    print("Brier score:", round(brier_score, 6))
    print()

    print("Calibration table:")
    print(calibration_table)
    print()

    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
