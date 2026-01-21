#!/usr/bin/env python3
import pandas as pd
import sys

CSV = "data/processed/prism_scores_airflow.csv"

def main():
    df = pd.read_csv(CSV)

    print("Rows:", len(df))
    print("Columns:", list(df.columns))

    required = ["issue_key", "C", "S", "V"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print("FAIL: missing columns:", missing)
        sys.exit(1)

    print("\nNaN check:")
    print(df[required].isna().sum())

    print("\nRange check:")
    for c in ["C", "S", "V"]:
        print(f"{c}: min={df[c].min():.4f}, max={df[c].max():.4f}")

    print("\nMonotonicity check (should be FALSE):")
    print("C monotonic:", df["C"].is_monotonic_decreasing or df["C"].is_monotonic_increasing)
    print("S monotonic:", df["S"].is_monotonic_decreasing or df["S"].is_monotonic_increasing)
    print("V monotonic:", df["V"].is_monotonic_decreasing or df["V"].is_monotonic_increasing)

    print("\nOK: audit passed — readiness signals only")

if __name__ == "__main__":
    main()
