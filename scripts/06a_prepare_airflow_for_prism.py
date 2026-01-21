#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv, dtype=str)

    # PRISM-required grouping columns
    df["system"] = "airflow"
    df["class"] = df["issue_type"].fillna("Unknown")
    df["req_id"] = df["issue_key"]
    df["source"] = "jira_airflow"

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print("OK")
    print(f"Wrote: {out_path}")
    print("Cols:", list(df.columns))
    print("Distinct classes:", df["class"].nunique())

if __name__ == "__main__":
    main()
