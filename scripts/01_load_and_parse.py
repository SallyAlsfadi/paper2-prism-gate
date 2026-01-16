#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import pandas as pd


COLS = [
    "internal_id",
    "issue_key",
    "project",
    "created_ts",
    "updated_ts",
    "issue_type",
    "status",
    "priority",
    "summary",
    "description",
]


def main():
    ap = argparse.ArgumentParser(description="Load and parse Airflow Jira export (Paper 2).")
    ap.add_argument("--in", dest="in_csv", required=True, help="Path to airflow_working.csv")
    ap.add_argument("--out_dir", required=True, help="Directory to write intermediate outputs")
    args = ap.parse_args()

    in_csv = Path(args.in_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Use python engine because the export includes messy quotes/newlines.
    df = pd.read_csv(
        in_csv,
        names=COLS,
        header=None,
        dtype=str,
        engine="python",
    )

    # Basic schema report (no filtering)
    report = {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "missing_per_column": {c: int(df[c].isna().sum()) for c in df.columns},
        "issue_type_counts_top10": df["issue_type"].value_counts().head(10).to_dict(),
        "priority_counts": df["priority"].value_counts().to_dict(),
        "status_counts_top10": df["status"].value_counts().head(10).to_dict(),
        "project_counts": df["project"].value_counts().to_dict(),
        "unique_issue_keys": int(df["issue_key"].nunique()),
        "duplicate_issue_keys": int(len(df) - df["issue_key"].nunique()),
    }

    (out_dir / "01_schema_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    # Small preview for sanity
    preview_cols = ["issue_key", "project", "created_ts", "updated_ts", "issue_type", "status", "priority", "summary"]
    df[preview_cols].head(50).to_csv(out_dir / "01_preview_sample.csv", index=False)

    print("OK")
    print(f"Rows loaded: {len(df)}")
    print(f"Schema report: {out_dir/'01_schema_report.json'}")
    print(f"Preview sample: {out_dir/'01_preview_sample.csv'}")


if __name__ == "__main__":
    main()
