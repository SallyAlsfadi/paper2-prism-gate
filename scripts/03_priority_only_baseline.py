#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import pandas as pd

PRIORITY_ORDER = {
    "Blocker": 1,
    "Critical": 2,
    "Major": 3,
    "Minor": 4,
    "Trivial": 5,
}

def main():
    ap = argparse.ArgumentParser(description="Build priority-only baseline ordering (Paper 2).")
    ap.add_argument("--in_csv", required=True, help="Processed CSV with text/text_len (minlen applied).")
    ap.add_argument("--out_csv", required=True, help="Output baseline CSV with rank.")
    ap.add_argument("--out_report", required=True, help="Output JSON report.")
    args = ap.parse_args()

    in_csv = Path(args.in_csv)
    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_csv, dtype=str)

    # Map priority to ordinal (industrial signal only)
    df["priority_ordinal"] = df["priority"].map(PRIORITY_ORDER)

    # Safety checks
    missing_ord = df["priority_ordinal"].isna().sum()
    if missing_ord > 0:
        bad = df[df["priority_ordinal"].isna()]["priority"].value_counts().to_dict()
        raise ValueError(f"Unknown priority values encountered: {bad}")

    # Make timestamps numeric for sorting
    df["created_ts_num"] = pd.to_numeric(df["created_ts"], errors="coerce")
    if df["created_ts_num"].isna().sum() > 0:
        raise ValueError("Some created_ts could not be parsed as numeric.")

    # Priority-only ordering with deterministic ties
    df = df.sort_values(
        by=["priority_ordinal", "created_ts_num", "issue_key"],
        ascending=[True, True, True],
        kind="mergesort",  # stable sort
    ).reset_index(drop=True)

    df["rank_priority_only"] = df.index + 1

    # Minimal report for paper + sanity
    report = {
        "rows": int(len(df)),
        "priority_distribution": df["priority"].value_counts().to_dict(),
        "ordering": {
            "primary": "priority (Blocker > Critical > Major > Minor > Trivial)",
            "tie_breakers": ["created_ts (older first)", "issue_key (asc)"],
        },
        "top10_issue_keys": df.head(10)["issue_key"].tolist(),
    }

    keep_cols = [
        "rank_priority_only",
        "issue_key",
        "project",
        "created_ts",
        "updated_ts",
        "issue_type",
        "status",
        "priority",
        "priority_ordinal",
        "text_len",
        "text",
    ]
    df[keep_cols].to_csv(out_csv, index=False)
    out_report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("OK")
    print(f"Rows: {len(df)}")
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_report}")
    print("Top-10 issue keys:")
    print("\\n".join(report["top10_issue_keys"]))

if __name__ == "__main__":
    main()
