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
    ap = argparse.ArgumentParser(description="Build text field + minimal text-length filter (Paper 2).")
    ap.add_argument("--in", dest="in_csv", required=True, help="Path to airflow_working.csv")
    ap.add_argument("--out_csv", required=True, help="Output processed CSV")
    ap.add_argument("--out_report", required=True, help="Output JSON report")
    ap.add_argument("--min_len", type=int, default=30, help="Minimum text length to keep (default: 30)")
    args = ap.parse_args()

    in_csv = Path(args.in_csv)
    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(
        in_csv,
        names=COLS,
        header=None,
        dtype=str,
        engine="python",
    )

    # LOCKED: text comes from summary only (it contains embedded description content in this export)
    df["text"] = (
        df["summary"]
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    df["text_len"] = df["text"].str.len()

    before = len(df)
    removed = int((df["text_len"] < args.min_len).sum())
    df_kept = df[df["text_len"] >= args.min_len].copy()
    after = len(df_kept)

    report = {
        "rows_before": int(before),
        "rows_after": int(after),
        "removed_short_text": int(removed),
        "min_len": int(args.min_len),
        "text_len_describe": df["text_len"].describe().to_dict(),
        "removed_examples_shortest5": (
            df.sort_values("text_len")[["issue_key","priority","issue_type","status","text_len","text"]]
            .head(5).to_dict(orient="records")
        ),
    }

    df_kept.to_csv(out_csv, index=False)
    out_report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("OK")
    print(f"Rows before: {before}")
    print(f"Rows after : {after}")
    print(f"Removed (<{args.min_len} chars): {removed}")
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_report}")

if __name__ == "__main__":
    main()
