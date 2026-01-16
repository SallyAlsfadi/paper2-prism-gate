#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Freeze Top-K selection sets from priority-only baseline.")
    ap.add_argument("--in_csv", required=True, help="priority_only_baseline.csv")
    ap.add_argument("--out_dir", required=True, help="Directory to write top-k CSVs")
    ap.add_argument("--ks", default="50,100,250,500", help="Comma-separated K values")
    args = ap.parse_args()

    in_csv = Path(args.in_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_csv, dtype=str)
    df["rank_priority_only"] = pd.to_numeric(df["rank_priority_only"], errors="raise")

    ks = [int(x.strip()) for x in args.ks.split(",") if x.strip()]
    max_k = max(ks)

    if max_k > len(df):
        raise ValueError(f"Requested K={max_k} but dataset has only {len(df)} rows.")

    for k in ks:
        topk = df.nsmallest(k, "rank_priority_only").copy()
        out_path = out_dir / f"baseline_topk_{k}.csv"
        topk.to_csv(out_path, index=False)

    print("OK")
    print(f"Read: {in_csv}")
    print(f"Wrote Top-K sets to: {out_dir}")
    print("Ks:", ks)

if __name__ == "__main__":
    main()
