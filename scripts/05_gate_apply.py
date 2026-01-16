#!/usr/bin/env python3
"""
Apply PRISM readiness gate to a priority-based selection set.

IMPORTANT: This script MUST NOT rank or optimize priorities.
It only assigns gate labels (ELIGIBLE / INSPECT / DEFER) based on readiness signals.
"""
import argparse
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selection_csv", required=True, help="One baseline_topk_K.csv")
    ap.add_argument("--scores_csv", required=True, help="PRISM scores file with issue_key,C,S,V (and optional reasons)")
    ap.add_argument("--out_csv", required=True, help="Gate-annotated output CSV")
    args = ap.parse_args()

    sel = pd.read_csv(args.selection_csv, dtype=str)
    scores = pd.read_csv(args.scores_csv, dtype=str)

    # expected columns in scores (we will enforce once we have the real file)
    # issue_key, C, S, V, (optional: reason_C, reason_S, reason_V)
    merged = sel.merge(scores, on="issue_key", how="left", validate="one_to_one")

    # sanity: do not proceed if scores missing
    if merged[["C", "S", "V"]].isna().any().any():
        missing = merged[merged["C"].isna()]["issue_key"].head(20).tolist()
        raise ValueError(f"Missing PRISM scores for some issues. Example keys: {missing}")

    # TODO (Step 10.2): implement gate logic you already locked.
    # For now, just create placeholder columns.
    merged["set_name"] = Path(args.selection_csv).stem
    merged["gate_label"] = "TODO"
    merged["gate_reason"] = ""

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)

    print("OK")
    print(f"Wrote: {out_path}")
    print("NOTE: gate_label is TODO until we wire the locked gate logic.")

if __name__ == "__main__":
    main()
