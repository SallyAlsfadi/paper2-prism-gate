#!/usr/bin/env python3
"""
Run PRISM readiness scoring (Criticality, Specificity, Volatility) for AIRFLOW.

- Computes readiness signals ONLY (C, S, V)
- Skips steps if outputs already exist
- Merges using req_id (mapped 1:1 to issue_key)
"""

import subprocess
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PRISM = ROOT / "prism" / "scripts"

INPUT_CSV = ROOT / "data" / "processed" / "airflow_for_prism_top500.csv"
OUTDIR = ROOT / "data" / "processed" / "prism_scores_tmp"
FINAL_OUT = ROOT / "data" / "processed" / "prism_scores_airflow.csv"


def run(cmd):
    print("RUN:", " ".join(cmd))
    subprocess.check_call(cmd)


def pick_col(df, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(f"None of {candidates} found in columns: {list(df.columns)}")


def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)

    crit_path = OUTDIR / "criticality_scored.csv"
    spec_path = OUTDIR / "specificity_scores.csv"
    vol_path = OUTDIR / "volatility_scores.csv"

    # 1) Criticality
    if not crit_path.exists():
        run([
            "python3", str(PRISM / "08_score_criticality.py"),
            "--input", str(INPUT_CSV),
            "--outdir", str(OUTDIR),
            "--embed_mode", "model",
        ])
    else:
        print(f"SKIP: criticality already exists at {crit_path}")

    # 2) Specificity
    if not spec_path.exists():
        run([
            "python3", str(PRISM / "09_score_specificity.py"),
            "--input", str(INPUT_CSV),
            "--out_scores", str(spec_path),
            "--out_meta", str(OUTDIR / "specificity_meta.csv"),
        ])
    else:
        print(f"SKIP: specificity already exists at {spec_path}")

    # 3) Volatility
    if not vol_path.exists():
        run([
            "python3", str(PRISM / "10_score_volatility.py"),
            "--input", str(INPUT_CSV),
            "--out_scores", str(vol_path),
            "--out_meta", str(OUTDIR / "volatility_meta.csv"),
            "--embed_mode", "onfly",
        ])
    else:
        print(f"SKIP: volatility already exists at {vol_path}")

    # 4) Merge scores (req_id → issue_key)
    c = pd.read_csv(crit_path)
    s = pd.read_csv(spec_path)
    v = pd.read_csv(vol_path)

    c_col = pick_col(c, ["Criticality", "criticality", "C", "criticality_score"])
    s_col = pick_col(s, ["S_final_sys", "specificity", "S", "specificity_score"])
    v_col = pick_col(v, ["V_final", "volatility", "V", "volatility_score"])

    merged = (
        c[["req_id", c_col]].rename(columns={c_col: "C"})
        .merge(s[["req_id", s_col]].rename(columns={s_col: "S"}), on="req_id", how="inner")
        .merge(v[["req_id", v_col]].rename(columns={v_col: "V"}), on="req_id", how="inner")
        .rename(columns={"req_id": "issue_key"})
    )

    merged.to_csv(FINAL_OUT, index=False)

    print("OK")
    print(f"Wrote final PRISM scores to: {FINAL_OUT}")
    print(f"Rows: {len(merged)}")


if __name__ == "__main__":
    main()
