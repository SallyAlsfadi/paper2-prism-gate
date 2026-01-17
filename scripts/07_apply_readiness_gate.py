#!/usr/bin/env python3
"""
Apply PRISM readiness gate to a priority-only baseline selection.

- Does NOT re-rank or optimize priorities.
- Preserves baseline order; only changes selection composition by deferring not-ready items.
- Supports:
  - LOCKED_GATE_V1: numeric thresholds (C_min, S_min, V_max)
  - LOCKED_GATE_V2: quantile thresholds (S_min_quantile, V_max_quantile) computed from scores scope
"""

import argparse, json
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline_csv", required=True)
    ap.add_argument("--scores_csv", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--ks", default="50,100,250,500")
    ap.add_argument("--gate_cfg", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base = pd.read_csv(args.baseline_csv, dtype=str)
    scores = pd.read_csv(args.scores_csv, dtype=str)

    # Ensure numeric
    for col in ["C", "S", "V"]:
        scores[col] = pd.to_numeric(scores[col], errors="coerce")

    cfg = json.loads(Path(args.gate_cfg).read_text(encoding="utf-8"))
    rule = cfg.get("rule", "LOCKED_GATE_V1")

    # Pre-compute quantile thresholds if needed (declared, not optimized)
    S_q = None
    V_q = None
    if rule == "LOCKED_GATE_V2":
        s_qp = float(cfg["S_min_quantile"])
        v_qp = float(cfg["V_max_quantile"])
        S_q = float(scores["S"].quantile(s_qp))
        V_q = float(scores["V"].quantile(v_qp))

    def gate_row(r):
        reasons = []

        if pd.isna(r["C"]) or pd.isna(r["S"]) or pd.isna(r["V"]):
            return ["missing_scores"]

        if rule == "LOCKED_GATE_V1":
            if r["C"] < float(cfg.get("C_min", 0.0)):
                reasons.append("low_C")
            if r["S"] < float(cfg.get("S_min", 0.0)):
                reasons.append("low_S")
            if r["V"] > float(cfg.get("V_max", 1.0)):
                reasons.append("high_V")

        elif rule == "LOCKED_GATE_V2":
            # Minimal admissibility: defer bottom-decile specificity and top-decile volatility
            if r["S"] < S_q:
                reasons.append("low_S_q")
            if r["V"] > V_q:
                reasons.append("high_V_q")

        else:
            raise ValueError(f"Unknown gate rule: {rule}")

        return reasons

    ks = [int(x) for x in args.ks.split(",") if x.strip()]
    report_all = {"gate_rule": rule}
    if rule == "LOCKED_GATE_V2":
        report_all["thresholds"] = {"S_q": S_q, "V_q": V_q, "S_min_quantile": cfg["S_min_quantile"], "V_max_quantile": cfg["V_max_quantile"]}

    for K in ks:
        topk = base.head(K).copy()

        joined = topk.merge(scores, on="issue_key", how="left")

        joined["gate_reasons"] = joined.apply(
            lambda r: gate_row(r),
            axis=1
        )
        joined["is_ready"] = joined["gate_reasons"].apply(lambda xs: len(xs) == 0)

        ready = joined[joined["is_ready"]].copy()
        deferred = joined[~joined["is_ready"]].copy()

        out_csv = out_dir / f"topk_{K}_gated.csv"
        ready.to_csv(out_csv, index=False)

        # Report
        reason_counts = pd.Series([x for xs in deferred["gate_reasons"] for x in xs]).value_counts().to_dict()

        report = {
            "K": K,
            "baseline_count": int(len(topk)),
            "ready_count": int(len(ready)),
            "deferred_count": int(len(deferred)),
            "deferred_rate": float(len(deferred)) / float(len(topk)) if len(topk) else 0.0,
            "deferred_reason_counts": reason_counts,
        }
        report_all[str(K)] = report

        (out_dir / f"gate_report_topk_{K}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"OK K={K}: ready={report['ready_count']} deferred={report['deferred_count']} wrote {out_csv}")

    (out_dir / "gate_report_all.json").write_text(json.dumps(report_all, indent=2), encoding="utf-8")
    print("OK wrote:", out_dir / "gate_report_all.json")


if __name__ == "__main__":
    main()
