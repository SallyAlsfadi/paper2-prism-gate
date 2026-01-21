#!/usr/bin/env python3
import argparse
import pandas as pd

REQ_08 = {"system", "class"}
REQ_09 = {"req_id", "system", "class", "source"}
REQ_10 = {"req_id", "system", "class"}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str)
    cols = set(df.columns)

    print("OK: loaded", len(df), "rows")
    print("Missing for 08_score_criticality:", sorted(REQ_08 - cols))
    print("Missing for 09_score_specificity:", sorted(REQ_09 - cols))
    print("Missing for 10_score_volatility:", sorted(REQ_10 - cols))

    if (REQ_08 - cols) or (REQ_09 - cols) or (REQ_10 - cols):
        raise SystemExit(2)

if __name__ == "__main__":
    main()
