from __future__ import annotations

import argparse
import json

from src.ml.train_lightgbm import train_lightgbm_model


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a LightGBM model on the materialized ml_feature_snapshot table.")
    parser.add_argument("--target", default="label_up_5d_4pct", help="Target label column to train on.")
    parser.add_argument("--threshold", type=float, default=0.55, help="Probability threshold for evaluation metrics.")
    parser.add_argument("--start-date", default=None, help="Optional inclusive lower date bound (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=None, help="Optional inclusive upper date bound (YYYY-MM-DD).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = train_lightgbm_model(
        target=args.target,
        threshold=args.threshold,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(json.dumps(result, indent=2))
