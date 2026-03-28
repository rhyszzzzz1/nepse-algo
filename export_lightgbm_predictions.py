from __future__ import annotations

import argparse
import json

from src.ml.prediction_export import export_latest_lightgbm_predictions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export latest LightGBM predictions into SQLite.")
    parser.add_argument("--target", default="label_up_5d_4pct", help="Target label the model was trained on.")
    parser.add_argument("--batch-date", default=None, help="Optional batch date to score (YYYY-MM-DD).")
    parser.add_argument("--model-key", default=None, help="Optional explicit model key for registry storage.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = export_latest_lightgbm_predictions(
        target=args.target,
        batch_date=args.batch_date,
        model_key=args.model_key,
    )
    print(json.dumps(result.__dict__, indent=2))
