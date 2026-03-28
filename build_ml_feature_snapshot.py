from __future__ import annotations

import json

from src.ml.feature_engineering import materialize_ml_feature_snapshot


if __name__ == "__main__":
    result = materialize_ml_feature_snapshot(rebuild=True)
    print(json.dumps({
        "row_count": result.row_count,
        "symbol_count": result.symbol_count,
        "date_min": result.date_min,
        "date_max": result.date_max,
    }, indent=2))
