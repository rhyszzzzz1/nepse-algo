from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.db import get_db
from src.ml.feature_engineering import load_ml_feature_snapshot
from src.ml.train_lightgbm import MODEL_DIR


@dataclass(frozen=True)
class PredictionExportResult:
    model_key: str
    target: str
    batch_date: str
    row_count: int
    threshold: float
    model_path: str


def ensure_ml_prediction_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_model_registry (
            model_key TEXT PRIMARY KEY,
            model_type TEXT NOT NULL,
            target TEXT NOT NULL,
            threshold REAL NOT NULL,
            feature_count INTEGER DEFAULT 0,
            trained_through TEXT,
            validation_start TEXT,
            validation_end TEXT,
            validation_precision REAL DEFAULT 0,
            validation_recall REAL DEFAULT 0,
            validation_accuracy REAL DEFAULT 0,
            model_path TEXT NOT NULL,
            meta_path TEXT NOT NULL,
            importance_path TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_predictions_daily (
            batch_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            model_key TEXT NOT NULL,
            prediction TEXT NOT NULL,
            probability REAL NOT NULL,
            confidence_band TEXT NOT NULL,
            rank INTEGER DEFAULT 0,
            expected_horizon_days INTEGER DEFAULT 5,
            threshold REAL NOT NULL,
            top_drivers_json TEXT,
            snapshot_json TEXT,
            features_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (batch_date, symbol, model_key)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ml_predictions_daily_lookup ON ml_predictions_daily (batch_date, model_key, rank)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ml_predictions_daily_symbol ON ml_predictions_daily (symbol, batch_date)"
    )
    conn.commit()


def latest_model_meta(target: str | None = None) -> dict[str, object]:
    if not MODEL_DIR.exists():
        raise FileNotFoundError(f"Model directory not found: {MODEL_DIR}")

    candidates = sorted(MODEL_DIR.glob("lightgbm_*.json"))
    if target:
        candidates = [path for path in candidates if f"lightgbm_{target}_" in path.name]
    if not candidates:
        raise FileNotFoundError("No LightGBM model metadata found. Train a model first.")

    meta_path = candidates[-1]
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["meta_path"] = str(meta_path)
    return meta


def load_model_bundle(model_path: str) -> dict[str, object]:
    with Path(model_path).open("rb") as fh:
        bundle = pickle.load(fh)
    return bundle


def prediction_label(probability: float, threshold: float) -> str:
    if probability >= max(threshold + 0.15, 0.75):
        return "BUY"
    if probability >= threshold:
        return "WATCH"
    if probability >= threshold - 0.1:
        return "HOLD"
    return "IGNORE"


def confidence_band(probability: float) -> str:
    if probability >= 0.75:
        return "high"
    if probability >= 0.6:
        return "medium"
    return "low"


def _clean_float(value: object) -> float:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(val):
        return 0.0
    return val


def build_driver_payload(
    feature_row: pd.Series,
    importance_lookup: dict[str, float],
    feature_order: list[str],
    limit: int = 5,
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for feature in feature_order:
        raw_value = _clean_float(feature_row.get(feature))
        importance = _clean_float(importance_lookup.get(feature))
        if importance <= 0:
            continue
        if abs(raw_value) < 1e-12:
            continue
        score = abs(raw_value) * importance
        items.append(
            {
                "feature": feature,
                "value": round(raw_value, 6),
                "importance": round(importance, 6),
                "score": round(score, 6),
                "direction": "positive" if raw_value >= 0 else "negative",
            }
        )
    items.sort(key=lambda item: item["score"], reverse=True)
    return items[:limit]


def export_latest_lightgbm_predictions(
    target: str = "label_up_5d_4pct",
    batch_date: str | None = None,
    model_key: str | None = None,
) -> PredictionExportResult:
    meta = latest_model_meta(target=target)
    bundle = load_model_bundle(str(meta["model_path"]))
    features = list(bundle["features"])
    threshold = float(bundle.get("threshold", meta.get("threshold", 0.55)))

    feature_df = load_ml_feature_snapshot(target=target, dropna_future=False)
    if feature_df.empty:
        raise ValueError("ml_feature_snapshot is empty. Build features first.")

    latest_date = str(feature_df["date"].max().date())
    target_date = batch_date or latest_date
    score_df = feature_df[feature_df["date"].dt.strftime("%Y-%m-%d") == target_date].copy()
    if score_df.empty:
        raise ValueError(f"No feature rows available for batch_date={target_date}")

    X = score_df[features].fillna(0.0)
    probabilities = bundle["model"].predict_proba(X)[:, 1]
    importance_lookup = {
        item["feature"]: _clean_float(item["importance"])
        for item in meta.get("top_features", [])
    }
    full_importance_path = meta.get("importance_path")
    if full_importance_path and Path(str(full_importance_path)).exists():
        importance_df = pd.read_csv(str(full_importance_path))
        importance_lookup = {
            str(row["feature"]): _clean_float(row["importance"])
            for _, row in importance_df.iterrows()
        }

    score_df["probability"] = probabilities
    score_df["prediction"] = score_df["probability"].map(lambda p: prediction_label(float(p), threshold))
    score_df["confidence_band"] = score_df["probability"].map(lambda p: confidence_band(float(p)))
    score_df = score_df.sort_values(["probability", "symbol"], ascending=[False, True]).reset_index(drop=True)
    score_df["rank"] = np.arange(1, len(score_df) + 1)

    model_key_value = model_key or (
        f"lightgbm_{target}_{Path(str(meta['model_path'])).stem.split('_')[-1]}"
    )

    rows: list[tuple[object, ...]] = []
    for _, row in score_df.iterrows():
        snapshot = {
            "date": target_date,
            "close": _clean_float(row.get("close")),
            "volume": _clean_float(row.get("volume")),
            "turnover": _clean_float(row.get("turnover")),
            "trades": _clean_float(row.get("trades")),
            "sector": row.get("sector"),
        }
        feature_snapshot = {
            "total_score": _clean_float(row.get("total_score")),
            "rsi14": _clean_float(row.get("rsi14")),
            "macd": _clean_float(row.get("macd")),
            "broker_accumulation": _clean_float(row.get("broker_accumulation")),
            "broker_distribution": _clean_float(row.get("broker_distribution")),
            "net_broker_flow": _clean_float(row.get("net_broker_flow")),
            "sector_strength_rank_pct": _clean_float(row.get("sector_strength_rank_pct")),
        }
        drivers = build_driver_payload(row, importance_lookup, features, limit=5)
        rows.append(
            (
                target_date,
                str(row["symbol"]).upper(),
                model_key_value,
                str(row["prediction"]),
                round(_clean_float(row["probability"]), 6),
                str(row["confidence_band"]),
                int(row["rank"]),
                5,
                round(threshold, 6),
                json.dumps(drivers, separators=(",", ":")),
                json.dumps(snapshot, separators=(",", ":")),
                json.dumps(feature_snapshot, separators=(",", ":")),
            )
        )

    conn = get_db()
    try:
        ensure_ml_prediction_tables(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO ml_model_registry (
                model_key, model_type, target, threshold, feature_count,
                trained_through, validation_start, validation_end,
                validation_precision, validation_recall, validation_accuracy,
                model_path, meta_path, importance_path, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                model_key_value,
                "lightgbm_classifier",
                str(meta["target"]),
                round(threshold, 6),
                int(meta.get("feature_count", len(features))),
                str(meta.get("train_end") or meta.get("validation_end") or ""),
                str(meta.get("validation_start") or ""),
                str(meta.get("validation_end") or ""),
                _clean_float((meta.get("validation_metrics") or {}).get("precision")),
                _clean_float((meta.get("validation_metrics") or {}).get("recall")),
                _clean_float((meta.get("validation_metrics") or {}).get("accuracy")),
                str(meta["model_path"]),
                str(meta["meta_path"]),
                str(meta.get("importance_path") or ""),
            ),
        )
        conn.execute(
            "DELETE FROM ml_predictions_daily WHERE batch_date = ? AND model_key = ?",
            (target_date, model_key_value),
        )
        conn.executemany(
            """
            INSERT INTO ml_predictions_daily (
                batch_date, symbol, model_key, prediction, probability, confidence_band,
                rank, expected_horizon_days, threshold, top_drivers_json, snapshot_json, features_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    return PredictionExportResult(
        model_key=model_key_value,
        target=str(meta["target"]),
        batch_date=target_date,
        row_count=len(rows),
        threshold=threshold,
        model_path=str(meta["model_path"]),
    )
