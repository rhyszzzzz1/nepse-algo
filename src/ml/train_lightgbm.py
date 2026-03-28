from __future__ import annotations

import json
import pickle
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.ml.feature_engineering import feature_columns, load_ml_feature_snapshot


MODEL_DIR = Path("C:/nepse-algo/data/models")


@dataclass(frozen=True)
class WindowMetrics:
    train_end: str
    test_start: str
    test_end: str
    sample_count: int
    positive_rate: float
    precision: float
    recall: float
    accuracy: float
    mean_probability: float


def _require_lightgbm():
    try:
        import lightgbm as lgb  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime dependency message
        raise SystemExit(
            "lightgbm is not installed. Add it with `pip install lightgbm` or install from requirements.txt first."
        ) from exc
    return lgb


def _safe_metric_ratio(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


def binary_metrics(y_true: np.ndarray, probs: np.ndarray, threshold: float = 0.5) -> dict[str, float]:
    preds = (probs >= threshold).astype(int)
    tp = float(((preds == 1) & (y_true == 1)).sum())
    tn = float(((preds == 0) & (y_true == 0)).sum())
    fp = float(((preds == 1) & (y_true == 0)).sum())
    fn = float(((preds == 0) & (y_true == 1)).sum())
    precision = _safe_metric_ratio(tp, tp + fp)
    recall = _safe_metric_ratio(tp, tp + fn)
    accuracy = _safe_metric_ratio(tp + tn, len(y_true))
    return {
        "precision": precision,
        "recall": recall,
        "accuracy": accuracy,
        "positive_rate": float(np.mean(y_true)) if len(y_true) else 0.0,
        "mean_probability": float(np.mean(probs)) if len(probs) else 0.0,
    }


def chronological_split(df: pd.DataFrame, val_fraction: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordered_dates = np.array(sorted(df["date"].dt.strftime("%Y-%m-%d").unique()))
    if len(ordered_dates) < 10:
        raise ValueError("Not enough unique dates to perform a chronological split.")
    split_idx = max(int(len(ordered_dates) * (1.0 - val_fraction)), 1)
    split_date = ordered_dates[split_idx]
    train_df = df[df["date"].dt.strftime("%Y-%m-%d") < split_date].copy()
    val_df = df[df["date"].dt.strftime("%Y-%m-%d") >= split_date].copy()
    if train_df.empty or val_df.empty:
        raise ValueError("Chronological split produced an empty train or validation set.")
    return train_df, val_df


def walk_forward_windows(df: pd.DataFrame, target: str, threshold: float = 0.5, n_windows: int = 3) -> list[WindowMetrics]:
    ordered_dates = sorted(df["date"].dt.strftime("%Y-%m-%d").unique())
    if len(ordered_dates) < 30:
        return []
    step = max(len(ordered_dates) // (n_windows + 2), 10)
    lgb = _require_lightgbm()
    feats = feature_columns(target)
    if target in feats:
        raise ValueError(f"Target leakage detected: feature set still contains target column '{target}'.")
    windows: list[WindowMetrics] = []

    for i in range(n_windows):
        train_end_idx = min(step * (i + 1), len(ordered_dates) - step - 1)
        test_end_idx = min(train_end_idx + step, len(ordered_dates) - 1)
        train_end = ordered_dates[train_end_idx]
        test_start = ordered_dates[train_end_idx + 1]
        test_end = ordered_dates[test_end_idx]
        train_df = df[df["date"].dt.strftime("%Y-%m-%d") <= train_end].copy()
        test_df = df[
            (df["date"].dt.strftime("%Y-%m-%d") >= test_start)
            & (df["date"].dt.strftime("%Y-%m-%d") <= test_end)
        ].copy()
        if train_df.empty or test_df.empty:
            continue
        model = lgb.LGBMClassifier(
            objective="binary",
            n_estimators=250,
            learning_rate=0.05,
            num_leaves=31,
            subsample=0.85,
            colsample_bytree=0.85,
            min_child_samples=30,
            reg_alpha=0.1,
            reg_lambda=0.2,
            is_unbalance=True,
            random_state=42,
            n_jobs=-1,
            verbosity=-1,
        )
        model.fit(train_df[feats], train_df[target])
        probs = model.predict_proba(test_df[feats])[:, 1]
        metrics = binary_metrics(test_df[target].to_numpy(dtype=int), probs, threshold=threshold)
        windows.append(
            WindowMetrics(
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                sample_count=int(len(test_df)),
                positive_rate=metrics["positive_rate"],
                precision=metrics["precision"],
                recall=metrics["recall"],
                accuracy=metrics["accuracy"],
                mean_probability=metrics["mean_probability"],
            )
        )
    return windows


def train_lightgbm_model(
    target: str = "label_up_5d_4pct",
    threshold: float = 0.55,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, object]:
    lgb = _require_lightgbm()
    df = load_ml_feature_snapshot(target=target, start_date=start_date, end_date=end_date)
    if df.empty:
        raise ValueError("ml_feature_snapshot is empty. Build features first.")

    feats = feature_columns(target)
    if target in feats:
        raise ValueError(f"Target leakage detected: feature set still contains target column '{target}'.")
    train_df, val_df = chronological_split(df, val_fraction=0.2)

    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=400,
        learning_rate=0.03,
        num_leaves=47,
        max_depth=-1,
        subsample=0.85,
        colsample_bytree=0.85,
        min_child_samples=35,
        reg_alpha=0.15,
        reg_lambda=0.25,
        is_unbalance=True,
        random_state=42,
        n_jobs=-1,
        verbosity=-1,
    )
    model.fit(
        train_df[feats],
        train_df[target],
        eval_set=[(val_df[feats], val_df[target])],
        eval_metric="binary_logloss",
        callbacks=[],
    )

    train_probs = model.predict_proba(train_df[feats])[:, 1]
    val_probs = model.predict_proba(val_df[feats])[:, 1]
    train_metrics = binary_metrics(train_df[target].to_numpy(dtype=int), train_probs, threshold=threshold)
    val_metrics = binary_metrics(val_df[target].to_numpy(dtype=int), val_probs, threshold=threshold)

    importances = pd.DataFrame(
        {
            "feature": feats,
            "importance": model.feature_importances_,
        }
    ).sort_values("importance", ascending=False)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base_name = f"lightgbm_{target}_{stamp}"
    model_path = MODEL_DIR / f"{base_name}.pkl"
    meta_path = MODEL_DIR / f"{base_name}.json"
    importance_path = MODEL_DIR / f"{base_name}_feature_importance.csv"

    with model_path.open("wb") as fh:
        pickle.dump(
            {
                "model": model,
                "features": feats,
                "target": target,
                "threshold": threshold,
            },
            fh,
        )

    walk_forward = walk_forward_windows(df, target=target, threshold=threshold, n_windows=3)
    metadata = {
        "target": target,
        "threshold": threshold,
        "feature_count": len(feats),
        "training_rows": int(len(train_df)),
        "validation_rows": int(len(val_df)),
        "train_start": str(train_df["date"].min().date()),
        "train_end": str(train_df["date"].max().date()),
        "validation_start": str(val_df["date"].min().date()),
        "validation_end": str(val_df["date"].max().date()),
        "train_metrics": train_metrics,
        "validation_metrics": val_metrics,
        "top_features": importances.head(25).to_dict(orient="records"),
        "walk_forward": [asdict(item) for item in walk_forward],
        "model_path": str(model_path),
        "importance_path": str(importance_path),
    }
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    importances.to_csv(importance_path, index=False)

    return {
        "model_path": str(model_path),
        "meta_path": str(meta_path),
        "importance_path": str(importance_path),
        "train_metrics": train_metrics,
        "validation_metrics": val_metrics,
        "top_features": importances.head(15).to_dict(orient="records"),
        "walk_forward": [asdict(item) for item in walk_forward],
    }
