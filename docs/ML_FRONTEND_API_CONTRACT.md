# ML and Frontend API Contract

This file defines the backend payloads the frontend should be designed around before implementation begins.

## Shared Conventions

- all dates use `YYYY-MM-DD`
- numbers are JSON numbers, not strings
- unknown or unavailable values should be `null`
- all responses include a freshness marker where relevant

## 1. ML Status

`GET /api/ml/status`

```json
{
  "status": "ready",
  "active_model": {
    "model_key": "rf_v1",
    "model_type": "random_forest",
    "trained_through": "2026-03-25",
    "created_at": "2026-03-26T02:00:00",
    "feature_set_version": "v1",
    "label_key": "target_up_7d_8pct_no_minus5pct_first"
  },
  "data_freshness": {
    "price_history": "2026-03-25",
    "floor_sheet": "2026-03-25",
    "broker_summary": "2026-03-25",
    "daily_price": "2026-03-25"
  }
}
```

## 2. Latest Predictions

`GET /api/ml/predictions/latest?limit=100&signal=buy`

```json
{
  "date": "2026-03-25",
  "model_key": "rf_v1",
  "items": [
    {
      "symbol": "NABIL",
      "company_name": "Nabil Bank Limited",
      "prediction": "BUY",
      "probability": 0.71,
      "confidence_band": "high",
      "threshold": 0.6,
      "expected_horizon_days": 7,
      "top_drivers": [
        { "feature": "broker_net_qty_5d", "impact": 0.18, "direction": "positive" },
        { "feature": "vol_ratio_5d", "impact": 0.14, "direction": "positive" },
        { "feature": "rsi_14", "impact": 0.09, "direction": "positive" }
      ],
      "snapshot": {
        "close": 539,
        "volume": 133329,
        "sector": "Commercial Banks",
        "market_condition": "bear"
      }
    }
  ]
}
```

## 3. Single Symbol Prediction

`GET /api/ml/predict/<symbol>?date=2026-03-25`

```json
{
  "symbol": "NABIL",
  "date": "2026-03-25",
  "model_key": "rf_v1",
  "prediction": "BUY",
  "probability": 0.71,
  "threshold": 0.6,
  "confidence_band": "high",
  "top_drivers": [
    { "feature": "broker_net_qty_5d", "impact": 0.18, "direction": "positive" },
    { "feature": "vol_ratio_5d", "impact": 0.14, "direction": "positive" },
    { "feature": "market_return_5d", "impact": -0.06, "direction": "negative" }
  ],
  "features": {
    "broker_net_qty_5d": 182340,
    "vol_ratio_5d": 2.41,
    "rsi_14": 56.1,
    "market_return_5d": -0.021
  }
}
```

## 4. Walk-Forward Metrics

`GET /api/ml/walk-forward`

```json
{
  "model_key": "rf_v1",
  "label_key": "target_up_7d_8pct_no_minus5pct_first",
  "windows": [
    {
      "train_start": "2023-08-01",
      "train_end": "2024-09-30",
      "test_start": "2024-10-01",
      "test_end": "2024-12-31",
      "precision": 0.59,
      "recall": 0.48,
      "roc_auc": 0.67,
      "pr_auc": 0.42,
      "n_predictions": 214
    }
  ],
  "aggregate": {
    "precision": 0.58,
    "recall": 0.46,
    "roc_auc": 0.66,
    "pr_auc": 0.4
  }
}
```

## 5. Broker Edge Research

`GET /api/research/broker-edge`

```json
{
  "as_of": "2026-03-25",
  "windows": [
    {
      "lookback_days": 5,
      "forward_days": 7,
      "threshold": 0.6,
      "sample_count": 4821,
      "positive_rate": 0.57,
      "avg_forward_return": 0.034,
      "median_forward_return": 0.018
    }
  ],
  "by_sector": [
    {
      "sector": "Hydro Power",
      "positive_rate": 0.61,
      "avg_forward_return": 0.042
    }
  ]
}
```

## Frontend Integration Priorities

The frontend should be prepared for four top-level surfaces:

- model status and freshness
- ranked prediction list
- symbol-level explanation panel
- research and validation views

