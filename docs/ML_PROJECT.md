# NEPSE ML Project

## Overview

This document describes the **current machine learning system actually implemented** in `C:\nepse-algo`, including:

- the data sources it uses
- the derived ML feature layer
- the current LightGBM training pipeline
- model export and prediction storage
- backend API endpoints
- current performance snapshot
- practical next steps

This is the operational ML project reference for the backend, not just a planning note.

## Project Goal

Build a forward-looking, broker-aware NEPSE decision-support system that:

- learns from broker flow, price, liquidity, and sector context
- predicts short-horizon upside opportunities
- stores daily model scores in the local database
- exposes those scores and model diagnostics through backend APIs
- powers research and frontend ML views in `C:\nepse-algo-ui`

The current system is built around **tabular gradient boosting** rather than deep learning, which is the right starting point for this dataset shape.

## Current ML Architecture

The implemented ML stack currently includes:

- feature engineering from local SQLite tables
- a persistent `ml_feature_snapshot` table
- a LightGBM binary classification training pipeline
- chronological train/validation splitting
- walk-forward window evaluation
- model artifact export
- model registry and daily prediction export
- backend ML endpoints for status, scores, predictions, and walk-forward metrics

Core implementation files:

- `C:\nepse-algo\src\ml\feature_engineering.py`
- `C:\nepse-algo\src\ml\train_lightgbm.py`
- `C:\nepse-algo\src\ml\prediction_export.py`
- `C:\nepse-algo\build_ml_feature_snapshot.py`
- `C:\nepse-algo\train_lightgbm_model.py`
- `C:\nepse-algo\export_lightgbm_predictions.py`
- `C:\nepse-algo\src\api\app.py`

## Source Data Used

The ML pipeline currently derives features from these local tables in `C:\nepse-algo\data\nepse.db`:

- `daily_price`
- `signals`
- `nepse_signals`
- `broker_summary`
- `sector_index`
- `about_company`

The wider project also maintains:

- `price_history`
- `clean_price_history`
- `floor_sheet`
- `market_summary`

Those support the rest of the product and can be expanded into later ML features.

## Derived ML Feature Layer

The current feature builder materializes a table called:

- `ml_feature_snapshot`

This is built from:

- technical indicators
- NEPSE-specific signal scores
- broker aggregate features
- rolling return and volatility features
- sector-relative features
- forward-return labels

Feature engineering implementation:

- `C:\nepse-algo\src\ml\feature_engineering.py`

### Feature Groups

Current implemented feature families include:

#### Base market features

- `close`
- `volume`
- `turnover`
- `trades`
- `change_pct`
- `sector`

#### Technical / signal features

- `rsi14`
- `macd`
- `macd_hist`
- `bb_pct`
- `atr14`
- `volume_ratio`
- `total_score`
- `pump_score`
- `dump_score`
- `liquidity_spike`

#### Broker flow features

- `broker_accumulation`
- `broker_distribution`
- `net_broker_flow`
- `broker_total_buy_qty`
- `broker_total_sell_qty`
- `broker_total_net_qty`
- `broker_total_buy_amount`
- `broker_total_sell_amount`
- `broker_active_count`
- `broker_buy_sell_ratio`
- `broker_top3_buy_amount_share`
- `broker_top5_buy_amount_share`
- `broker_top3_net_qty_share`
- `broker_top5_net_qty_share`
- `broker_avg_trade_size`

#### Sector features

- `sector_change_pct`
- `sector_point_change`
- `sector_index_value`
- `sector_change_ma_5`
- `sector_change_ma_10`
- `sector_strength_rank_pct`

#### Rolling symbol features

- `ret_1d`
- `ret_3d`
- `ret_5d`
- `ret_10d`
- `ret_20d`
- `turnover_ratio_3_20`
- `volume_ratio_5_20`
- `trades_ratio_5_20`
- `volatility_5d`
- `volatility_20d`
- `close_vs_20d_high_pct`
- `close_vs_20d_low_pct`
- `distance_from_ema20_pct`
- `rsi_delta_3d`
- `score_delta_3d`
- `score_ma_5`
- `score_ma_10`
- `net_broker_flow_ma_3`
- `net_broker_flow_ma_5`
- `net_broker_flow_z20`
- `broker_accumulation_ma_3`
- `broker_distribution_ma_3`
- `liquidity_spike_ma_5`
- `pump_score_ma_3`
- `dump_score_ma_3`

## Labels

The current ML snapshot also generates forward-looking labels and regression-style future fields.

Implemented forward outcome fields:

- `fwd_return_3d`
- `fwd_return_5d`
- `fwd_return_10d`
- `fwd_max_upside_5d`
- `fwd_max_drawdown_5d`

Implemented binary labels:

- `label_up_3d_3pct`
- `label_up_5d_4pct`
- `label_up_10d_6pct`
- `label_breakout_5d`

The current production model is trained on:

- `label_up_5d_4pct`

Meaning:

- the model predicts whether a symbol is likely to achieve roughly **4% upside over the next 5 trading days**

## Current Training Pipeline

Training implementation:

- `C:\nepse-algo\src\ml\train_lightgbm.py`

Current design:

- model type: `LightGBMClassifier`
- split type: chronological only
- train/validation split: forward-only
- leakage guard: explicitly blocks target columns from entering the feature set
- additional walk-forward windows are computed after training

Important rules already implemented:

- no random train/test split
- no target leakage through feature selection
- date-ordered split only
- prediction threshold saved with the model

## Current Model Artifact

Latest clean model artifact:

- `C:\nepse-algo\data\models\lightgbm_label_up_5d_4pct_20260326T191433Z.pkl`

Model metadata:

- `C:\nepse-algo\data\models\lightgbm_label_up_5d_4pct_20260326T191433Z.json`

Feature importance:

- `C:\nepse-algo\data\models\lightgbm_label_up_5d_4pct_20260326T191433Z_feature_importance.csv`

## Current Training Snapshot

From the latest saved metadata:

- target: `label_up_5d_4pct`
- threshold: `0.55`
- feature count: `61`
- training rows: `211,627`
- validation rows: `62,202`
- train range: `2022-01-25` to `2025-05-11`
- validation range: `2025-05-13` to `2026-03-26`

### Train metrics

- precision: `0.4917`
- recall: `0.6769`
- accuracy: `0.8049`
- positive rate: `0.1907`
- mean probability: `0.4179`

### Validation metrics

- precision: `0.2641`
- recall: `0.4748`
- accuracy: `0.7152`
- positive rate: `0.1541`
- mean probability: `0.4403`

## Current Walk-Forward Snapshot

Stored walk-forward windows from the latest metadata:

### Window 1

- train end: `2022-11-09`
- test start: `2022-11-10`
- test end: `2023-08-30`
- sample count: `50,077`
- precision: `0.4204`
- recall: `0.0277`
- accuracy: `0.8138`

### Window 2

- train end: `2023-08-30`
- test start: `2023-09-03`
- test end: `2024-07-08`
- sample count: `57,826`
- precision: `0.2382`
- recall: `0.4951`
- accuracy: `0.5476`

### Window 3

- train end: `2024-07-08`
- test start: `2024-07-09`
- test end: `2025-05-11`
- sample count: `60,466`
- precision: `0.3058`
- recall: `0.5765`
- accuracy: `0.6481`

## What The Current Metrics Mean

The model is live and structurally valid, but it is **not yet strong enough to be treated as a trading-ready edge without further work**.

Main observations:

- validation precision is modest
- walk-forward performance is unstable across windows
- the model is learning signal, but not with enough consistency yet
- sector-relative rank is currently dominating feature importance more than ideal

This is useful as a **research / scoring system**, but it still needs stronger feature engineering and better target experimentation before production capital should rely on it.

## Top Current Features

Top saved importance leaders from the latest LightGBM run:

- `sector_strength_rank_pct`
- `close`
- `ret_20d`
- `volatility_20d`
- `macd`
- `ret_10d`
- `trades_ratio_5_20`
- `macd_hist`
- `rsi_delta_3d`
- `close_vs_20d_high_pct`
- `ret_5d`
- `score_ma_10`
- `bb_pct`
- `close_vs_20d_low_pct`
- `ret_3d`

This suggests the current model is using:

- trend/momentum
- relative sector context
- volatility
- some broker participation context

Broker features are present, but they are not yet dominating the model as strongly as the project thesis would ideally want.

## Prediction Export Layer

Daily prediction export implementation:

- `C:\nepse-algo\src\ml\prediction_export.py`

CLI entrypoint:

- `C:\nepse-algo\export_lightgbm_predictions.py`

This layer:

- loads the latest model artifact
- scores the latest available feature snapshot
- writes results into SQLite
- updates model registry records

Current ML persistence tables:

- `ml_model_registry`
- `ml_predictions_daily`

These support:

- latest prediction pages
- model status pages
- symbol score lookups
- versioned model metadata access

## Backend API Surface

The backend already exposes model-backed ML endpoints through:

- `C:\nepse-algo\src\api\app.py`

Implemented routes include:

- `GET /api/ml/status`
- `GET /api/ml/predictions/latest`
- `GET /api/ml/scores/latest`
- `GET /api/ml/predict/<symbol>`
- `GET /api/ml/scores/<symbol>`
- `GET /api/ml/walk-forward`
- `POST /api/ml/export/latest`

These endpoints now serve **real LightGBM-backed values** rather than placeholder ML heuristics.

## CLI Commands

### Build the ML feature snapshot

```powershell
python C:\nepse-algo\build_ml_feature_snapshot.py
```

### Train the LightGBM model

```powershell
python C:\nepse-algo\train_lightgbm_model.py --target label_up_5d_4pct --threshold 0.55
```

### Export latest daily predictions

```powershell
python C:\nepse-algo\export_lightgbm_predictions.py --target label_up_5d_4pct
```

### Run the full daily update including ML export

```powershell
python C:\nepse-algo\daily_update_all_data.py
```

## Current Dependencies

ML-specific dependency file:

- `C:\nepse-algo\requirements-ml.txt`

This supports:

- LightGBM
- scikit-learn
- pandas / numpy based feature prep

## Current Strengths

What is already good:

- forward-only split discipline
- target leakage guard
- persistent ML feature table
- persistent model registry
- persistent daily prediction export
- backend API integration
- frontend can already consume real ML outputs

## Current Weaknesses

What still needs improvement:

- validation precision is still weak
- walk-forward stability is uneven
- current target may not be the best production target
- broker edge is present but not yet dominant enough in the model
- no multi-target benchmark comparison yet
- no formal model registry table documentation or governance document yet

## Recommended Next Steps

Highest-value next steps:

1. Train multiple targets and compare:
   - `label_up_3d_3pct`
   - `label_up_10d_6pct`
   - `label_breakout_5d`

2. Add stronger broker-derived features:
   - repeated broker persistence over 3 to 5 days
   - concentration stability
   - broker participation acceleration
   - symbol-specific broker quality memory

3. Add more floor-sheet derived microstructure features:
   - average trade size trend
   - buyer vs seller diversity
   - last-trade quantity context
   - matched participation concentration

4. Benchmark against simpler models:
   - logistic regression
   - random forest

5. Add walk-forward reporting by target and threshold

6. Add prediction calibration review:
   - bucketed probabilities
   - actual hit rates by confidence band

7. Add drift monitoring:
   - feature drift
   - score drift
   - monthly hit-rate degradation

## Practical Readiness Assessment

Current status:

- **Research-ready:** yes
- **Frontend-integrated:** yes
- **Daily scoring-ready:** yes
- **Production capital-ready:** not yet

The current ML system is a strong foundation:

- it is real
- it is connected end to end
- it is using the live local NEPSE dataset
- it is much more than a placeholder

But it still needs more iteration before it should be trusted as a primary execution signal.

## Related Docs

Existing companion docs:

- `C:\nepse-algo\docs\ML_SYSTEM_IMPLEMENTATION_PACK.md`
- `C:\nepse-algo\docs\ML_FRONTEND_API_CONTRACT.md`

The difference:

- `ML_SYSTEM_IMPLEMENTATION_PACK.md` is the implementation blueprint
- `ML_FRONTEND_API_CONTRACT.md` is the payload contract
- `ML_PROJECT.md` is the **current-state operational reference**

