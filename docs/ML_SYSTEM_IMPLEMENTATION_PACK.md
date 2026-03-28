# NEPSE ML System Implementation Pack

## Goal

Turn the current NEPSE data pipeline into a production-grade decision-support system that:

- validates whether broker flow has predictive power
- establishes a rules-based baseline
- establishes a Random Forest baseline
- optionally advances to gradient boosting and neural networks
- exposes outputs through backend APIs for `C:\nepse-algo-ui`

This document is the execution blueprint for the backend repo at `C:\nepse-algo`.

## Current Data Position

The project already has the critical raw ingredients:

- `price_history`
- `clean_price_history`
- `floor_sheet`
- `broker_summary`
- `daily_price`
- `market_summary`
- `sector_index`
- `signals`
- `nepse_signals`
- parquet exports for analytics workloads

This is enough to begin edge validation safely.

## Non-Negotiable Modeling Rules

1. Do not use random train/test splits for predictive trading tasks.
2. Use forward-only chronology.
3. Treat transaction-level rows as raw evidence, not as independent labels.
4. Build labels from future outcomes using only information available as of decision time.
5. Include cost and slippage assumptions before claiming signal quality.
6. Require a rules baseline before accepting ML complexity.
7. Prefer tabular baselines before neural nets.

## Recommended Build Order

### Phase 1: Data Quality and Edge Validation

Deliverables:

- broker-flow correlation study
- symbol/date coverage audit
- instrument-universe audit
- leakage audit
- label-definition signoff

Core questions:

- when broker net accumulation is strong over a 3 to 5 day window, what happens to forward returns over the next 3, 5, 7, and 10 trading days?
- does signal quality hold by sector, liquidity bucket, and volatility regime?
- does it hold on forward windows, not just pooled history?

Suggested outputs:

- `data/research/broker_correlation_summary.parquet`
- `data/research/broker_correlation_by_symbol.parquet`
- `data/research/broker_correlation_by_regime.parquet`
- `reports/broker_edge_validation.md`

### Phase 2: Rules Baseline

Deliverables:

- simple rules strategy based on broker accumulation and liquidity
- walk-forward backtest
- baseline precision, recall, expectancy, drawdown

Example baseline logic:

- `BUY` if:
  - broker accumulation ratio >= threshold
  - volume ratio >= threshold
  - close above short EMA
- otherwise `HOLD`

Baseline success criteria:

- forward hit rate above naive benchmark
- performance remains positive after fees/slippage
- no major collapse across windows

### Phase 3: Random Forest Baseline

Deliverables:

- feature table
- label table
- walk-forward training script
- model artifact
- feature importance report

Reason to start here:

- easier to debug than a neural net
- strong on tabular problems
- gives immediate signal on whether the feature set is useful

### Phase 4: Gradient Boosting

Preferred next model:

- XGBoost or LightGBM style model

Why:

- often better than RF on structured financial data
- handles non-linear interactions cleanly
- easier to interpret than deep nets

### Phase 5: Neural Network

Only after the above are stable.

Neural net is justified if:

- RF / boosting show real but interaction-heavy signal
- walk-forward performance is stable
- feature volume is large enough to justify added complexity

## Backend Module Plan

Create these modules under `C:\nepse-algo\src\ml`:

- `features.py`
- `labels.py`
- `datasets.py`
- `rules_baseline.py`
- `random_forest.py`
- `gradient_boosting.py`
- `neural_net.py`
- `walk_forward.py`
- `metrics.py`
- `explainability.py`
- `registry.py`
- `inference.py`

Recommended support modules:

- `C:\nepse-algo\src\research\broker_edge.py`
- `C:\nepse-algo\src\research\instrument_audit.py`
- `C:\nepse-algo\src\research\label_sanity.py`

## Feature Groups

Build one row per `symbol, decision_date`.

### Technical Features

- `rsi_14`
- `macd`
- `macd_signal`
- `macd_hist`
- `ema_10_gap`
- `ema_20_gap`
- `ema_50_gap`
- `atr_14`
- `stoch_k`
- `stoch_d`
- `roc_5`
- `roc_10`
- `bb_pos`

### Broker Features

- `broker_net_qty_1d`
- `broker_net_qty_3d`
- `broker_net_qty_5d`
- `broker_net_amount_1d`
- `broker_net_amount_3d`
- `broker_net_amount_5d`
- `broker_accumulation_days_5d`
- `broker_distribution_days_5d`
- `top5_broker_concentration`
- `broker_buy_sell_imbalance`
- `broker_count_active`

### Price and Liquidity Features

- `ret_1d`
- `ret_3d`
- `ret_5d`
- `ret_10d`
- `vol_ratio_5d`
- `vol_ratio_20d`
- `vwap_gap`
- `turnover_zscore`
- `realized_vol_5d`
- `realized_vol_20d`
- `intraday_range_pct`

### Context Features

- `sector_return_5d`
- `sector_strength_rank`
- `market_return_3d`
- `market_return_5d`
- `market_vol_regime`

## Label Design

Use multiple labels, not just one.

Primary candidate labels:

- `target_up_5d_5pct`
- `target_up_7d_8pct`
- `target_up_10d_10pct`

Risk-aware variants:

- `target_up_5d_5pct_no_minus3pct_first`
- `target_up_7d_8pct_no_minus5pct_first`

Regression targets:

- `fwd_return_3d`
- `fwd_return_5d`
- `fwd_return_7d`
- `max_upside_7d`
- `max_drawdown_7d`

Do not finalize model choice before comparing label stability.

## Validation Design

Use walk-forward windows such as:

1. Train: 2023-08 to 2024-06, Validate: 2024-07 to 2024-09
2. Train: 2023-08 to 2024-09, Validate: 2024-10 to 2024-12
3. Train: 2023-08 to 2024-12, Validate: 2025-01 to 2025-03
4. Continue rolling forward

Track:

- precision
- recall
- ROC-AUC
- PR-AUC
- hit rate after fees
- average forward return
- max drawdown
- calibration quality

## Production API Surface

Add these backend endpoints to `C:\nepse-algo\src\api\app.py` after the model stack exists:

- `GET /api/ml/status`
- `GET /api/ml/features/<symbol>`
- `GET /api/ml/feature-importance`
- `POST /api/ml/predict`
- `GET /api/ml/predictions/latest`
- `GET /api/ml/model-metrics`
- `GET /api/ml/walk-forward`
- `GET /api/research/broker-edge`
- `GET /api/research/regimes`

## Deployment Milestones

### Milestone 1

Research only:

- edge validation complete
- no frontend changes required beyond charts/tables for diagnostics

### Milestone 2

Rules + RF:

- expose diagnostics in API
- expose prediction confidence
- expose top drivers

### Milestone 3

Production ML:

- scheduled retraining
- model versioning
- monitoring and drift checks

## Acceptance Criteria

The system is only ready for frontend integration when:

- label definition is frozen
- walk-forward evaluation is repeatable
- API contracts are stable
- model metrics are available by version
- prediction endpoint returns confidence and top drivers
- stale-data handling is explicit

## Frontend Handoff

The companion frontend integration spec lives at:

- `C:\nepse-algo-ui\docs\FRONTEND_ML_INTEGRATION_SPEC.md`
- `C:\nepse-algo-ui\docs\CURSOR_FRONTEND_PROMPT.md`

