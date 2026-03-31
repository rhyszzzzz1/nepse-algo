# NEPSE Quant Audit Response

## Purpose

This document responds to the latest external-style quant assessment of the NEPSE project and maps it to the **actual current repo state** as of March 2026.

It is intended to answer four practical questions:

1. What did the audit get right?
2. What has already been fixed in the codebase?
3. What still needs work before production trading confidence is justified?
4. What should be executed over the next two weeks?

This is not a defensive document. It is a working engineering response.

## Overall Position

The current project should be viewed as:

- **strong as an engineering and research platform**
- **real as an ML and analytics system**
- **not yet fully validated as a live-capital trading system**

The audit is directionally correct in its warnings, but parts of it are already outdated because the repo now includes:

- a persistent ML feature layer
- LightGBM training
- walk-forward evaluation
- model export and prediction storage
- ML API endpoints
- frontend integration of real model outputs

So the right summary is:

- the project is **no longer a toy**
- but it is **still short of capital-deployment proof**

## What The Audit Got Right

The audit’s strongest valid points are below.

### 1. Edge validation still needs to be made explicit

This is the most important unresolved issue.

The current ML system is learning from broker, price, liquidity, and sector features, but the repo still needs a first-class research output that explicitly proves:

- whether broker accumulation predicts forward returns
- how strong that relationship is
- where it holds
- where it breaks

That means:

- by sector
- by liquidity bucket
- by volatility regime
- by period

The model can exist before this, but production confidence should not.

### 2. Transaction cost and slippage modeling are still underdeveloped

The audit is right that the project still needs stronger realism around:

- brokerage cost
- slippage
- liquidity constraints
- capacity assumptions

This matters especially in NEPSE because:

- many symbols are thin
- broker concentration can matter
- getting the exact open or last price is unrealistic in several names

### 3. Portfolio-level realism is still missing

The audit is also right that single-signal or symbol-level evaluation is not enough.

A live portfolio needs:

- concurrent positions
- capital allocation rules
- correlation awareness
- portfolio drawdown tracking
- position caps
- exposure limits by sector and signal family

Without this, symbol-level quality may still produce poor portfolio behavior.

### 4. Validation precision is not strong enough yet

From the current model metadata:

- train precision: `0.4917`
- validation precision: `0.2641`

That is a meaningful drop.

So while the ML system is real and functioning, the audit is correct that:

- the current model is not yet strong enough for blind trust
- overfitting or instability is still a live concern

### 5. Risk-management and deployment hardening still need work

The current backend is significantly more capable than before, but the audit is still right that the project should improve:

- logging
- request validation
- risk framework
- portfolio simulation
- operational monitoring

## What Is Already Fixed

This section highlights places where the audit would be unfair if read as a statement of the current repo.

### 1. ML is implemented

This is no longer a concept-only system.

The backend now includes:

- `C:\nepse-algo\src\ml\feature_engineering.py`
- `C:\nepse-algo\src\ml\train_lightgbm.py`
- `C:\nepse-algo\src\ml\prediction_export.py`

Supporting entrypoints:

- `C:\nepse-algo\build_ml_feature_snapshot.py`
- `C:\nepse-algo\train_lightgbm_model.py`
- `C:\nepse-algo\export_lightgbm_predictions.py`

This means:

- features are built
- models are trained
- predictions are exported
- artifacts are versioned

### 2. Walk-forward evaluation exists

The audit’s earlier framing that walk-forward was missing is no longer true.

Current training code includes:

- chronological split
- explicit walk-forward windows
- saved walk-forward metrics inside the model metadata

The existing implementation is in:

- `C:\nepse-algo\src\ml\train_lightgbm.py`

It may need to be expanded, but it does exist.

### 3. Target leakage protection exists

The repo now includes explicit target leakage protection.

The training code checks that the target label is not accidentally included in the feature set before model fitting.

This is an important and non-trivial correctness improvement.

### 4. Derived analytics are now substantial

The system now has a broad derived analytics layer beyond raw backtests.

Examples:

- `stock_factor_snapshot`
- `sector_factor_snapshot`
- `support_resistance_levels`
- `broker_analytics_daily`
- `scanner_results_daily`

This is a major step up from a simple indicator toy project.

### 5. ML scores are stored and served through APIs

The backend now supports real ML serving through:

- `GET /api/ml/status`
- `GET /api/ml/predictions/latest`
- `GET /api/ml/scores/latest`
- `GET /api/ml/predict/<symbol>`
- `GET /api/ml/scores/<symbol>`
- `GET /api/ml/walk-forward`

This is not a mock ML layer anymore.

### 6. Frontend ML integration is real

The frontend is not relying only on placeholder logic now.

It can consume:

- model status
- prediction batch dates
- validation metrics
- symbol-level score outputs
- walk-forward metrics

So on architecture and product integration, the project is ahead of what the audit implies.

### 7. MeroShare and portfolio integration now exist

The project now also includes:

- MeroShare account connection
- holdings sync
- import into portfolio tables

This expands the system from a pure analytics terminal toward a real portfolio-aware workflow.

## What Still Needs Work

This is the most important section operationally.

These are the gaps that still matter before live-trading confidence should be high.

### 1. Broker-edge validation research needs to become first-class

The current repo should add a dedicated broker-edge research module that explicitly computes:

- correlation between broker accumulation and forward returns
- conditional positive rates
- return distributions after accumulation thresholds
- results by sector
- results by volatility regime
- results by time period

Recommended outputs:

- research parquet tables
- a persisted summary table
- a markdown report in `docs/` or `reports/`

### 2. Cost-aware evaluation needs to be added to the ML system

Right now the ML stack mostly evaluates pure classification outcomes.

It should also evaluate:

- net expected return after brokerage
- cost-adjusted hit rate
- slippage-adjusted target success
- turnover burden of high-frequency signals

This should appear in:

- walk-forward reports
- model metadata
- research dashboards

### 3. Slippage and liquidity constraints need explicit modeling

The project already has several liquidity signals, but that is not the same as execution modeling.

Add a slippage model that depends on:

- turnover
- volume
- trade count
- liquidity score
- maybe symbol bucket

The model does not need to be perfect at first; it needs to be explicit and consistent.

### 4. Portfolio-level simulation is still missing

The current pipeline is still much stronger at symbol-level scoring than portfolio-level truth.

Needed capabilities:

- multiple concurrent positions
- max positions
- sector caps
- score-based position ranking
- capital deployment rules
- daily equity curve
- drawdown metrics
- portfolio-level Sharpe-like stability metrics

### 5. Multi-target benchmarking is not complete

The system already has multiple labels in the feature snapshot, but the current live artifact is mainly built around:

- `label_up_5d_4pct`

The next step is to compare:

- `label_up_3d_3pct`
- `label_up_10d_6pct`
- `label_breakout_5d`

And decide which target is:

- most stable
- most economically useful
- most robust in walk-forward

### 6. Walk-forward robustness should be expanded

The current implementation uses 3 windows.

That is useful, but not enough for high confidence.

Recommended improvement:

- increase the number of windows
- produce a stable report over more periods
- compare feature importance across windows
- check degradation by date

### 7. Internal quality still needs strengthening

The project has improved externally visible docs a lot, but it still needs:

- better internal docstrings
- stronger logging
- more targeted tests
- stronger request validation in the expanding API surface

This is less important than edge validation, but still important for long-term maintainability.

## Current Honest Rating

As of the current repo state:

- engineering and platform quality: **high**
- research maturity: **strong**
- ML implementation maturity: **real and usable**
- live-capital confidence: **not yet proven**

Practical interpretation:

- **about 8/10 as a serious research and product system**
- **not yet at the same confidence level for production trading deployment**

That is a meaningful difference.

## Execution Plan For The Next 2 Weeks

This plan is intended to turn the project from “strong research platform” into “validated trading research system.”

### Week 1: Edge Validation and Cost Realism

#### 1. Broker-edge correlation study

Deliver:

- broker accumulation vs forward return summary
- conditional success rates by threshold
- sector breakdown
- regime breakdown

Build:

- `src/research/broker_edge.py`
- persisted output tables or parquet
- `docs/BROKER_EDGE_VALIDATION.md`

#### 2. Rules baseline report

Before trusting ML, create a simple benchmark strategy that uses:

- broker accumulation
- liquidity
- price trend

Then measure it on forward data.

Goal:

- determine whether the ML is improving on something real

#### 3. Add transaction costs to evaluation

Add cost-aware scoring to the research pipeline:

- brokerage
- optional fee assumptions

Outputs:

- raw metrics
- cost-adjusted metrics

#### 4. Add a first slippage approximation

Simple first-pass model:

- liquid symbols: low slippage
- mid liquidity: medium slippage
- thin symbols: high slippage

Then rerun evaluation.

### Week 2: Robustness and Portfolio Reality

#### 5. Expand walk-forward evaluation

Increase beyond the current 3 windows and add:

- by-window precision stability
- by-window recall stability
- by-window feature importance drift

#### 6. Multi-target benchmark run

Train and compare at least:

- `label_up_3d_3pct`
- `label_up_5d_4pct`
- `label_up_10d_6pct`
- `label_breakout_5d`

Choose a preferred production research target based on:

- stability
- precision
- cost-adjusted usefulness

#### 7. Build first portfolio-level simulator

Requirements:

- max concurrent positions
- simple capital allocation
- equity curve
- drawdown
- symbol overlap control

This does not need to be perfect yet, but it must exist.

#### 8. Add model-quality diagnostics document

Write a short report after reruns:

- what target won
- how much precision survives after costs
- whether broker edge remains after validation
- whether the model is stable enough for paper trading

## Suggested Deliverables

At the end of the next two weeks, the repo should ideally contain:

- `C:\nepse-algo\docs\BROKER_EDGE_VALIDATION.md`
- `C:\nepse-algo\docs\MODEL_STABILITY_REPORT.md`
- `C:\nepse-algo\docs\PORTFOLIO_SIMULATION_PLAN.md`
- a cost-aware evaluation extension in the ML pipeline
- a portfolio-level backtest or simulation module

## Final Position

The project has made major progress in the right direction.

The system is now:

- technically real
- meaningfully integrated
- strong enough to support real research iteration

What remains is the hard part that every serious quant system must face:

- proving the edge
- modeling the frictions
- validating the portfolio behavior

That is the right place to be.

The next milestone is no longer “build the ML system.”

It is:

- **prove that the ML system is economically valid**

