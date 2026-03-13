# config.py
# Central settings for the NEPSE Algorithmic Trading System.
# All modules import from here — no more scattered magic numbers.
#
# Override any value by setting the matching env variable in .env

import os
from pathlib import Path

# Try to load .env; silently skip if python-dotenv not available yet
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# ── DATABASE ──────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).parent               # c:\nepse-algo
DB_PATH = str(ROOT / "data" / "nepse.db")    # absolute, safe from CWD changes

# ── FLASK ─────────────────────────────────────────────────────────────────────
API_PORT  = int(os.getenv("API_PORT",  5000))
FLASK_ENV = os.getenv("FLASK_ENV", "development")
FLASK_DEBUG = FLASK_ENV == "development"

# ── BACKTEST DEFAULTS ─────────────────────────────────────────────────────────
DEFAULT_STOP_LOSS_PCT     = float(os.getenv("DEFAULT_STOP_LOSS_PCT",    5.0))
DEFAULT_TAKE_PROFIT_PCT   = float(os.getenv("DEFAULT_TAKE_PROFIT_PCT",  10.0))
DEFAULT_MAX_HOLD_DAYS     = int(os.getenv("DEFAULT_MAX_HOLD_DAYS",      15))
DEFAULT_INITIAL_CAPITAL   = float(os.getenv("DEFAULT_INITIAL_CAPITAL",  100_000))
DEFAULT_POSITION_SIZE_PCT = float(os.getenv("DEFAULT_POSITION_SIZE_PCT", 10.0))

DEFAULT_BACKTEST_CONFIG = {
    "stop_loss_pct":     DEFAULT_STOP_LOSS_PCT,
    "take_profit_pct":   DEFAULT_TAKE_PROFIT_PCT,
    "max_hold_days":     DEFAULT_MAX_HOLD_DAYS,
    "initial_capital":   DEFAULT_INITIAL_CAPITAL,
    "position_size_pct": DEFAULT_POSITION_SIZE_PCT,
}

# ── SURVIVAL GATE THRESHOLDS ──────────────────────────────────────────────────
MIN_WINRATE          = float(os.getenv("MIN_WINRATE",        0.48))   # Gate 1
WALK_FORWARD_SPLIT   = float(os.getenv("WALK_FORWARD_SPLIT", 0.70))   # Gate 2 train%
WALK_FORWARD_MIN_WR  = float(os.getenv("WALK_FORWARD_MIN_WR", 0.45))  # Gate 2 test%
REGIME_MIN_PROFITABLE = int(os.getenv("REGIME_MIN_PROFITABLE", 2))    # Gate 3
MONTE_CARLO_RUNS     = int(os.getenv("MONTE_CARLO_RUNS",    1000))
MONTE_CARLO_MIN_P5   = float(os.getenv("MONTE_CARLO_MIN_P5", 0.40))

# ── SCORING WEIGHTS (must sum to 1.0) ─────────────────────────────────────────
WEIGHT_WINRATE        = float(os.getenv("WEIGHT_WINRATE",        0.35))
WEIGHT_PROFIT_FACTOR  = float(os.getenv("WEIGHT_PROFIT_FACTOR",  0.30))
WEIGHT_CONSISTENCY    = float(os.getenv("WEIGHT_CONSISTENCY",    0.20))
WEIGHT_DRAWDOWN       = float(os.getenv("WEIGHT_DRAWDOWN",       0.15))

# Cap on profit factor before normalization (avoids extreme outliers)
PROFIT_FACTOR_CAP = float(os.getenv("PROFIT_FACTOR_CAP", 3.0))

# ── NEPSE SIGNAL THRESHOLDS ───────────────────────────────────────────────────
LIQUIDITY_SPIKE_THRESHOLD  = float(os.getenv("LIQUIDITY_SPIKE_THRESHOLD",  1.5))
BROKER_CONCENTRATION_ALERT = float(os.getenv("BROKER_CONCENTRATION_ALERT", 60.0))
PUMP_SCORE_ALERT           = float(os.getenv("PUMP_SCORE_ALERT",           60.0))

# ── DATA PIPELINE ─────────────────────────────────────────────────────────────
MIN_PRICE_ROWS     = int(os.getenv("MIN_PRICE_ROWS", 30))   # min rows to backtest
OPTIMIZER_TOP_N    = int(os.getenv("OPTIMIZER_TOP_N", 20))  # top survivors → combos
COMBINATION_TOP_N  = int(os.getenv("COMBINATION_TOP_N", 20))


if __name__ == "__main__":
    print("NEPSE Config")
    print(f"  DB_PATH             = {DB_PATH}")
    print(f"  API_PORT            = {API_PORT}")
    print(f"  DEFAULT_STOP_LOSS   = {DEFAULT_STOP_LOSS_PCT}%")
    print(f"  DEFAULT_TAKE_PROFIT = {DEFAULT_TAKE_PROFIT_PCT}%")
    print(f"  DEFAULT_MAX_HOLD    = {DEFAULT_MAX_HOLD_DAYS}d")
    print(f"  MIN_WINRATE         = {MIN_WINRATE*100:.0f}%")
    print(f"  MONTE_CARLO_RUNS    = {MONTE_CARLO_RUNS}")
    print(f"  Weights: WR={WEIGHT_WINRATE} PF={WEIGHT_PROFIT_FACTOR} "
          f"Con={WEIGHT_CONSISTENCY} DD={WEIGHT_DRAWDOWN}")
    print(f"  Sum of weights      = {WEIGHT_WINRATE+WEIGHT_PROFIT_FACTOR+WEIGHT_CONSISTENCY+WEIGHT_DRAWDOWN:.2f}")
