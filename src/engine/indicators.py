# src/engine/indicators.py
# Step 6: Technical Indicator Library + Signal Generator
# Defines every indicator + parameter combination and generates BUY/SELL/HOLD
# signals for each.  Uses ONLY the 'ta' library (not pandas_ta).

import itertools
import sqlite3
import os
import traceback

import pandas as pd
import numpy as np
import ta

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH = "data/nepse.db"


def get_db():
    os.makedirs("data", exist_ok=True)
    return sqlite3.connect(DB_PATH)


# ═══════════════════════════════════════════════════════════════════════════════
# INDICATOR CONFIG CATALOGUE
# ═══════════════════════════════════════════════════════════════════════════════

def get_all_indicator_configs(silent: bool = False):
    """
    Return a list of every indicator config dict.
    Each dict has keys: name, type, indicator, params.
    Uses itertools.product to enumerate all parameter combos.
    Pass silent=True to suppress the count print.
    """
    configs = []

    # ── TREND: EMA crossover ──────────────────────────────────────────────────
    fast_periods = [5, 9, 13, 21]
    slow_periods = [21, 34, 50, 100]
    for fast, slow in itertools.product(fast_periods, slow_periods):
        if fast >= slow:
            continue
        configs.append({
            "name":      f"EMA_{fast}_{slow}",
            "type":      "trend",
            "indicator": "ema_cross",
            "params":    {"fast": fast, "slow": slow},
        })

    # ── TREND: SMA crossover ──────────────────────────────────────────────────
    for fast, slow in itertools.product(fast_periods, slow_periods):
        if fast >= slow:
            continue
        configs.append({
            "name":      f"SMA_{fast}_{slow}",
            "type":      "trend",
            "indicator": "sma_cross",
            "params":    {"fast": fast, "slow": slow},
        })

    # ── TREND: MACD ───────────────────────────────────────────────────────────
    for fast, slow, sig in itertools.product([8, 12], [21, 26], [7, 9]):
        if fast >= slow:
            continue
        configs.append({
            "name":      f"MACD_{fast}_{slow}_{sig}",
            "type":      "trend",
            "indicator": "macd",
            "params":    {"fast": fast, "slow": slow, "signal": sig},
        })

    # ── TREND: ADX ────────────────────────────────────────────────────────────
    for period, threshold in itertools.product([10, 14, 20], [20, 25, 30]):
        configs.append({
            "name":      f"ADX_{period}_{threshold}",
            "type":      "trend",
            "indicator": "adx",
            "params":    {"period": period, "threshold": threshold},
        })

    # ── TREND: Supertrend ────────────────────────────────────────────────────
    for period, mult in itertools.product([7, 10, 14], [2.0, 3.0, 4.0]):
        configs.append({
            "name":      f"ST_{period}_{mult}",
            "type":      "trend",
            "indicator": "supertrend",
            "params":    {"period": period, "multiplier": mult},
        })

    # ── MOMENTUM: RSI ─────────────────────────────────────────────────────────
    for period, oversold, overbought in itertools.product(
            [7, 9, 14, 21], [25, 30, 35], [65, 70, 75]):
        configs.append({
            "name":      f"RSI_{period}_{oversold}_{overbought}",
            "type":      "momentum",
            "indicator": "rsi",
            "params":    {"period": period, "oversold": oversold,
                          "overbought": overbought},
        })

    # ── MOMENTUM: Stochastic ──────────────────────────────────────────────────
    for k, d, oversold, overbought in itertools.product(
            [9, 14, 21], [3, 5], [20, 25], [75, 80]):
        configs.append({
            "name":      f"STOCH_{k}_{d}_{oversold}_{overbought}",
            "type":      "momentum",
            "indicator": "stochastic",
            "params":    {"k_period": k, "d_period": d,
                          "oversold": oversold, "overbought": overbought},
        })

    # ── MOMENTUM: CCI ─────────────────────────────────────────────────────────
    for period, threshold in itertools.product([14, 20, 30], [100, 150]):
        configs.append({
            "name":      f"CCI_{period}_{threshold}",
            "type":      "momentum",
            "indicator": "cci",
            "params":    {"period": period, "threshold": threshold},
        })

    # ── MOMENTUM: Williams %R ─────────────────────────────────────────────────
    for period, threshold in itertools.product([10, 14, 21], [20]):
        configs.append({
            "name":      f"WR_{period}_{threshold}",
            "type":      "momentum",
            "indicator": "williams_r",
            "params":    {"period": period, "threshold": threshold},
        })

    # ── MOMENTUM: ROC ─────────────────────────────────────────────────────────
    for period in [9, 12, 21]:
        configs.append({
            "name":      f"ROC_{period}",
            "type":      "momentum",
            "indicator": "roc",
            "params":    {"period": period, "threshold": 0},
        })

    # ── VOLATILITY: Bollinger Bands ───────────────────────────────────────────
    for period, std in itertools.product([10, 20, 30], [1.5, 2.0, 2.5]):
        configs.append({
            "name":      f"BB_{period}_{std}",
            "type":      "volatility",
            "indicator": "bollinger",
            "params":    {"period": period, "std": std},
        })

    # ── VOLATILITY: ATR (sizing only, signal = trend direction) ──────────────
    for period in [10, 14, 20]:
        configs.append({
            "name":      f"ATR_{period}",
            "type":      "volatility",
            "indicator": "atr",
            "params":    {"period": period},
        })

    # ── VOLATILITY: Keltner Channel ───────────────────────────────────────────
    for period, mult in itertools.product([10, 20], [1.5, 2.0]):
        configs.append({
            "name":      f"KC_{period}_{mult}",
            "type":      "volatility",
            "indicator": "keltner",
            "params":    {"period": period, "multiplier": mult},
        })

    # ── VOLUME: OBV ───────────────────────────────────────────────────────────
    configs.append({
        "name":      "OBV_20",
        "type":      "volume",
        "indicator": "obv",
        "params":    {"ema_period": 20},
    })

    # ── VOLUME: MFI ───────────────────────────────────────────────────────────
    for period, oversold, overbought in itertools.product(
            [10, 14, 20], [20, 30], [70, 80]):
        configs.append({
            "name":      f"MFI_{period}_{oversold}_{overbought}",
            "type":      "volume",
            "indicator": "mfi",
            "params":    {"period": period, "oversold": oversold,
                          "overbought": overbought},
        })

    # ── VOLUME: CMF ───────────────────────────────────────────────────────────
    for period in [10, 20]:
        configs.append({
            "name":      f"CMF_{period}",
            "type":      "volume",
            "indicator": "cmf",
            "params":    {"period": period, "threshold": 0},
        })

    # ── VOLUME: Volume MA crossover ───────────────────────────────────────────
    for fast, slow in itertools.product([5, 10], [20, 50]):
        if fast >= slow:
            continue
        configs.append({
            "name":      f"VOLMA_{fast}_{slow}",
            "type":      "volume",
            "indicator": "volume_ma_cross",
            "params":    {"fast": fast, "slow": slow},
        })

    if not silent:
        print(f"Total indicator configs: {len(configs)}")
    return configs


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL GENERATION — ONE INDICATOR + ONE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

def _cross_above(s1, s2):
    """Return boolean Series: True where s1 just crossed above s2."""
    return (s1 > s2) & (s1.shift(1) <= s2.shift(1))


def _cross_below(s1, s2):
    """Return boolean Series: True where s1 just crossed below s2."""
    return (s1 < s2) & (s1.shift(1) >= s2.shift(1))


def generate_signals(df, config):
    """
    Given a clean_price_history DataFrame and one indicator config dict,
    compute the indicator and return the input DataFrame with an added
    'signal' column: 1=BUY, -1=SELL, 0=HOLD.

    All NaN indicator rows default to HOLD (0).
    """
    df = df.copy()
    df["signal"] = 0

    try:
        ind  = config["indicator"]
        p    = config["params"]
        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        # ── EMA crossover ────────────────────────────────────────────────────
        if ind == "ema_cross":
            ema_fast = ta.trend.EMAIndicator(close, window=p["fast"],  fillna=False).ema_indicator()
            ema_slow = ta.trend.EMAIndicator(close, window=p["slow"],  fillna=False).ema_indicator()
            df.loc[_cross_above(ema_fast, ema_slow), "signal"] =  1
            df.loc[_cross_below(ema_fast, ema_slow), "signal"] = -1

        # ── SMA crossover ────────────────────────────────────────────────────
        elif ind == "sma_cross":
            sma_fast = ta.trend.SMAIndicator(close, window=p["fast"],  fillna=False).sma_indicator()
            sma_slow = ta.trend.SMAIndicator(close, window=p["slow"],  fillna=False).sma_indicator()
            df.loc[_cross_above(sma_fast, sma_slow), "signal"] =  1
            df.loc[_cross_below(sma_fast, sma_slow), "signal"] = -1

        # ── MACD ─────────────────────────────────────────────────────────────
        elif ind == "macd":
            macd_obj  = ta.trend.MACD(close, window_fast=p["fast"],
                                       window_slow=p["slow"],
                                       window_sign=p["signal"], fillna=False)
            macd_line = macd_obj.macd()
            sig_line  = macd_obj.macd_signal()
            df.loc[_cross_above(macd_line, sig_line), "signal"] =  1
            df.loc[_cross_below(macd_line, sig_line), "signal"] = -1

        # ── ADX ──────────────────────────────────────────────────────────────
        elif ind == "adx":
            adx_obj = ta.trend.ADXIndicator(high, low, close,
                                              window=p["period"], fillna=False)
            adx  = adx_obj.adx()
            dip  = adx_obj.adx_pos()   # +DI
            dim  = adx_obj.adx_neg()   # -DI
            strong = adx >= p["threshold"]
            df.loc[strong & _cross_above(dip, dim), "signal"] =  1
            df.loc[strong & _cross_below(dip, dim), "signal"] = -1

        # ── Supertrend (manual, ta has no supertrend) ─────────────────────────
        elif ind == "supertrend":
            period = p["period"]
            mult   = p["multiplier"]
            atr    = ta.volatility.AverageTrueRange(
                high, low, close, window=period, fillna=True
            ).average_true_range()
            hl2 = (high + low) / 2
            upper_band = hl2 + mult * atr
            lower_band = hl2 - mult * atr

            supertrend = pd.Series(index=df.index, dtype=float)
            direction  = pd.Series(0, index=df.index, dtype=int)

            for i in range(len(df)):
                if i == 0:
                    supertrend.iloc[i] = lower_band.iloc[i]
                    direction.iloc[i]  = 1
                    continue
                prev_st  = supertrend.iloc[i - 1]
                prev_dir = direction.iloc[i - 1]
                c = close.iloc[i]

                if prev_dir == 1:
                    st = max(lower_band.iloc[i], prev_st) if c > prev_st else upper_band.iloc[i]
                    direction.iloc[i] = 1 if c > st else -1
                else:
                    st = min(upper_band.iloc[i], prev_st) if c < prev_st else lower_band.iloc[i]
                    direction.iloc[i] = -1 if c < st else 1
                supertrend.iloc[i] = st

            dir_shift = direction.shift(1)
            df.loc[(direction == 1)  & (dir_shift == -1), "signal"] =  1
            df.loc[(direction == -1) & (dir_shift ==  1), "signal"] = -1

        # ── RSI ───────────────────────────────────────────────────────────────
        elif ind == "rsi":
            rsi = ta.momentum.RSIIndicator(close, window=p["period"],
                                            fillna=False).rsi()
            df.loc[_cross_above(rsi, pd.Series(p["oversold"],  index=df.index)), "signal"] =  1
            df.loc[_cross_below(rsi, pd.Series(p["overbought"], index=df.index)), "signal"] = -1

        # ── Stochastic ────────────────────────────────────────────────────────
        elif ind == "stochastic":
            stoch = ta.momentum.StochasticOscillator(
                high, low, close,
                window=p["k_period"], smooth_window=p["d_period"],
                fillna=False
            )
            k = stoch.stoch()
            d = stoch.stoch_signal()
            buy  = (k > p["oversold"])  & _cross_above(k, d)
            sell = (k < p["overbought"]) & _cross_below(k, d)
            df.loc[buy,  "signal"] =  1
            df.loc[sell, "signal"] = -1

        # ── CCI ───────────────────────────────────────────────────────────────
        elif ind == "cci":
            cci = ta.trend.CCIIndicator(high, low, close,
                                         window=p["period"], fillna=False).cci()
            thr = p["threshold"]
            df.loc[_cross_above(cci, pd.Series(-thr, index=df.index)), "signal"] =  1
            df.loc[_cross_below(cci, pd.Series( thr, index=df.index)), "signal"] = -1

        # ── Williams %R ───────────────────────────────────────────────────────
        elif ind == "williams_r":
            wr = ta.momentum.WilliamsRIndicator(
                high, low, close, lbp=p["period"], fillna=False
            ).williams_r()
            thr = p["threshold"]
            df.loc[_cross_above(wr, pd.Series(-100 + thr, index=df.index)), "signal"] =  1
            df.loc[_cross_below(wr, pd.Series(-thr,        index=df.index)), "signal"] = -1

        # ── ROC ───────────────────────────────────────────────────────────────
        elif ind == "roc":
            roc = ta.momentum.ROCIndicator(close, window=p["period"],
                                            fillna=False).roc()
            zero = pd.Series(p["threshold"], index=df.index)
            df.loc[_cross_above(roc, zero), "signal"] =  1
            df.loc[_cross_below(roc, zero), "signal"] = -1

        # ── Bollinger Bands ───────────────────────────────────────────────────
        elif ind == "bollinger":
            bb = ta.volatility.BollingerBands(
                close, window=p["period"], window_dev=p["std"], fillna=False
            )
            lower = bb.bollinger_lband()
            upper = bb.bollinger_hband()
            mid   = bb.bollinger_mavg()
            # BUY: price touches lower band and closes back above it
            df.loc[(close.shift(1) <= lower.shift(1)) & (close > lower), "signal"] =  1
            # SELL: price touches upper band and closes back below it
            df.loc[(close.shift(1) >= upper.shift(1)) & (close < upper), "signal"] = -1

        # ── ATR (trend direction proxy) ───────────────────────────────────────
        elif ind == "atr":
            # ATR itself is a sizing tool; use price vs. SMA-20 as the signal
            atr  = ta.volatility.AverageTrueRange(
                high, low, close, window=p["period"], fillna=False
            ).average_true_range()
            sma  = ta.trend.SMAIndicator(close, window=20, fillna=False).sma_indicator()
            df.loc[_cross_above(close, sma), "signal"] =  1
            df.loc[_cross_below(close, sma), "signal"] = -1

        # ── Keltner Channel ───────────────────────────────────────────────────
        elif ind == "keltner":
            kc = ta.volatility.KeltnerChannel(
                high, low, close,
                window=p["period"], window_atr=p["period"],
                multiplier=p["multiplier"], fillna=False
            )
            lower = kc.keltner_channel_lband()
            upper = kc.keltner_channel_hband()
            df.loc[(close.shift(1) <= lower.shift(1)) & (close > lower), "signal"] =  1
            df.loc[(close.shift(1) >= upper.shift(1)) & (close < upper), "signal"] = -1

        # ── OBV ───────────────────────────────────────────────────────────────
        elif ind == "obv":
            obv = ta.volume.OnBalanceVolumeIndicator(
                close, volume, fillna=False
            ).on_balance_volume()
            obv_ema = ta.trend.EMAIndicator(
                obv, window=p["ema_period"], fillna=False
            ).ema_indicator()
            df.loc[_cross_above(obv, obv_ema), "signal"] =  1
            df.loc[_cross_below(obv, obv_ema), "signal"] = -1

        # ── MFI ───────────────────────────────────────────────────────────────
        elif ind == "mfi":
            mfi = ta.volume.MFIIndicator(
                high, low, close, volume,
                window=p["period"], fillna=False
            ).money_flow_index()
            df.loc[_cross_above(mfi, pd.Series(p["oversold"],   index=df.index)), "signal"] =  1
            df.loc[_cross_below(mfi, pd.Series(p["overbought"], index=df.index)), "signal"] = -1

        # ── CMF ───────────────────────────────────────────────────────────────
        elif ind == "cmf":
            cmf = ta.volume.ChaikinMoneyFlowIndicator(
                high, low, close, volume,
                window=p["period"], fillna=False
            ).chaikin_money_flow()
            zero = pd.Series(p["threshold"], index=df.index)
            df.loc[_cross_above(cmf, zero), "signal"] =  1
            df.loc[_cross_below(cmf, zero), "signal"] = -1

        # ── Volume MA crossover ───────────────────────────────────────────────
        elif ind == "volume_ma_cross":
            vol_fast = volume.rolling(window=p["fast"], min_periods=1).mean()
            vol_slow = volume.rolling(window=p["slow"], min_periods=1).mean()
            # Volume surge = bullish only when price is also rising
            price_up = close > close.shift(1)
            df.loc[_cross_above(vol_fast, vol_slow) & price_up,  "signal"] =  1
            df.loc[_cross_below(vol_fast, vol_slow) & ~price_up, "signal"] = -1

    except Exception as e:
        # On any failure return all-HOLD to avoid crashing the caller
        df["signal"] = 0

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# GENERATE ALL SIGNALS FOR ONE SYMBOL
# ═══════════════════════════════════════════════════════════════════════════════

def generate_all_signals(symbol):
    """
    Load clean_price_history for *symbol*, run generate_signals() for every
    indicator config, and return a dict mapping config_name -> signal Series.
    """
    conn = get_db()
    df = pd.read_sql_query("""
        SELECT date, open, high, low, close, volume,
               price_change_pct, volume_ratio, atr14, market_condition
        FROM   clean_price_history
        WHERE  symbol = ?
        ORDER  BY date ASC
    """, conn, params=(symbol,))
    conn.close()

    if df.empty:
        print(f"  No data for {symbol}")
        return {}

    configs = get_all_indicator_configs()
    results = {}

    for cfg in configs:
        try:
            out = generate_signals(df, cfg)
            results[cfg["name"]] = out["signal"].values
        except Exception:
            results[cfg["name"]] = np.zeros(len(df), dtype=int)

    return results


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE Indicator Library - Step 6")
    print("=" * 55)
    print()

    # 1. Build and count all configs
    configs = get_all_indicator_configs()
    print()

    # Group by type
    type_counts = {}
    for c in configs:
        type_counts[c["type"]] = type_counts.get(c["type"], 0) + 1
    for t, n in sorted(type_counts.items()):
        print(f"  {t:<12} {n} configs")
    print()

    # 2. Load NABIL
    conn = get_db()
    nabil = pd.read_sql_query("""
        SELECT date, open, high, low, close, volume,
               price_change_pct, volume_ratio, atr14, market_condition
        FROM   clean_price_history
        WHERE  symbol = 'NABIL'
        ORDER  BY date ASC
    """, conn)
    conn.close()

    print(f"NABIL: {len(nabil)} clean rows loaded")
    print()

    # 3. Test 5 diverse configs
    test_configs = [
        next(c for c in configs if c["name"] == "RSI_14_30_70"),
        next(c for c in configs if c["name"] == "MACD_12_26_9"),
        next(c for c in configs if c["name"] == "BB_20_2.0"),
        next(c for c in configs if c["name"] == "EMA_9_21"),
        next(c for c in configs if c["name"] == "OBV_20"),
    ]

    print(f"{'Config':<22} {'BUY':>5} {'SELL':>5} {'HOLD':>5}  Last signal")
    print("-" * 55)
    for cfg in test_configs:
        result = generate_signals(nabil.copy(), cfg)
        sigs   = result["signal"]
        n_buy  = (sigs == 1).sum()
        n_sell = (sigs == -1).sum()
        n_hold = (sigs == 0).sum()
        last   = {1: "BUY", -1: "SELL", 0: "HOLD"}.get(sigs.iloc[-1], "?")
        print(f"  {cfg['name']:<20} {n_buy:>5} {n_sell:>5} {n_hold:>5}  {last}")

    print()
    print("[OK] Step 6 complete!")
