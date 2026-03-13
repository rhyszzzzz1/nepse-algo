# src/api/app.py
# Step 7: NEPSE Flask REST API
# Serves all data (signals, backtests, prices, sectors, market summary)
# to the frontend dashboard via clean JSON endpoints.
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import sqlite3
from db import get_db
import os
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=["*"])

try:
    import sys as _sys
    _sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    from config import (API_PORT, FLASK_DEBUG,
                        DEFAULT_STOP_LOSS_PCT, DEFAULT_TAKE_PROFIT_PCT,
                        DEFAULT_MAX_HOLD_DAYS, DEFAULT_INITIAL_CAPITAL,
                        DEFAULT_POSITION_SIZE_PCT)
except ImportError:
    DB_PATH    = os.path.join(os.path.dirname(__file__), "..", "..", "data", "nepse.db")
    API_PORT   = 5000; FLASK_DEBUG = True
    DEFAULT_STOP_LOSS_PCT = 5.0; DEFAULT_TAKE_PROFIT_PCT = 10.0
    DEFAULT_MAX_HOLD_DAYS = 15;  DEFAULT_INITIAL_CAPITAL = 100_000
    DEFAULT_POSITION_SIZE_PCT = 10.0


# ── DB HELPER ─────────────────────────────────────────────────────────────────
def get_db():
    from db import get_db as _get_db
    conn = _get_db()
    conn.row_factory = True
    return conn

def rows_to_list(cursor_rows):
    return [dict(r) for r in cursor_rows]


# ── ROOT INDEX ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    """API index — lists all available endpoints."""
    return jsonify({
        "name":    "NEPSE Algorithmic Trading API",
        "version": "1.0",
        "status":  "running",
        "endpoints": [
            "GET  /api/health",
            "GET  /api/market-summary",
            "GET  /api/market/overview",
            "GET  /api/top-movers",
            "GET  /api/companies",
            "GET  /api/sectors",
            "GET  /api/signals?signal=BUY&limit=20",
            "GET  /api/signals/<SYMBOL>",
            "GET  /api/price/<SYMBOL>?days=90",
            "GET  /api/stock/<SYMBOL>",
            "GET  /api/stock/<SYMBOL>/ohlcv",
            "GET  /api/screener",
            "GET  /api/backtest",
            "GET  /api/backtest/<SYMBOL>",
            "POST /api/run-backtest",
            "GET  /api/run-backtest/<SYMBOL>",
            "GET  /api/optimizer/leaderboard",
            "GET  /api/optimizer/<SYMBOL>",
            "GET  /api/rules",
            "GET  /api/rules/<SYMBOL>",
        ]
    })


# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})


# ── MARKET OVERVIEW ───────────────────────────────────────────────────────────
@app.route("/api/market/overview")
def market_overview():
    """
    Return the latest market summary + sector index snapshot +
    aggregate signal breakdown for today.
    """
    conn = get_db()
    try:
        # Latest market summary
        summary = conn.execute(
            "SELECT * FROM market_summary ORDER BY date DESC LIMIT 1"
        ).fetchone()

        # Latest sectors
        latest_sector_date = conn.execute(
            "SELECT MAX(date) FROM sector_index"
        ).fetchone()[0]
        sectors = rows_to_list(conn.execute(
            "SELECT sector, value FROM sector_index WHERE date = ?",
            (latest_sector_date,)
        ).fetchall()) if latest_sector_date else []

        # Signal breakdown for most recent signal date
        sig_date = conn.execute(
            "SELECT MAX(date) FROM signals"
        ).fetchone()[0]
        breakdown = rows_to_list(conn.execute("""
            SELECT signal, COUNT(*) AS count
            FROM   signals
            WHERE  date = ?
            GROUP  BY signal
        """, (sig_date,)).fetchall()) if sig_date else []

        return jsonify({
            "market_summary": dict(summary) if summary else {},
            "sector_indices": sectors,
            "signal_date":    sig_date,
            "signal_breakdown": breakdown,
        })
    finally:
        conn.close()


# ── SIGNALS ────────────────────────────────────────────────────────────────────
@app.route("/api/signals")
def get_signals():
    """
    Return signals for the most recent date (or a specified ?date=YYYY-MM-DD).
    Supports ?signal=BUY|SELL|HOLD and ?limit=N filters.
    """
    conn = get_db()
    try:
        date   = request.args.get("date")
        signal = request.args.get("signal", "").upper()
        limit  = int(request.args.get("limit", 100))

        if not date:
            date = conn.execute(
                "SELECT MAX(date) FROM signals"
            ).fetchone()[0]

        query  = "SELECT * FROM signals WHERE date = ?"
        params = [date]
        if signal in ("BUY", "SELL", "HOLD"):
            query  += " AND signal = ?"
            params.append(signal)
        query += " ORDER BY total_score DESC LIMIT ?"
        params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())
        return jsonify({"date": date, "count": len(rows), "signals": rows})
    finally:
        conn.close()


# ── SINGLE STOCK ──────────────────────────────────────────────────────────────
@app.route("/api/stock/<symbol>")
def get_stock(symbol):
    """
    Return clean price history + latest signal for a single symbol.
    Supports ?days=N (default 220).
    """
    conn = get_db()
    try:
        days = int(request.args.get("days", 220))
        symbol = symbol.upper()

        prices = rows_to_list(conn.execute("""
            SELECT date, open, high, low, close, volume,
                   price_change_pct, volume_ratio, atr14, market_condition
            FROM   clean_price_history
            WHERE  symbol = ?
            ORDER  BY date DESC
            LIMIT  ?
        """, (symbol, days)).fetchall())

        # Return in ascending order for charting
        prices.reverse()

        latest_signal = conn.execute("""
            SELECT * FROM signals
            WHERE  symbol = ?
            ORDER  BY date DESC
            LIMIT  1
        """, (symbol,)).fetchone()

        company = conn.execute(
            "SELECT * FROM companies WHERE symbol = ?", (symbol,)
        ).fetchone()

        return jsonify({
            "symbol":        symbol,
            "company":       dict(company) if company else {},
            "price_history": prices,
            "latest_signal": dict(latest_signal) if latest_signal else {},
        })
    finally:
        conn.close()


# ── PRICE HISTORY (OHLCV for charting) ───────────────────────────────────────
@app.route("/api/stock/<symbol>/ohlcv")
def get_ohlcv(symbol):
    """Return raw OHLCV rows as arrays suitable for candlestick charts."""
    conn = get_db()
    try:
        symbol = symbol.upper()
        days   = int(request.args.get("days", 220))
        rows   = conn.execute("""
            SELECT date, open, high, low, close, volume
            FROM   clean_price_history
            WHERE  symbol = ?
            ORDER  BY date DESC LIMIT ?
        """, (symbol, days)).fetchall()
        rows = list(reversed(rows))

        return jsonify({
            "symbol": symbol,
            "dates":  [r["date"]   for r in rows],
            "open":   [r["open"]   for r in rows],
            "high":   [r["high"]   for r in rows],
            "low":    [r["low"]    for r in rows],
            "close":  [r["close"]  for r in rows],
            "volume": [r["volume"] for r in rows],
        })
    finally:
        conn.close()


# ── BACKTEST RESULTS ───────────────────────────────────────────────────────────
@app.route("/api/backtest")
def get_backtest():
    """
    Return backtest summary rows.
    ?sort=total_return_pct|sharpe_ratio|win_rate_pct  (default: total_return_pct)
    ?order=asc|desc  (default: desc)
    ?limit=N  (default: 50)
    """
    conn = get_db()
    try:
        sort_col = request.args.get("sort", "total_return_pct")
        order    = "ASC" if request.args.get("order", "desc").lower() == "asc" else "DESC"
        limit    = int(request.args.get("limit", 50))

        allowed_cols = {"total_return_pct", "sharpe_ratio", "win_rate_pct",
                        "max_drawdown_pct", "total_trades", "avg_return_pct"}
        if sort_col not in allowed_cols:
            sort_col = "total_return_pct"

        rows = rows_to_list(conn.execute(f"""
            SELECT symbol_filter AS symbol, start_date, end_date,
                   total_trades, win_rate_pct, avg_return_pct,
                   total_return_pct, max_drawdown_pct, sharpe_ratio,
                   starting_capital, ending_capital
            FROM   backtest_summary
            ORDER  BY {sort_col} {order}
            LIMIT  ?
        """, (limit,)).fetchall())

        return jsonify({"count": len(rows), "results": rows})
    finally:
        conn.close()


@app.route("/api/backtest/<symbol>")
def get_backtest_symbol(symbol):
    """Return full trade log for a specific symbol."""
    conn = get_db()
    try:
        symbol = symbol.upper()
        summary = conn.execute("""
            SELECT * FROM backtest_summary
            WHERE  symbol_filter = ?
            ORDER  BY created_at DESC LIMIT 1
        """, (symbol,)).fetchone()

        trades = rows_to_list(conn.execute("""
            SELECT * FROM backtest_trades
            WHERE  symbol = ?
            ORDER  BY buy_date ASC
        """, (symbol,)).fetchall())

        return jsonify({
            "symbol":  symbol,
            "summary": dict(summary) if summary else {},
            "trades":  trades,
        })
    finally:
        conn.close()


# ── SCREENER ──────────────────────────────────────────────────────────────────
@app.route("/api/screener")
def screener():
    """
    Multi-factor screener. All params optional:
      ?signal=BUY|SELL|HOLD
      ?market_condition=bull|bear|sideways
      ?min_rsi=N  &max_rsi=N
      ?min_volume_ratio=N
      ?min_score=N
      ?limit=N (default 50)
    """
    conn = get_db()
    try:
        signal     = request.args.get("signal", "").upper()
        condition  = request.args.get("market_condition", "").lower()
        min_rsi    = request.args.get("min_rsi",    type=float)
        max_rsi    = request.args.get("max_rsi",    type=float)
        min_vol    = request.args.get("min_volume_ratio", type=float)
        min_score  = request.args.get("min_score",  type=int)
        limit      = int(request.args.get("limit", 50))

        sig_date = conn.execute(
            "SELECT MAX(date) FROM signals"
        ).fetchone()[0]

        query  = "SELECT * FROM signals WHERE date = ?"
        params = [sig_date]

        if signal in ("BUY", "SELL", "HOLD"):
            query += " AND signal = ?";   params.append(signal)
        if condition in ("bull", "bear", "sideways"):
            query += " AND market_condition = ?"; params.append(condition)
        if min_rsi is not None:
            query += " AND rsi14 >= ?";   params.append(min_rsi)
        if max_rsi is not None:
            query += " AND rsi14 <= ?";   params.append(max_rsi)
        if min_vol is not None:
            query += " AND volume_ratio >= ?"; params.append(min_vol)
        if min_score is not None:
            query += " AND total_score >= ?";  params.append(min_score)

        query += " ORDER BY total_score DESC LIMIT ?"
        params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())
        return jsonify({
            "date":   sig_date,
            "count":  len(rows),
            "filter": {"signal": signal, "market_condition": condition,
                       "min_rsi": min_rsi, "max_rsi": max_rsi,
                       "min_volume_ratio": min_vol, "min_score": min_score},
            "results": rows,
        })
    finally:
        conn.close()


# ── COMPANIES LIST ────────────────────────────────────────────────────────────
@app.route("/api/companies")
def get_companies():
    """Return all listed companies, optionally filtered by ?sector=..."""
    conn = get_db()
    try:
        sector = request.args.get("sector", "")
        if sector:
            rows = rows_to_list(conn.execute(
                "SELECT * FROM companies WHERE sector = ? ORDER BY symbol",
                (sector,)
            ).fetchall())
        else:
            rows = rows_to_list(conn.execute(
                "SELECT * FROM companies ORDER BY symbol"
            ).fetchall())
        return jsonify({"count": len(rows), "companies": rows})
    finally:
        conn.close()


# ── SECTOR SUMMARY ────────────────────────────────────────────────────────────
@app.route("/api/sectors")
def get_sectors():
    """Return all sector index values for the latest date."""
    conn = get_db()
    try:
        latest = conn.execute(
            "SELECT MAX(date) FROM sector_index"
        ).fetchone()[0]
        rows = rows_to_list(conn.execute(
            "SELECT sector, value FROM sector_index WHERE date = ? ORDER BY value DESC",
            (latest,)
        ).fetchall()) if latest else []
        return jsonify({"date": latest, "sectors": rows})
    finally:
        conn.close()


# ── OPTIMIZER LEADERBOARD ─────────────────────────────────────────────────────
@app.route("/api/optimizer/leaderboard")
def optimizer_leaderboard():
    """
    Return the best indicator per symbol ranked by composite_score.
    ?limit=N (default 50)
    ?indicator_type=trend|momentum|volatility|volume  (optional filter)
    """
    conn = get_db()
    try:
        limit    = int(request.args.get("limit", 50))
        ind_type = request.args.get("indicator_type", "").lower()

        query  = "SELECT * FROM optimizer_best"
        params = []
        if ind_type in ("trend", "momentum", "volatility", "volume"):
            query += " WHERE indicator_type = ?"
            params.append(ind_type)
        query += " ORDER BY composite_score DESC LIMIT ?"
        params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())

        # Indicator popularity summary
        popularity = rows_to_list(conn.execute("""
            SELECT best_indicator AS indicator, indicator_type AS type,
                   COUNT(*) AS symbol_count,
                   AVG(winrate) AS avg_winrate,
                   AVG(total_return_pct) AS avg_return
            FROM   optimizer_best
            GROUP  BY best_indicator
            ORDER  BY symbol_count DESC
            LIMIT  15
        """).fetchall())

        # Type summary
        type_summary = rows_to_list(conn.execute("""
            SELECT indicator_type AS type,
                   COUNT(*) AS symbols,
                   AVG(winrate) AS avg_winrate,
                   AVG(total_return_pct) AS avg_return,
                   AVG(composite_score) AS avg_score
            FROM   optimizer_best
            GROUP  BY indicator_type
            ORDER  BY avg_score DESC
        """).fetchall())

        return jsonify({
            "count":        len(rows),
            "leaderboard":  rows,
            "popularity":   popularity,
            "type_summary": type_summary,
        })
    finally:
        conn.close()


# ── OPTIMIZER SYMBOL DETAIL ───────────────────────────────────────────────────
@app.route("/api/optimizer/<symbol>")
def optimizer_symbol(symbol):
    """
    Return full optimizer results for one symbol — all indicator configs
    tried, sorted by composite_score DESC.
    """
    conn = get_db()
    try:
        symbol = symbol.upper()

        best = conn.execute(
            "SELECT * FROM optimizer_best WHERE symbol = ?", (symbol,)
        ).fetchone()

        all_results = rows_to_list(conn.execute("""
            SELECT indicator, indicator_type, total_trades, winning_trades,
                   winrate, profit_factor, avg_win_pct, avg_loss_pct,
                   max_drawdown, total_return_pct, consistency, composite_score
            FROM   optimizer_results
            WHERE  symbol = ?
            ORDER  BY composite_score DESC
        """, (symbol,)).fetchall())

        return jsonify({
            "symbol":      symbol,
            "best":        dict(best) if best else {},
            "all_results": all_results,
            "count":       len(all_results),
        })
    finally:
        conn.close()


# ── TRADING RULES ─────────────────────────────────────────────────────────────
@app.route("/api/rules/<symbol>")
def get_rules_for_symbol(symbol):
    """
    Return all scored trading rules for a symbol, sorted by weighted_score DESC.
    ?limit=N (default 20)  ?type=single|combination  (optional filter)
    """
    conn = get_db()
    try:
        symbol    = symbol.upper()
        limit     = int(request.args.get("limit", 20))
        rule_type = request.args.get("type", "").lower()

        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "trading_rules" not in tables:
            return jsonify({"symbol": symbol, "count": 0, "rules": [],
                            "message": "trading_rules table not yet created — run scorer.py first"})

        query  = "SELECT * FROM trading_rules WHERE symbol = ?"
        params = [symbol]
        if rule_type in ("single", "combination"):
            query += " AND rule_type = ?"
            params.append(rule_type)
        query += " ORDER BY weighted_score DESC LIMIT ?"
        params.append(limit)

        rows = rows_to_list(conn.execute(query, params).fetchall())
        return jsonify({"symbol": symbol, "count": len(rows), "rules": rows})
    finally:
        conn.close()


@app.route("/api/rules")
def get_rules_summary():
    """
    Return a summary of how many trading rules exist per symbol.
    ?limit=N (default 50)
    """
    conn = get_db()
    try:
        limit = int(request.args.get("limit", 50))
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "trading_rules" not in tables:
            return jsonify({"count": 0, "symbols": [],
                            "message": "trading_rules table not yet created"})

        total = conn.execute("SELECT COUNT(*) FROM trading_rules").fetchone()[0]
        by_type = rows_to_list(conn.execute("""
            SELECT rule_type, COUNT(*) AS n FROM trading_rules GROUP BY rule_type
        """).fetchall())

        symbols = rows_to_list(conn.execute("""
            SELECT symbol, COUNT(*) AS rule_count,
                   MAX(weighted_score) AS top_score,
                   AVG(weighted_score) AS avg_score
            FROM   trading_rules
            GROUP  BY symbol
            ORDER  BY rule_count DESC
            LIMIT  ?
        """, (limit,)).fetchall())

        return jsonify({
            "total_rules":  total,
            "by_type":      by_type,
            "symbols":      symbols,
            "symbol_count": len(symbols),
        })
    finally:
        conn.close()


# ── BACKGROUND JOB TRACKER ────────────────────────────────────────────────────
import threading

_jobs: dict = {}   # job_id -> {'status', 'symbol', 'result', 'error'}
_job_lock = threading.Lock()


def _job_id(symbol: str) -> str:
    return f"backtest_{symbol.upper()}"


# ── PRICE ALIAS ───────────────────────────────────────────────────────────────
@app.route("/api/price/<symbol>")
def get_price(symbol):
    """
    Return last ?days=90 rows from clean_price_history for *symbol*.
    Shape matches the Lovable frontend expectation.
    """
    conn = get_db()
    try:
        symbol = symbol.upper()
        days   = int(request.args.get("days", 90))

        rows = conn.execute("""
            SELECT date, open, high, low, close, volume, market_condition
            FROM   clean_price_history
            WHERE  symbol = ?
            ORDER  BY date DESC LIMIT ?
        """, (symbol, days)).fetchall()

        if not rows:
            return jsonify({"error": f"Symbol '{symbol}' not found"}), 404

        data = [
            {"date": r[0], "open": r[1], "high": r[2], "low": r[3],
             "close": r[4], "volume": r[5], "market_condition": r[6]}
            for r in reversed(rows)
        ]
        return jsonify({"symbol": symbol, "days": len(data), "prices": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── NEPSE SIGNALS FOR ONE SYMBOL ──────────────────────────────────────────────
@app.route("/api/signals/<symbol>")
def get_nepse_signals_for_symbol(symbol):
    """
    Return the latest nepse_signals row for *symbol*
    (pump_score, dump_score, broker_accumulation_pct, liquidity_spike, etc.)
    """
    conn = get_db()
    try:
        symbol = symbol.upper()

        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "nepse_signals" not in tables:
            return jsonify({"error": "nepse_signals table not found"}), 404

        row = conn.execute("""
            SELECT *
            FROM   nepse_signals
            WHERE  symbol = ?
            ORDER  BY date DESC LIMIT 1
        """, (symbol,)).fetchone()

        if not row:
            return jsonify({"error": f"No nepse signals for '{symbol}'"}), 404

        return jsonify({"symbol": symbol, "signals": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── POST /api/run-backtest (background thread) ────────────────────────────────
@app.route("/api/run-backtest", methods=["POST"])
def run_backtest_endpoint():
    """
    Kick off a full pipeline run for a symbol in a background thread.
    Body JSON: { symbol, stop_loss_pct, take_profit_pct, max_hold_days }
    Poll GET /api/run-backtest/<symbol> to check status.
    """
    try:
        body = request.get_json(force=True) or {}
        symbol = str(body.get("symbol", "")).upper()
        if not symbol:
            return jsonify({"error": "symbol is required"}), 400

        sl   = float(body.get("stop_loss_pct",   DEFAULT_STOP_LOSS_PCT))
        tp   = float(body.get("take_profit_pct", DEFAULT_TAKE_PROFIT_PCT))
        hold = int(body.get("max_hold_days",     DEFAULT_MAX_HOLD_DAYS))

        job_id = _job_id(symbol)
        with _job_lock:
            if _jobs.get(job_id, {}).get("status") == "running":
                return jsonify({"status": "already_running",
                                "job_id": job_id,
                                "message": f"Pipeline for {symbol} already running"}), 409
            _jobs[job_id] = {"status": "running", "symbol": symbol,
                             "result": None, "error": None}

        def _run():
            import sys, os
            ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, os.path.join(ROOT, "src"))
            try:
                from engine.scorer import run_full_pipeline
                cfg = {
                    "stop_loss_pct":     sl,
                    "take_profit_pct":   tp,
                    "max_hold_days":     hold,
                    "initial_capital":   DEFAULT_INITIAL_CAPITAL,
                    "position_size_pct": DEFAULT_POSITION_SIZE_PCT,
                }
                rules = run_full_pipeline(symbol, cfg, verbose=False)
                with _job_lock:
                    _jobs[job_id]["status"] = "done"
                    _jobs[job_id]["result"] = {
                        "rules_saved": len(rules),
                        "top_rule":    rules[0]["rule_name"] if rules else None,
                        "top_score":   rules[0]["weighted_score"] if rules else None,
                    }
            except Exception as e:
                with _job_lock:
                    _jobs[job_id]["status"] = "error"
                    _jobs[job_id]["error"]  = str(e)

        threading.Thread(target=_run, daemon=True).start()

        return jsonify({
            "status":  "running",
            "job_id":  job_id,
            "symbol":  symbol,
            "message": f"Full pipeline started for {symbol} "
                       f"(SL={sl}% TP={tp}% hold={hold}d). "
                       f"Poll GET /api/run-backtest/{symbol} for status.",
        }), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-backtest/<symbol>")
def get_backtest_job_status(symbol):
    """Poll the status of a background pipeline job."""
    job_id = _job_id(symbol)
    with _job_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"status": "not_found",
                        "message": f"No job found for {symbol.upper()}"}), 404
    return jsonify(job)


# ── MARKET SUMMARY ALIAS ──────────────────────────────────────────────────────
@app.route("/api/market-summary")
def market_summary_simple():
    """Return the latest market_summary row (simple alias of /api/market/overview)."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM market_summary ORDER BY date DESC LIMIT 1"
        ).fetchone()
        return jsonify({"market_summary": dict(row) if row else {}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── TOP MOVERS ────────────────────────────────────────────────────────────────
@app.route("/api/top-movers")
def top_movers():
    """
    Return top 5 gainers and top 5 losers based on the latest trading day's
    price_change_pct from clean_price_history.
    """
    conn = get_db()
    try:
        latest_date = conn.execute(
            "SELECT MAX(date) FROM clean_price_history"
        ).fetchone()[0]

        if not latest_date:
            return jsonify({"date": None, "gainers": [], "losers": []}), 200

        gainers = rows_to_list(conn.execute("""
            SELECT symbol, close, price_change_pct, volume, market_condition
            FROM   clean_price_history
            WHERE  date = ? AND price_change_pct IS NOT NULL
            ORDER  BY price_change_pct DESC LIMIT 5
        """, (latest_date,)).fetchall())

        losers = rows_to_list(conn.execute("""
            SELECT symbol, close, price_change_pct, volume, market_condition
            FROM   clean_price_history
            WHERE  date = ? AND price_change_pct IS NOT NULL
            ORDER  BY price_change_pct ASC LIMIT 5
        """, (latest_date,)).fetchall())

        return jsonify({
            "date":    latest_date,
            "gainers": gainers,
            "losers":  losers,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE API Server")
    print("=" * 55)
    print("Starting Flask on http://localhost:5000")
    print()
    print("Endpoints:")
    print("  GET  /api/health")
    print("  GET  /api/market/overview")
    print("  GET  /api/market-summary")
    print("  GET  /api/top-movers")
    print("  GET  /api/signals?signal=BUY&limit=20")
    print("  GET  /api/signals/<SYMBOL>        (nepse signals)")
    print("  GET  /api/price/<SYMBOL>?days=90")
    print("  GET  /api/stock/<SYMBOL>")
    print("  GET  /api/stock/<SYMBOL>/ohlcv")
    print("  GET  /api/backtest")
    print("  GET  /api/backtest/<SYMBOL>")
    print("  POST /api/run-backtest            (background pipeline)")
    print("  GET  /api/run-backtest/<SYMBOL>   (poll job status)")
    print("  GET  /api/screener?signal=BUY&market_condition=bull&min_score=2")
    print("  GET  /api/companies")
    print("  GET  /api/sectors")
    print("  GET  /api/optimizer/leaderboard")
    print("  GET  /api/optimizer/<SYMBOL>")
    print("  GET  /api/rules/<SYMBOL>")
    print("  GET  /api/rules")
    print()
    port = int(os.environ.get("PORT", API_PORT))
    app.run(host="0.0.0.0", debug=FLASK_DEBUG, port=port, use_reloader=False)
