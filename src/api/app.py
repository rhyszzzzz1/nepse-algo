# src/api/app.py
# Step 7: NEPSE Flask REST API
# Serves all data (signals, backtests, prices, sectors, market summary)
# to the frontend dashboard via clean JSON endpoints.

import sqlite3
import os
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "nepse.db")


# ── DB HELPER ─────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row   # rows behave like dicts
    return conn


def rows_to_list(cursor_rows):
    return [dict(r) for r in cursor_rows]


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


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("NEPSE API Server - Step 7")
    print("=" * 55)
    print("Starting Flask on http://localhost:5000")
    print()
    print("Endpoints:")
    print("  GET /api/health")
    print("  GET /api/market/overview")
    print("  GET /api/signals?signal=BUY&limit=20")
    print("  GET /api/stock/<SYMBOL>")
    print("  GET /api/stock/<SYMBOL>/ohlcv")
    print("  GET /api/backtest")
    print("  GET /api/backtest/<SYMBOL>")
    print("  GET /api/screener?signal=BUY&market_condition=bull&min_score=2")
    print("  GET /api/companies")
    print("  GET /api/sectors")
    print()
    app.run(debug=True, port=5000, use_reloader=False)
