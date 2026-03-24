import argparse
import json
import os
import shutil
import sqlite3
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT / "data" / "nepse.db"
DEFAULT_OUT_DIR = ROOT / "data" / "parquet_ml"
SYSTEM_TABLES = {"sqlite_sequence"}


def get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [row[0] for row in rows if row[0] not in SYSTEM_TABLES]


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [row[1] for row in rows]


def partition_columns(columns: list[str]) -> list[str]:
    partitions = []
    for col in ("date", "symbol"):
        if col in columns:
            partitions.append(col)
    return partitions


def export_table(
    conn: sqlite3.Connection,
    table: str,
    out_dir: Path,
    chunk_size: int,
) -> dict:
    row_count = count_rows(conn, table)
    columns = table_columns(conn, table)
    partitions = partition_columns(columns)
    table_out_dir = out_dir / table

    if table_out_dir.exists():
        shutil.rmtree(table_out_dir, ignore_errors=True)
    table_out_dir.mkdir(parents=True, exist_ok=True)

    if row_count == 0:
        return {
            "table": table,
            "rows": 0,
            "columns": columns,
            "partition_cols": partitions,
            "path": str(table_out_dir),
            "status": "empty",
        }

    chunks_written = 0
    distinct_dates = None

    # Tables partitioned by both date and symbol can exceed Arrow's per-write
    # partition limit if we stream a large mixed chunk. Writing one date at a
    # time keeps each dataset write bounded while preserving the final layout.
    if partitions == ["date", "symbol"]:
        distinct_dates = [
            row[0]
            for row in conn.execute(
                f'SELECT DISTINCT date FROM "{table}" ORDER BY date'
            ).fetchall()
        ]
        for date_value in distinct_dates:
            query = f'SELECT * FROM "{table}" WHERE date = ?'
            chunk_iter = pd.read_sql_query(
                query, conn, params=(date_value,), chunksize=chunk_size
            )
            for chunk in chunk_iter:
                arrow_table = pa.Table.from_pandas(chunk, preserve_index=False)
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(table_out_dir),
                    partition_cols=partitions,
                    compression="snappy",
                )
                chunks_written += 1
    else:
        query = f'SELECT * FROM "{table}"'
        chunk_iter = pd.read_sql_query(query, conn, chunksize=chunk_size)
        for chunk in chunk_iter:
            arrow_table = pa.Table.from_pandas(chunk, preserve_index=False)
            pq.write_to_dataset(
                arrow_table,
                root_path=str(table_out_dir),
                partition_cols=partitions,
                compression="snappy",
            )
            chunks_written += 1

    return {
        "table": table,
        "rows": row_count,
        "columns": columns,
        "partition_cols": partitions,
        "path": str(table_out_dir),
        "chunks_written": chunks_written,
        "distinct_dates": 0 if distinct_dates is None else len(distinct_dates),
        "status": "exported",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export SQLite tables to partitioned Parquet datasets."
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Optional subset of table names to export.",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    out_dir = Path(args.out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)

    with get_conn(db_path) as conn:
        tables = args.tables or list_tables(conn)
        summary = []

        for table in tables:
            print(f"Exporting {table}...")
            result = export_table(conn, table, out_dir, args.chunk_size)
            summary.append(result)
            print(
                f"  {result['status']}: rows={result['rows']} "
                f"partitions={result['partition_cols']}"
            )

    summary_path = out_dir / "_export_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nWrote summary: {summary_path}")


if __name__ == "__main__":
    main()
