#!/usr/bin/env python3
"""Health check script: runs a small DuckDB query against the partitioned parquet dataset.
Exits 0 on success, non-zero on failure. Prints summary output.

Usage: .venv312/bin/python scripts/health_check.py
"""
import sys
try:
    import duckdb
except Exception as e:
    print('ERROR: could not import duckdb:', e)
    sys.exit(2)

try:
    conn = duckdb.connect(':memory:')
    df = conn.execute("select count(*) as cnt from parquet_scan('data/processed_part/*/*/part-*.parquet')").df()
    cnt = int(df['cnt'].iloc[0])
    print('rows:', cnt)
    if cnt < 1000:
        print('ERROR: row count suspiciously low')
        sys.exit(3)
    print('Health check OK')
    sys.exit(0)
except Exception as e:
    print('ERROR running query:', e)
    sys.exit(4)
