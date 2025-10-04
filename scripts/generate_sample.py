#!/usr/bin/env python3
"""Generate a small sample parquet file for demo/Cloud use.

Writes `data/sample/nyc311_50k.parquet` by default. Prefers partitioned dataset
`data/processed_part/*/*/part-*.parquet` when present, else falls back to
`data/processed/nyc311_clean.parquet`.

Usage:
    python scripts/generate_sample.py --rows 50000
"""
import argparse
from pathlib import Path
import duckdb

DEFAULT_ROWS = 50000
OUT_DIR = Path("data/sample")
OUT_FILE = OUT_DIR / f"nyc311_{DEFAULT_ROWS}.parquet"


def get_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    p.add_argument("--out", default=str(OUT_FILE))
    return p.parse_args()


def main():
    args = get_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # prefer partitioned dataset
    part_glob = "data/processed_part/*/*/part-*.parquet"
    single = "data/processed/nyc311_clean.parquet"
    base = part_glob if Path("data/processed_part").exists() else single

    con = duckdb.connect()
    # use random sampling via DuckDB to avoid loading everything into Python
    sql = f"""
        CREATE OR REPLACE TABLE sample AS
        SELECT * FROM parquet_scan('{base}')
        ORDER BY RANDOM()
        LIMIT {int(args.rows)}
    """
    try:
        con.execute(sql)
        # write out as parquet
        out_path = args.out
        con.execute(f"COPY sample TO '{out_path}' (FORMAT PARQUET)")
        print(f"Wrote sample → {out_path}")
    except Exception as e:
        print("Error generating sample:", e)
        raise


if __name__ == '__main__':
    main()
