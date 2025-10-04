import sys
import os
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import ingest_311
from ingest_311 import SELECT_COLS


class DummyClient:
    def __init__(self, pages=2):
        self.pages = pages
        self.calls = 0

    def get(self, dataset, **kwargs):
        # emulate pages of results; return 'limit' rows for the first `pages` calls
        self.calls += 1
        limit = int(kwargs.get('limit', 1))
        if self.calls <= self.pages:
            rows = []
            for i in range(limit):
                # minimal fake row matching SELECT_COLS
                row = {c: None for c in SELECT_COLS}
                row['unique_key'] = f"k{self.calls}-{i}"
                row['created_date'] = '2025-01-01T00:00:00'
                row['latitude'] = '40.0'
                row['longitude'] = '-73.0'
                rows.append(row)
            return rows
        return []


def test_integration_writes_parquet(tmp_path, monkeypatch):
    # ensure ingest_311 uses our DummyClient instead of real Socrata
    monkeypatch.setenv('SOCRATA_APP_TOKEN', 'fake')
    monkeypatch.setattr(ingest_311, 'Socrata', lambda domain, token, timeout: DummyClient(pages=2))

    # run the script with a small limit and page size so it writes two files (3 + 2 rows)
    monkeypatch.setattr(sys, 'argv', ['ingest_311.py', '--limit', '5', '--page-size', '3', '--out-dir', str(tmp_path)])

    # execute main (should write parquet files into tmp_path)
    ingest_311.main()

    files = sorted(tmp_path.glob('nyc311_*.parquet'))
    assert len(files) == 2

    # validate contents of the parquet files
    df1 = pd.read_parquet(files[0])
    df2 = pd.read_parquet(files[1])

    assert len(df1) == 3
    assert len(df2) == 2

    # created_date should be parsed to datetime dtype
    assert 'created_date' in df1.columns
    assert pd.api.types.is_datetime64_any_dtype(df1['created_date'])

    # lat/long numeric
    assert pd.api.types.is_float_dtype(df1['latitude']) or pd.api.types.is_numeric_dtype(df1['latitude'])
    assert pd.api.types.is_float_dtype(df1['longitude']) or pd.api.types.is_numeric_dtype(df1['longitude'])
