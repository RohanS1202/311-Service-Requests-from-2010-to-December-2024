import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import types
from ingest_311 import get_with_retry, SELECT_COLS


class DummyClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls = 0

    def get(self, dataset, **kwargs):
        # return one page of fake rows until pages exhausted
        self.calls += 1
        if self.calls <= self.pages:
            return [ {c: None for c in SELECT_COLS} for _ in range(kwargs.get('limit', 1)) ]
        return []


def test_get_with_retry_success(monkeypatch):
    client = DummyClient(pages=1)
    rows = get_with_retry(client, 'ds', limit=3)
    assert isinstance(rows, list)
    assert len(rows) == 3


def test_dummy_client_pages(monkeypatch):
    client = DummyClient(pages=2)
    rows1 = client.get('ds', limit=2)
    rows2 = client.get('ds', limit=2)
    rows3 = client.get('ds', limit=2)

    assert len(rows1) == 2
    assert len(rows2) == 2
    assert rows3 == []
