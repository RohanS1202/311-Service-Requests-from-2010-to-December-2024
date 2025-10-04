import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_get_args_env_defaults(monkeypatch):
    # set env defaults and call get_args() with no argv extras
    monkeypatch.setenv('YEARS_BACK', '2')
    monkeypatch.setenv('PAGE_SIZE', '123')
    monkeypatch.setenv('OUT_DIR', 'tmp/out')
    monkeypatch.setattr('sys.argv', ['ingest_311.py'])

    from ingest_311 import get_args
    args = get_args()

    assert args.years == 2
    assert args.page_size == 123
    assert args.out_dir == 'tmp/out'


def test_get_args_cli_overrides(monkeypatch):
    monkeypatch.setenv('YEARS_BACK', '5')
    monkeypatch.setenv('PAGE_SIZE', '50000')
    monkeypatch.setattr('sys.argv', ['ingest_311.py', '--years', '1', '--page-size', '500', '--out-dir', 'o'])

    from ingest_311 import get_args
    args = get_args()

    assert args.years == 1
    assert args.page_size == 500
    assert args.out_dir == 'o'
