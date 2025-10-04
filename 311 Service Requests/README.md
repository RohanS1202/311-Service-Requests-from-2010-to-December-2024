ingest_311.py — NYC 311 ingestion helper

Note: This project targets Python 3.12 for local development. Use a Python 3.12 virtualenv (see below).

Usage

This script downloads NYC 311 Service Requests from Socrata and writes them to parquet files.

Quick test (dry-run):

python3 ingest_311.py --years 1 --page-size 1000 --limit 5000 --dry-run

Normal run:

python3 ingest_311.py

Environment

Create a `.env` file in the same folder (you can copy `.env.example`). The key to add is:

SOCRATA_APP_TOKEN=your_app_token_here

If you don't provide an app token you'll be subject to Socrata throttling.

Keep secrets out of source control: this repo includes `.env.example` with placeholders; never commit an actual `.env` or `.streamlit/secrets.toml` file. In cloud/CI, store `SOCRATA_APP_TOKEN` as a secret and inject it into the environment.

Flags

--years      How many years back to fetch (default from YEARS_BACK env or 5)
--page-size  Rows per page/request
--limit      Optional max number of rows to fetch (useful for testing)
--out-dir    Output directory for parquet files
--max-retries Max retries for transient network errors
--timeout    Request timeout in seconds
--dry-run    Don't write parquet files, just show counts/pages

Notes

The script includes a small retry/backoff wrapper for transient network errors. For large runs, provide a valid `SOCRATA_APP_TOKEN` to avoid throttling and consider running on a machine with enough disk space for the parquet outputs.

Streamlit app

To run the dashboard locally:

1. Install streamlit (if you haven't):

	pip install streamlit

2. Run the app from the repo root:

	streamlit run "311 Service Requests/app_streamlit.py"

Or use the convenience script:

	./"311 Service Requests"/run_streamlit.sh

For faster development you can sample the processed dataset by setting:

	export STREAMLIT_MAX_ROWS=20000

This will cause the app to load only the first N rows from `data/processed/nyc311_clean.parquet`.

## Local development notes

- A recommended local virtual environment targeting Python 3.12 can be created as `.venv312`.
- To create and install dependencies into that venv:

```bash
python3.12 -m venv .venv312
.venv312/bin/python -m pip install -r requirements.txt
```

- A quick health-check script that verifies DuckDB can read the partitioned parquet dataset is available at `scripts/health_check.py`.
  Run it with the venv Python:

```bash
.venv312/bin/python scripts/health_check.py
```

Automation / daily refresh

- Makefile targets:

```bash
make venv       # create .venv312
make install    # install deps into .venv312
make precompute # run precompute summaries
make health     # run health check
```

- To run the precompute periodically on macOS, you can install the provided `launchd` plist in `launchd/com.example.refresh_summaries.plist` (edit the path if your repo is not at the same `pwd`).

- For CI/GitHub Actions, there is a scheduled workflow at `.github/workflows/refresh_summaries.yml` which runs daily and uploads the `data/summaries` as an artifact. If you want the action to push the summaries back into a storage bucket, I can add steps for that (S3/GCS/etc.).
