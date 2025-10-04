# NYC 311 — Response Time & SLA Risk

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.x-ff4b4b.svg)](https://streamlit.io)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.x-7749BD.svg)](https://duckdb.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Interactive analytics for NYC 311 service requests using the public dataset **`erm2-nwe9`**.  
The app computes response times, flags SLA breaches, and surfaces risk patterns by complaint type, hour, and weekday—with sub-second interactivity on millions of rows (DuckDB + Parquet).

> **Live demo:** soon.. 
> **Data window (example):** 2020-01-01 → 2024-12-31

[Dashboard]   <img width="1512" height="859" alt="Screenshot 2025-10-04 at 1 55 29 AM" src="https://github.com/user-attachments/assets/07ba1990-119f-431c-a43b-7c5adce4389f" />
<img width="1512" height="859" alt="Screenshot 2025-10-04 at 1 55 48 AM" src="https://github.com/user-attachments/assets/55e417c5-5975-4dac-9db9-2669f43582b7" />
<img width="1512" height="865" alt="Screenshot 2025-10-04 at 1 56 30 AM" src="https://github.com/user-attachments/assets/fb63da5a-4f31-4c99-9423-c62300df2b25" />
<img width="1512" height="855" alt="Screenshot 2025-10-04 at 1 56 54 AM" src="https://github.com/user-attachments/assets/11e3199a-4c37-446c-bc2c-e8c16e895285" />
<img width="1512" height="857" alt="Screenshot 2025-10-04 at 1 57 41 AM" src="https://github.com/user-attachments/assets/e33d97fc-f147-413d-95f2-82d40c697732" />




---

## Table of contents
- [Features](#features)
- [Tech stack](#tech-stack)
- [Repository structure](#repository-structure)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Ingest & Process](#ingest--process)
- [Run the app](#run-the-app)
- [Tableau exports](#tableau-exports)
- [Performance design](#performance-design)
- [Testing & CI](#testing--ci)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [License](#license)

---

## Features
- **KPI tiles**: Median response time, % within SLA, ticket count (reactive to filters).
- **Breach rate by Complaint Type**: Ranked horizontal bars with % labels & tooltips.
- **Seasonality**:
  - Day × Hour **heatmap** (breach rate).
  - **Day-of-week** lines: median response hours and % within SLA.
- **Filters**: Date range, Borough, Complaint type, SLA threshold (presets + slider).
- **Data sample**: Interactive table + one-click CSV download of the current view.
- **Fast at scale**: DuckDB SQL over partitioned Parquet with result caching.

---

## Tech stack
- **Python 3.12**, **Streamlit** UI (Altair charts)
- **DuckDB** (in-process OLAP) querying **Parquet** (partitioned by year/month)
- **pandas/pyarrow** for ETL
- Optional **Tableau** extracts (CSV)

---

## Repository structure

```text
.
├── app_streamlit.py            # Streamlit app (DuckDB queries + Altair)
├── ingest_311.py               # Ingest NYC Open Data (erm2-nwe9) with date bounds
├── process_311.py              # Feature engineering → clean & partitioned Parquet
├── export_tableau.py           # Daily/borough & complaint-type summaries (CSV)
├── health_check.py             # Smoke test (DuckDB over parquet)
├── requirements.txt            # Pinned deps (tested on Python 3.12)
├── .env.example                # Local config template (no secrets committed)
├── .streamlit/
│   └── config.toml             # Stable file-watcher, theme tweaks
├── tests/                      # Minimal schema tests
├── data/                       # (gitignored) raw & processed data
├── exports/                    # (gitignored) Tableau CSVs
└── assets/                     # Screenshots/GIFs for README


---
```

## Quickstart

```bash
# 1) Create & activate a Python 3.12 virtual env
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip

# 2) Install dependencies
pip install -r requirements.txt

# 3) (Optional) Ingest + process a 2020→2024 window
python ingest_311.py --since 2020-01-01 --until 2024-12-31
python process_311.py

# 4) Run the dashboard
python -m streamlit run app_streamlit.py
```


## Configuration
```bash
SOCRATA_APP_TOKEN=PUT_YOUR_TOKEN_HERE
YEARS_BACK=5
SLA_HOURS=24
```
## Ingest & Process
Ingest (NYC Open Data)
Pull a fixed window (recommended) or let YEARS_BACK drive it:
```bash
python ingest_311.py --since 2020-01-01 --until 2024-12-31
```
Notes
Each ingest run writes to data/raw/<since>_<until>/nyc311_00001.parquet etc. (no overwrite).
Paging & ordering on created_date avoid API limits/timeouts.

## Process & feature engineering
```bash
python process_311.py
```

## Outputs:
data/processed/nyc311_clean.parquet (single file)
data/processed_part/ partitioned by year/month (fast scans)
Adds:
response_hours, within_sla (default threshold), hour, dow_name, month_name, is_holiday (if holidays installed).

## Run the app
```bash
python -m streamlit run app_streamlit.py --server.fileWatcherType=poll
```
DuckDB view over data/processed_part/**/*.parquet.
Query functions cached with @st.cache_data for snappy interactions.
## Tableau exports
Produce analyst-friendly CSVs:
```bash
python export_tableau.py
```
Emits to exports/:
daily_by_borough.csv
breach_rate_by_complaint.csv

## Performance design
Storage: columnar Parquet partitioned by year/month (prunes I/O).
Compute: DuckDB vectorized SQL with filter pushdown.
Caching: @st.cache_resource (DB handle) + @st.cache_data (query results).
UI: Altair with consistent theme; top-N bars and compact heatmap visualizations.
Typical interactions are sub-second on multi-million-row windows.

##Testing & CI
Local smoke test
```bash
python health_check.py
```
Asserts parquet is readable and returns row counts/min/max dates.
Minimal schema tests
```bash
pip install pytest
pytest -q
```
## GitHub Actions (optional)

.github/workflows/ci.yml
```yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: python -m pip install -U pip
      - run: pip install -r requirements.txt
      - run: python health_check.py
      - run: pip install pytest ruff
      - run: pytest -q
      - run: ruff check .
```
If your repo excludes data/, commit a tiny sample (e.g., data/sample/nyc311_50k.parquet) and make health_check.py use it on CI.

## Deployment
Streamlit Community Cloud
Push to GitHub.
“Deploy an app” → choose app_streamlit.py.

## Secrets:
```toml
SOCRATA_APP_TOKEN = "your_token_here"
```
(Optional) Ship a small sample parquet in data/sample/ so the cloud demo loads instantly.
Other targets
Any container host (Render/Fly.io/etc.)—install deps, set env var(s), run:
```bash
streamlit run app_streamlit.py
```
## Troubleshooting
Charts don’t change with filters
Selections are cleaned so “ALL” doesn’t override specific choices. The caption under the title echoes active filters and row count—use it to confirm.
Reload loop during development
Use .streamlit/config.toml:
```toml
[server]
```
fileWatcherType = "poll"
folderWatchBlacklist = ["data","exports",".venv",".pytest_cache","logs"]

Only one year appears
Re-ingest may have overwritten earlier raw files. This repo saves each ingest to a unique subfolder; re-run:
```bash
rm -rf data/processed* && python process_311.py
```
DuckDB not found in VS Code
Select the project’s .venv (Python 3.12) interpreter and pip install -r requirements.txt.

## Roadmap
Borough/ZIP map with breach hotspots (PyDeck).
Agency-specific SLA overrides (e.g., NYPD 8h, DSNY 48h).
Nightly ingest Action that writes to cloud storage.
Alerts when breach rate spikes (by complaint type).

## License
MIT. Uses public NYC Open Data; please review and honor their terms.

