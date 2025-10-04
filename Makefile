# Makefile for Project A (311 Service Requests)
.PHONY: venv install precompute health clean
.PHONY: run ingest2020_2024 build

VENV=.venv312
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

venv:
	python3.12 -m venv $(VENV)
	$(PY) -m pip install --upgrade pip

install: venv
	$(PIP) install -r "311 Service Requests/requirements.txt"

precompute: install
	$(PY) scripts/precompute_summaries.py --sla 24

health: install
	$(PY) scripts/health_check.py

clean:
	rm -rf data/summaries/*

run:
	. .venv/bin/activate && python -m streamlit run "311 Service Requests/app_streamlit.py" --server.fileWatcherType=poll

ingest2020_2024:
	. .venv/bin/activate && python "311 Service Requests/ingest_311.py" --since 2020-01-01 --until 2024-12-31

build:
	. .venv/bin/activate && python "311 Service Requests/process_311.py" && python "311 Service Requests/export_tableau.py"
