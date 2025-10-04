#!/usr/bin/env python3
"""Precompute small aggregated summaries for the Streamlit UI.
Writes files to `data/summaries/`:
 - daily_summary.parquet: date, borough, complaint_type, tickets, median_response, pct_within
 - complaint_type_summary.parquet: complaint_type, borough, tickets, breach_rate (aggregated over all dates)
 - dow_hour_summary.parquet: dow_name, hour, borough, complaint_type, tickets, breach_rate, median_response

Usage:
  .venv312/bin/python scripts/precompute_summaries.py --sla 24

"""
import argparse
import duckdb
import pandas as pd
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--sla", type=float, default=float(24), help="SLA hours threshold used for pct_within calculation")
parser.add_argument("--out-dir", default="data/summaries", help="Output directory for summaries")
args = parser.parse_args()

OUT = Path(args.out_dir)
OUT.mkdir(parents=True, exist_ok=True)
BASE = "data/processed_part/*/*/part-*.parquet"
con = duckdb.connect()

print('Computing daily_summary...')
sla = args.sla
sql_daily = f"""
SELECT
    CAST(created_dt AS DATE) as date,
    borough,
    complaint_type,
    count(*) as tickets,
    median(response_hours) as median_response,
    avg(CASE WHEN response_hours <= {sla} THEN 1 ELSE 0 END) as pct_within
FROM parquet_scan('{BASE}')
WHERE created_dt IS NOT NULL
GROUP BY 1,2,3
ORDER BY 1 DESC
"""
df_daily = con.execute(sql_daily).df()
df_daily.to_parquet(OUT / 'daily_summary.parquet', index=False)
print('Wrote', OUT / 'daily_summary.parquet', 'rows:', len(df_daily))

print('Computing complaint_type_summary (aggregated over dates)...')
# Aggregate the daily_summary into complaint_type x borough over the full period
if len(df_daily):
    df_ct = (
        df_daily.groupby(['complaint_type','borough'], dropna=False, observed=True)
        .agg(tickets=('tickets','sum'),
             breaches=('tickets', lambda s: 0) # placeholder, will compute below
        )
        .reset_index()
    )
    # compute breaches and breach_rate correctly using weighted average from daily
    # To compute breaches: sum over rows of tickets*(1-pct_within)
    df_daily['breaches'] = df_daily['tickets'] * (1.0 - df_daily['pct_within'])
    df_ct = (
        df_daily.groupby(['complaint_type','borough'], dropna=False, observed=True)
        .agg(tickets=('tickets','sum'), breaches=('breaches','sum'))
        .reset_index()
    )
    df_ct['breach_rate'] = (df_ct['breaches'] / df_ct['tickets']).fillna(0.0)
    df_ct = df_ct[['complaint_type','borough','tickets','breach_rate']]
    df_ct.to_parquet(OUT / 'complaint_type_summary.parquet', index=False)
    print('Wrote', OUT / 'complaint_type_summary.parquet', 'rows:', len(df_ct))
else:
    print('No daily rows to aggregate for complaint_type_summary')

print('Computing dow_hour_summary...')
sql_dow = f"""
SELECT
    strftime(created_dt, '%A') as dow_name,
    extract(hour from created_dt) as hour,
    borough,
    complaint_type,
    count(*) as tickets,
    avg(CASE WHEN response_hours > {sla} THEN 1 ELSE 0 END) as breach_rate,
    median(response_hours) as median_response
FROM parquet_scan('{BASE}')
WHERE created_dt IS NOT NULL
GROUP BY 1,2,3,4
ORDER BY 1,2
"""
df_dow = con.execute(sql_dow).df()
df_dow.to_parquet(OUT / 'dow_hour_summary.parquet', index=False)
print('Wrote', OUT / 'dow_hour_summary.parquet', 'rows:', len(df_dow))

print('Done.')
