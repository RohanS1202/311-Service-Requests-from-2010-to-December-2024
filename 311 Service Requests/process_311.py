# process_311.py
import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np
import os
import warnings

try:
    from holidays import US
    _HOLIDAYS_AVAILABLE = True
except Exception:  # pragma: no cover - environment-dependent
    US = None
    _HOLIDAYS_AVAILABLE = False
    warnings.warn("python-holidays not installed; 'is_holiday' will be all False."
                  "Install the 'holidays' package to enable holiday detection.")

SLA_HOURS = float(os.getenv("SLA_HOURS", "24"))

def load_raw():
    files = sorted(Path("data/raw").glob("nyc311_*.parquet"))
    if not files:
        raise SystemExit("No raw parquet files found in data/raw/. Run ingest_311.py first.")
    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    return df

def engineer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # unify timestamps (assume Eastern local timestamps as provided by NYC Open Data)
    # ensure created_date exists
    df = df[~df["created_date"].isna()].copy()

    # choose response_end = closed_date else resolution_action_updated_date
    # (Some tickets never close; we exclude rows with no end when computing response_hours)
    response_end = df["closed_date"].fillna(df["resolution_action_updated_date"])
    df["response_hours"] = (response_end - df["created_date"]).dt.total_seconds()/3600.0
    df.loc[response_end.isna(), "response_hours"] = np.nan

    # SLA
    df["within_sla"] = df["response_hours"].le(SLA_HOURS)

    # time features
    # parse timestamps as UTC if timezone-naive, then convert to America/New_York
    import pytz
    tz = pytz.timezone("America/New_York")
    df["created_dt"] = pd.to_datetime(df["created_date"])
    # if timezone-naive, treat as UTC then convert; otherwise keep tz-aware and convert to NY
    if df["created_dt"].dt.tz is None:
        df["created_dt"] = df["created_dt"].dt.tz_localize('UTC').dt.tz_convert(tz)
    else:
        df["created_dt"] = df["created_dt"].dt.tz_convert(tz)
    df["date"] = df["created_dt"].dt.date
    df["hour"] = df["created_dt"].dt.hour
    df["day_of_week"] = df["created_dt"].dt.day_of_week   # 0=Mon
    df["dow_name"] = df["created_dt"].dt.day_name()
    df["month"] = df["created_dt"].dt.month
    df["month_name"] = df["created_dt"].dt.month_name()

    # holidays (NY) — if the holidays package isn't available, default to False
    if _HOLIDAYS_AVAILABLE:
        ny_holidays = US(state='NY', years=sorted(df["created_dt"].dt.year.unique()))
        df["is_holiday"] = df["created_dt"].dt.date.map(lambda d: d in ny_holidays)
    else:
        df["is_holiday"] = False

    # small cleanups
    for c in ["borough","complaint_type","descriptor","status","agency","open_data_channel_type","city","incident_zip"]:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # normalize and fill city where missing: prefer borough when city is missing, then 'Unknown'
    if 'city' in df.columns:
        # if borough exists, use it as a fallback for missing city
        if 'borough' in df.columns:
            df['city'] = df['city'].fillna(df['borough'])
        # normalize casing to title-case and strip
        df['city'] = df['city'].str.title().str.strip()
        df['city'] = df['city'].fillna('Unknown')

    # save compact selection for app/analytics
    keep = [
        "unique_key","created_dt","date","hour","day_of_week","dow_name","month","month_name","is_holiday",
        "borough","complaint_type","descriptor","agency","status","open_data_channel_type",
        "response_hours","within_sla","latitude","longitude","incident_zip","city"
    ]
    keep = [c for c in keep if c in df.columns]
    return df[keep]

def main():
    df = load_raw()
    clean = engineer(df)
    # Schema guard: ensure expected columns exist and basic sanity checks before writing
    expected = {
        "unique_key","created_dt","borough","complaint_type","descriptor",
        "response_hours","within_sla","hour","dow_name","month_name"
    }
    missing = expected - set(clean.columns)
    assert not missing, f"Missing columns: {sorted(missing)}"
    assert clean["unique_key"].is_unique, "unique_key has duplicates"
    # quick row sanity
    assert clean["response_hours"].dropna().ge(0).all(), "negative response_hours found"
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    clean_path = Path("data/processed/nyc311_clean.parquet")
    clean.to_parquet(clean_path, index=False)
    print(f"Saved clean dataset → {clean_path} ({len(clean):,} rows)")
    # also write a partitioned dataset for fast filtering
    try:
        import pyarrow as pa, pyarrow.dataset as ds
        clean["year"] = clean["created_dt"].dt.year
        clean["month"] = clean["created_dt"].dt.month
        table = pa.Table.from_pandas(clean, preserve_index=False)
        ds.write_dataset(
            table, base_dir="data/processed_part/",
            format="parquet", partitioning=["year","month"], existing_data_behavior="overwrite_or_ignore"
        )
        print("Wrote partitioned dataset → data/processed_part/")
    except Exception as e:  # pragma: no cover - optional dependency
        print("Skipping partitioned write (pyarrow not available or error):", e)

if __name__ == "__main__":
    main()
