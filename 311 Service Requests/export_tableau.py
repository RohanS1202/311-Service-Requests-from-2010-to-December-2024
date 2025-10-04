import pandas as pd
from pathlib import Path
from datetime import datetime

clean_path = Path("data/processed/nyc311_clean.parquet")
df = pd.read_parquet(clean_path)
df["created_dt"] = pd.to_datetime(df["created_dt"])
df["date"] = df["created_dt"].dt.date

# daily x borough summary
daily_boro = (
    df.dropna(subset=["response_hours"])
      .groupby(["date","borough"], dropna=False)
      .agg(
          tickets=("unique_key","count"),
          median_response=("response_hours","median"),
          pct_within_sla=("within_sla","mean")
      )
      .reset_index()
)
daily_boro["pct_within_sla"] = (daily_boro["pct_within_sla"]*100).round(2)

# complaint-type breach rates (overall)
ctype = (
    df.dropna(subset=["response_hours"])
      .assign(breach=lambda x: ~x["within_sla"])
      .groupby(["complaint_type"], dropna=False)
      .agg(tickets=("unique_key","count"), breaches=("breach","sum"))
      .assign(breach_rate=lambda x: (x["breaches"]/x["tickets"]*100).round(2))
      .reset_index()
      .sort_values("tickets", ascending=False)
)

Path("exports").mkdir(exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
daily_path = Path(f"exports/daily_borough_summary_{ts}.csv")
ctype_path = Path(f"exports/complaint_type_breach_rates_{ts}.csv")
daily_boro.to_csv(daily_path, index=False)
ctype.to_csv(ctype_path, index=False)
print(f"Wrote {daily_path} and {ctype_path}")
