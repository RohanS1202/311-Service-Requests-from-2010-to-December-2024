import pandas as pd


def test_processed_schema():
    df = pd.read_parquet("data/processed/nyc311_clean.parquet")
    expected = {
        "unique_key","created_dt","borough","complaint_type","descriptor",
        "response_hours","within_sla","hour","dow_name","month_name"
    }
    assert expected.issubset(df.columns)
    assert df["unique_key"].is_unique
    assert (df["response_hours"].dropna() >= 0).all()
