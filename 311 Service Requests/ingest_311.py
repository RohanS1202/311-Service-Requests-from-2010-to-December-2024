# ingest_311.py
import os
import math
import time
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# load environment variables from .env early
load_dotenv()

from sodapy import Socrata
import pandas as pd
from pathlib import Path
from requests.exceptions import RequestException

DATASET_ID = "erm2-nwe9"  # NYC 311 Service Requests
DOMAIN = "data.cityofnewyork.us"

SELECT_COLS = [
    "unique_key","created_date","closed_date","resolution_action_updated_date",
    "agency","complaint_type","descriptor","status","borough","incident_zip",
    "city","open_data_channel_type","latitude","longitude"
]


def get_args():
    p = argparse.ArgumentParser(description="Ingest NYC 311 Service Requests to parquet files")
    p.add_argument("--years", type=int, default=int(os.getenv("YEARS_BACK", "5")),
                   help="How many years back to fetch (default from YEARS_BACK env or 5)")
    p.add_argument("--page-size", type=int, default=int(os.getenv("PAGE_SIZE", "50000")),
                   help="Rows per page/request")
    p.add_argument("--limit", type=int, default=None,
                   help="Optional max number of rows to fetch (useful for testing)")
    p.add_argument("--out-dir", default=os.getenv("OUT_DIR", "data/raw"),
                   help="Output directory for parquet files")
    p.add_argument("--max-retries", type=int, default=5,
                   help="Max retries for transient network errors")
    p.add_argument("--timeout", type=int, default=120,
                   help="Request timeout in seconds")
    p.add_argument("--dry-run", action="store_true",
                   help="Don't write parquet files, just show counts/pages")
    p.add_argument("--so-token-env", default="SOCRATA_APP_TOKEN",
                   help="Env var name that stores the Socrata app token (default SOCRATA_APP_TOKEN)")
    # Optional explicit date-range overrides (YYYY-MM-DD). If provided, these take
    # precedence over the --years/backfill behavior. Alternatively set SINCE_DATE
    # or UNTIL_DATE in your .env to achieve the same effect.
    p.add_argument("--since", help="inclusive start date YYYY-MM-DD")
    p.add_argument("--until", help="inclusive end date YYYY-MM-DD")
    return p.parse_args()


def get_with_retry(client, dataset, max_retries=5, **kwargs):
    """Wrapper around client.get that retries transient errors with backoff.

    Note: do NOT pass network-level kwargs like `timeout` into client.get because
    sodapy treats unknown kwargs as SoQL query parameters and will return 400.
    The Socrata client timeout should be configured in the Socrata(...) constructor.
    """
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            return client.get(dataset, **kwargs)
        except RequestException as exc:
            logging.warning("Request failed (attempt %d/%d): %s", attempt, max_retries, exc)
            if attempt == max_retries:
                logging.error("Max retries reached; re-raising exception")
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main():
    args = get_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # read token from env using provided env var name (fall back to a common alias)
    app_token = os.getenv(args.so_token_env) or os.getenv("SOCRATA_TOKEN")
    if not app_token:
        logging.warning("No %s found in environment; you'll be throttled.", args.so_token_env)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Date range selection: prefer --since/--until, then SINCE_DATE/UNTIL_DATE from
    # the environment, otherwise fall back to the years-back behavior.
    YEARS_BACK = int(os.getenv("YEARS_BACK", "5"))

    def _iso(d, end=False):
        # expect d in YYYY-MM-DD; return SoQL-style timestamp inclusive of the day
        return f"{d}T23:59:59" if end else f"{d}T00:00:00"

    # Parse & validate incoming dates (prefer CLI args, then .env)
    def _parse_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {s}")

    if args.since:
        since_date = _parse_date(args.since)
    elif os.getenv("SINCE_DATE"):
        since_date = _parse_date(os.getenv("SINCE_DATE"))
    else:
        since_date = (datetime.now() - timedelta(days=365 * YEARS_BACK)).date()

    if args.until:
        until_date = _parse_date(args.until)
    elif os.getenv("UNTIL_DATE"):
        until_date = _parse_date(os.getenv("UNTIL_DATE"))
    else:
        until_date = datetime.now().date()

    if since_date > until_date:
        raise SystemExit(f"since ({since_date.isoformat()}) is after until ({until_date.isoformat()})")

    since_iso = _iso(since_date.isoformat())
    until_iso = _iso(until_date.isoformat(), end=True)

    where = f"created_date between '{since_iso}' and '{until_iso}'"
    logging.info("WHERE %s", where)

    client = Socrata(DOMAIN, app_token, timeout=args.timeout)

    # If user provided a --limit, skip the expensive count() call and calculate pages from limit
    if args.limit:
        total_to_fetch = args.limit
    else:
        logging.info("Counting total rows for query to page through results...")
        count_res = get_with_retry(client, DATASET_ID, select="count(1)", where=where,
                                   max_retries=args.max_retries)
        total_to_fetch = int(count_res[0]["count_1"]) if count_res else 0

    page_size = args.page_size
    pages = math.ceil(total_to_fetch / page_size) if total_to_fetch else 0
    logging.info("Fetching ~%s rows for range %s to %s in %s pages (page_size=%s)",
                 f"{total_to_fetch:,}", since_iso, until_iso, pages, page_size)

    offset = 0
    page_idx = 0
    written = 0
    # tag filenames with the requested since date to avoid overwriting when
    # running multiple per-year ingests into the same out_dir
    since_tag = since_date.isoformat()
    while True:
        # adjust limit for last page or when args.limit is set
        this_limit = page_size
        if args.limit:
            remaining = args.limit - written
            if remaining <= 0:
                break
            this_limit = min(page_size, remaining)

        rows = get_with_retry(
            client,
            DATASET_ID,
            where=where,
            order="created_date ASC",
            select=",".join(SELECT_COLS),
            limit=this_limit,
            offset=offset,
            max_retries=args.max_retries,
        )
        if not rows:
            break

        df = pd.DataFrame.from_records(rows)
        # standardize dtypes now; safer for parquet & later processing
        for c in ["created_date", "closed_date", "resolution_action_updated_date"]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        for c in ["latitude", "longitude"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        page_idx += 1
        written += len(df)

        # include the requested since date in the filename so repeated per-year
        # ingests don't overwrite each other's page files when writing to the
        # same `out_dir`. Use ISO date (YYYY-MM-DD) which is filesystem-safe.
        out_path = out_dir / f"nyc311_{since_tag}_{page_idx:05d}.parquet"
        if args.dry_run:
            logging.info("Dry-run: would write %d rows → %s", len(df), out_path)
        else:
            df.to_parquet(out_path, index=False)
            logging.info("Wrote %d rows → %s", len(df), out_path)

        # stop if we retrieved fewer rows than requested for this page (end of data)
        if len(df) < this_limit:
            break

        # stop if we've reached the user-specified limit
        if args.limit and written >= args.limit:
            break

        offset += this_limit

    logging.info("Done. Total rows written: %d", written)


if __name__ == "__main__":
    main()
