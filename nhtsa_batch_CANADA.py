#!/usr/bin/env python3
"""Batch caller for Canadian vehicle specifications from vPIC.

Reads a CSV of vehicles (headers: year, make) and queries the Canadian
GetCanadianVehicleSpecifications endpoint. Results are cached and
written to `out/canada_results.csv`. Errors to `out/canada_errors.csv`.

The endpoint returns vehicle specifications as CSV format:
  https://vpic.nhtsa.dot.gov/vehicles/GetCanadianVehicleSpecifications/?year=2011&make=Acura&format=csv
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import deque
import tempfile
import io

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
import psycopg2.extras
from datetime import datetime
import threading

# User-defined input/output variables (override via CLI args)
# INPUT_CSV -> --csv
# OUTPUT_DIR -> --outdir
# CACHE_PATH -> --cache
# MAX_REQS_PER_SEC -> --max-requests-per-second
# WORKERS -> --workers
INPUT_CSV = "year_make_list.csv"
OUTPUT_DIR = "out"
CACHE_PATH = "out/cache_canadian.json"
MAX_REQS_PER_SEC = 5
WORKERS = 2


def make_session(retries: int = 3, backoff_factor: float = 0.5, user_agent: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=(429, 500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    ua = user_agent or "python-requests/vpic-batch"
    s.headers.update({"User-Agent": ua, "Accept": "application/json, text/plain;q=0.9,*/*;q=0.8"})
    return s


def cache_get(cache: Dict[str, dict], key: str) -> Optional[dict]:
    return cache.get(key)


def cache_set(cache: Dict[str, dict], key: str, value: dict) -> None:
    cache[key] = value

import logging

logger = logging.getLogger(__name__)

def load_cache(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning(f"Cache file not found: {path}")
        return {}
    except json.JSONDecodeError as e:
        logger.warning(f"Cache file corrupted: {path} - {e}")
        return {}

#handles file I/O using pathlib GEMINI Edits (tempfile)
def save_cache(path: Path, cache: Dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create a temp file in the same directory
    with tempfile.NamedTemporaryFile('w', dir=path.parent, delete=False, encoding="utf-8") as tf:
        json.dump(cache, tf, ensure_ascii=False, indent=2)
        temp_name = tf.name
    
    # Replace the old cache with the new one atomically
    os.replace(temp_name, path)

#deterministic fingerprint for URL caching
def url_cache_key(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


#rate limits the API calls
#edits from gemini to use while loop for more accurate safe timing
class RateLimiter:
    def __init__(self, max_calls: int, time_period: float):
        self.max_calls = max_calls
        self.time_period = time_period
        self.lock = threading.Lock()
        self.calls = deque() # More efficient for popping from the left

    def __call__(self) -> None:
        with self.lock:
            while True:
                now = time.time()
                
                # Clean up expired calls
                while self.calls and now - self.calls[0] >= self.time_period:
                    self.calls.popleft()

                if len(self.calls) < self.max_calls:
                    # Success: Record the call and exit the loop
                    self.calls.append(now)
                    return

                # Wait until the oldest call expires
                sleep_time = self.time_period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                # Loop continues to re-verify after sleeping

#defensive function for interacting with the NHTSA API
def fetch_canadian_specs(session: requests.Session, year: str, make: str, cache: Dict[str, dict], cache_path: Path, rate_limiter: 'RateLimiter') -> Tuple[bool, dict]:
    """Fetch Canadian vehicle specifications for a year/make combination.
    
    The endpoint returns CSV format data with all specifications for that year/make.
    """
    url = f"https://vpic.nhtsa.dot.gov/api/vehicles/GetCanadianVehicleSpecifications/?year={year}&make={make}&format=csv"
    key = url_cache_key(url)
    
    cached = cache_get(cache, key)
    if cached is not None:
        return True, {"url": url, "from_cache": True, "data": cached}

    try:
        rate_limiter()
        r = session.get(url, timeout=30)
        r.raise_for_status()
        
        # Parse the CSV response
        csv_content = r.text
        # Store the raw CSV for caching
        cache_set(cache, key, csv_content)
        save_cache(cache_path, cache)
        
        return True, {"url": url, "from_cache": False, "data": csv_content}
    except Exception as e:
        return False, {"url": url, "error": str(e)}


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Batch Canadian vehicle specifications fetcher")
    p.add_argument("--csv", default=INPUT_CSV, help="CSV file with headers year,make (model is optional, will be ignored)")
    p.add_argument("--outdir", default=OUTPUT_DIR, help="Output directory")
    p.add_argument("--workers", type=int, default=WORKERS, help="Parallel worker threads")
    p.add_argument("--user-agent", help="Custom User-Agent header")
    p.add_argument("--cache", default=CACHE_PATH, help="Cache file path")
    p.add_argument("--max-requests-per-second", type=float, default=MAX_REQS_PER_SEC, help="Max API requests per second (NHTSA limit is 100, default 50 for safety)")
    # Postgres enrichment args
    p.add_argument("--pg-host", help="Postgres host (e.g. localhost)")
    p.add_argument("--pg-port", type=int, default=5432, help="Postgres port")
    p.add_argument("--pg-db", help="Postgres database name")
    p.add_argument("--pg-user", help="Postgres username")
    p.add_argument("--pg-password", help="Postgres password (or set PG_PASSWORD env)")
    p.add_argument("--pg-table", default="canadian_vehicle_specs", help="Postgres table to upsert results into")
    args = p.parse_args(argv)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    results_csv = outdir / "canada_results.csv"
    errors_csv = outdir / "canada_errors.csv"
    cache_path = Path(args.cache)

    df = pd.read_csv(args.csv, dtype=str)
    # normalize columns - for Canadian specs, we only need year and make
    if not all(c in df.columns for c in ["year", "make"]):
        raise SystemExit("CSV must have headers: year, make")

    # Use only year and make (model is ignored for Canadian API)
    vehicles = df[["year", "make"]].drop_duplicates().to_records(index=False)

    session = make_session(user_agent=args.user_agent)
    cache = load_cache(cache_path)
    rate_limiter = RateLimiter(max_calls=int(args.max_requests_per_second), time_period=1)

    # Optional Postgres connection
    pg_conn = None
    if args.pg_host and args.pg_db and args.pg_user:
        pg_password = args.pg_password or os.environ.get("PG_PASSWORD")
        conn_info = {
            "host": args.pg_host,
            "port": args.pg_port,
            "dbname": args.pg_db,
            "user": args.pg_user,
            "password": pg_password,
        }
        try:
            pg_conn = psycopg2.connect(**{k: v for k, v in conn_info.items() if v is not None})
            pg_conn.autocommit = True
            print("Connected to Postgres for enrichment")
            # create table if not exists
            with pg_conn.cursor() as cur:
                cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {psycopg2.sql.Identifier(args.pg_table).string} (
                    year TEXT,
                    make TEXT,
                    model TEXT,
                    variable_id INTEGER,
                    variable_name TEXT,
                    values_json JSONB,
                    source_url TEXT,
                    from_cache BOOLEAN,
                    fetched_at TIMESTAMP DEFAULT now(),
                    PRIMARY KEY (year, make, model, variable_id)
                )
                """)
        except Exception as e:
            print("Postgres connection failed:", e)
            pg_conn = None

    print("Fetching Canadian specifications...")
    print(f"Will query {len(vehicles)} year/make combinations")
    print(f"Saving raw CSVs to {outdir}")

    # Create error file writer
    err_f = open(errors_csv, "w", newline="", encoding="utf-8")
    err_writer = csv.writer(err_f)
    err_writer.writerow(["year", "make", "error", "source_url"])

    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        future_to_meta = {}
        for year, make in vehicles:
            future = exe.submit(fetch_canadian_specs, session, year, make, cache, cache_path, rate_limiter)
            future_to_meta[future] = (year, make)

        for future in as_completed(future_to_meta):
            year, make = future_to_meta[future]
            ok, payload = future.result()
            if ok:
                csv_data = payload.get("data")
                
                # Save raw CSV to file: {year}_{make}.csv
                safe_make = make.replace(" ", "_").replace("/", "_")
                output_file = outdir / f"{year}_{safe_make}.csv"
                
                try:
                    with open(output_file, "w", encoding="utf-8") as f:
                        f.write(csv_data)
                    print(f"Saved: {output_file}")
                except Exception as e:
                    print(f"Error saving {output_file}: {e}")
                    err_writer.writerow([year, make, f"Failed to save file: {e}", payload.get("url")])
            else:
                err_writer.writerow([year, make, payload.get("error"), payload.get("url")])
    print("Done. Results:", results_csv, "Errors:", errors_csv)


if __name__ == "__main__":
    main()
