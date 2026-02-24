#!/usr/bin/env python3
"""Batch VIN decoder for vPIC.

Reads a CSV of VINs and queries the vPIC DecodeVinValuesBatch endpoint.
Results are cached and written to `out/vin_decode_results.csv`. Errors
to `out/vin_decode_errors.csv`.

Endpoint:
    https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesBatch/
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
from typing import Dict, Iterable, List, Optional, Tuple
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
INPUT_CSV = r"G:\My Drive\CODING - - -\DataSets\PAX DATA\VINs.csv"
OUTPUT_DIR = "out"
CACHE_PATH = "out/cache_vin_decode.json"
MAX_REQS_PER_SEC = 5
WORKERS = 2
BATCH_SIZE = 50


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
def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_vin_batch(
    session: requests.Session,
    vins: List[str],
    cache: Dict[str, dict],
    cache_path: Path,
    rate_limiter: "RateLimiter",
) -> Tuple[bool, dict]:
    """Decode a batch of VINs using vPIC."""
    url = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesBatch/"
    payload = {
        "DATA": ";".join(vins),
        "format": "json",
    }

    try:
        rate_limiter()
        r = session.post(url, data=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        results = data.get("Results", [])

        for row in results:
            vin_key = str(row.get("VIN", "")).strip()
            if vin_key:
                cache_set(cache, vin_key, row)
        save_cache(cache_path, cache)

        return True, {"url": url, "from_cache": False, "data": results}
    except Exception as e:
        return False, {"url": url, "error": str(e), "vins": vins}


def build_invalid_vin_row(row: dict) -> Optional[dict]:
    vin = str(row.get("VIN", "")).strip()
    error_code = str(row.get("ErrorCode", "")).strip()
    error_text = str(row.get("ErrorText", "")).strip()

    reasons = []
    if vin and len(vin) != 17:
        reasons.append("invalid_length")
    if error_code and error_code != "0":
        reasons.append("error_code")
    if error_text:
        reasons.append("error_text")

    if not reasons:
        return None

    return {
        "vin": vin,
        "error_code": error_code,
        "error_text": error_text,
        "reason": ";".join(reasons),
        "from_cache": bool(row.get("_from_cache", False)),
    }


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Batch VIN decoder (vPIC)")
    p.add_argument("--csv", default=INPUT_CSV, help="CSV file with VIN column")
    p.add_argument("--vin-column", default="VIN", help="VIN column name (case-insensitive)")
    p.add_argument("--outdir", default=OUTPUT_DIR, help="Output directory")
    p.add_argument("--workers", type=int, default=WORKERS, help="Parallel worker threads")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="VINs per API request")
    p.add_argument("--user-agent", help="Custom User-Agent header")
    p.add_argument("--cache", default=CACHE_PATH, help="Cache file path")
    p.add_argument("--max-requests-per-second", type=float, default=MAX_REQS_PER_SEC, help="Max API requests per second (NHTSA limit is 100, default 50 for safety)")
    # Postgres enrichment args
    p.add_argument("--pg-host", help="Postgres host (e.g. localhost)")
    p.add_argument("--pg-port", type=int, default=5432, help="Postgres port")
    p.add_argument("--pg-db", help="Postgres database name")
    p.add_argument("--pg-user", help="Postgres username")
    p.add_argument("--pg-password", help="Postgres password (or set PG_PASSWORD env)")
    p.add_argument("--pg-table", default="vin_decode_results", help="Postgres table to upsert results into")
    args = p.parse_args(argv)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    results_csv = outdir / "vin_decode_results.csv"
    errors_csv = outdir / "vin_decode_errors.csv"
    invalid_csv = outdir / "vin_decode_invalid.csv"
    cache_path = Path(args.cache)

    df = pd.read_csv(args.csv, dtype=str)
    vin_col = None
    for c in df.columns:
        if c.strip().lower() == args.vin_column.strip().lower():
            vin_col = c
            break
    if vin_col is None:
        raise SystemExit(f"CSV must have VIN column named '{args.vin_column}'")

    vins = (
        df[vin_col]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"": None})
        .dropna()
        .drop_duplicates()
        .tolist()
    )

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
                    vin TEXT PRIMARY KEY,
                    decode_json JSONB,
                    source_url TEXT,
                    from_cache BOOLEAN,
                    fetched_at TIMESTAMP DEFAULT now()
                )
                """)
        except Exception as e:
            print("Postgres connection failed:", e)
            pg_conn = None

    print("Decoding VINs via vPIC...")
    print(f"Will query {len(vins)} VINs")
    print(f"Saving results to {outdir}")

    # Create error file writer
    err_f = open(errors_csv, "w", newline="", encoding="utf-8")
    err_writer = csv.writer(err_f)
    err_writer.writerow(["vin", "error", "source_url"])

    cache = load_cache(cache_path)
    cached_rows = []
    uncached_vins = []
    for vin in vins:
        cached = cache_get(cache, vin)
        if cached is not None:
            row = dict(cached)
            row["_from_cache"] = True
            cached_rows.append(row)
        else:
            uncached_vins.append(vin)

    results_rows = []
    if cached_rows:
        results_rows.extend(cached_rows)

    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        future_to_meta = {}
        for batch in chunked(uncached_vins, args.batch_size):
            future = exe.submit(fetch_vin_batch, session, batch, cache, cache_path, rate_limiter)
            future_to_meta[future] = batch

        for future in as_completed(future_to_meta):
            batch = future_to_meta[future]
            ok, payload = future.result()
            if ok:
                rows = payload.get("data", [])
                for row in rows:
                    row = dict(row)
                    row["_from_cache"] = False
                    results_rows.append(row)
            else:
                for vin in payload.get("vins", batch):
                    err_writer.writerow([vin, payload.get("error"), payload.get("url")])

    if results_rows:
        df_out = pd.DataFrame(results_rows)
        df_out.to_csv(results_csv, index=False)

        invalid_rows = []
        for row in results_rows:
            invalid = build_invalid_vin_row(row)
            if invalid is not None:
                invalid_rows.append(invalid)

        if invalid_rows:
            pd.DataFrame(invalid_rows).to_csv(invalid_csv, index=False)

        if pg_conn is not None:
            with pg_conn.cursor() as cur:
                for row in results_rows:
                    vin = str(row.get("VIN", "")).strip()
                    if not vin:
                        continue
                    cur.execute(
                        f"""
                        INSERT INTO {psycopg2.sql.Identifier(args.pg_table).string}
                        (vin, decode_json, source_url, from_cache)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (vin) DO UPDATE SET
                            decode_json = EXCLUDED.decode_json,
                            source_url = EXCLUDED.source_url,
                            from_cache = EXCLUDED.from_cache,
                            fetched_at = now()
                        """,
                        (
                            vin,
                            json.dumps(row),
                            "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesBatch/",
                            bool(row.get("_from_cache", False)),
                        ),
                    )

    print("Done. Results:", results_csv, "Errors:", errors_csv)


if __name__ == "__main__":
    main()
