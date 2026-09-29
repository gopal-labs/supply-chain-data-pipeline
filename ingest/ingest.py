"""
Ingestion script for the DataCo / Evoastra supply chain dataset.

Simulates a daily batch ingestion process by slicing a static source CSV
into partitions based on the order date column. Each run lands one or more
"batch_date" partitions into the raw (bronze) layer.

Usage:
    python ingest.py --batch-date 2018-01-13
    python ingest.py --backfill --start 2018-01-01 --end 2018-01-31
    python ingest.py --all
    python ingest.py --batch-date 2018-01-13 --no-overwrite
"""
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from config import SOURCE_CSV, RAW_DIR, LOG_DIR, DATE_COLUMN, ENCODING


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"ingest_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )
    return log_file


def load_source() -> pd.DataFrame:
    logging.info(f"Loading source dataset from {SOURCE_CSV}")
    if not SOURCE_CSV.exists():
        logging.error(f"Source file not found: {SOURCE_CSV}")
        sys.exit(1)

    df = pd.read_csv(SOURCE_CSV, encoding=ENCODING)
    logging.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors="coerce")
    bad_dates = int(df[DATE_COLUMN].isna().sum())
    if bad_dates:
        logging.warning(
            f"{bad_dates} rows had an unparseable '{DATE_COLUMN}' value and were dropped"
        )
        df = df.dropna(subset=[DATE_COLUMN])

    return df


def write_partition(df: pd.DataFrame, batch_date: str, overwrite: bool) -> Path:
    partition_dir = RAW_DIR / f"batch_date={batch_date}"
    output_path = partition_dir / "data.csv"

    if output_path.exists() and not overwrite:
        logging.info(f"Partition {batch_date} already exists, skipping (--no-overwrite set)")
        return output_path

    partition_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logging.info(f"Wrote {len(df):,} rows to {output_path}")
    return output_path


def ingest_batch(df: pd.DataFrame, batch_date: str, overwrite: bool) -> int:
    target_date = pd.to_datetime(batch_date).date()
    batch_df = df[df[DATE_COLUMN].dt.date == target_date]

    if batch_df.empty:
        logging.warning(f"No rows found for batch_date={batch_date}")
        return 0

    write_partition(batch_df, batch_date, overwrite=overwrite)
    return len(batch_df)


def daterange(start: datetime, end: datetime):
    for n in range((end - start).days + 1):
        yield (start + timedelta(days=n)).strftime("%Y-%m-%d")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest the supply chain dataset into daily batch partitions"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch-date", help="Single date to ingest, format YYYY-MM-DD")
    group.add_argument("--backfill", action="store_true", help="Ingest a range of dates")
    group.add_argument(
        "--all", action="store_true", help="Ingest every distinct date present in the source data"
    )
    parser.add_argument("--start", help="Start date for --backfill, format YYYY-MM-DD")
    parser.add_argument("--end", help="End date for --backfill, format YYYY-MM-DD")
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Skip partitions that already exist instead of overwriting them",
    )

    args = parser.parse_args()

    if args.backfill and (not args.start or not args.end):
        parser.error("--backfill requires --start and --end")

    log_file = setup_logging()
    logging.info("Ingestion run started")
    logging.info(f"Args: {vars(args)}")

    df = load_source()
    overwrite = not args.no_overwrite

    total_rows = 0
    partitions_written = 0

    if args.batch_date:
        rows = ingest_batch(df, args.batch_date, overwrite=overwrite)
        total_rows += rows
        partitions_written += 1 if rows else 0

    elif args.backfill:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d")
        for d in daterange(start, end):
            rows = ingest_batch(df, d, overwrite=overwrite)
            total_rows += rows
            partitions_written += 1 if rows else 0

    elif args.all:
        all_dates = sorted(df[DATE_COLUMN].dt.date.unique())
        logging.info(f"Found {len(all_dates)} distinct dates in source data")
        for d in all_dates:
            rows = ingest_batch(df, d.strftime("%Y-%m-%d"), overwrite=overwrite)
            total_rows += rows
            partitions_written += 1 if rows else 0

    logging.info(
        f"Ingestion run complete. {partitions_written} partition(s) written, "
        f"{total_rows:,} total rows."
    )
    logging.info(f"Log file: {log_file}")


if __name__ == "__main__":
    main()