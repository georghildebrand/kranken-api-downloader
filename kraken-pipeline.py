import csv
import os
import sys
import argparse
import datetime as dt
import requests as r
import pandas as pd
from pathlib import Path
import re
import traceback
import logging

# Constants
PAIRS_URL = 'https://api.kraken.com/0/public/AssetPairs'
TIME_URL = 'https://api.kraken.com/0/public/Time'
OHLC_URL = 'https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1&since={since}'


def setup_logging(log_file, log_level):
    logger = logging.getLogger("kraken_pipeline")
    logger.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    return logger


def ensure_dir(path):
    path.mkdir(parents=True, exist_ok=True)


def is_valid_parquet(file_path, logger=None):
    try:
        pd.read_parquet(file_path, engine='pyarrow')
        return True
    except Exception as e:
        if logger:
            logger.warning("Parquet validation failed for %s: %s", file_path, e)
        return False


def download_data(base_path, logger, selected_pairs=None):
    try:
        logger.debug("Fetching Kraken server time...")
        resp = r.get(TIME_URL).json()
        server_now = resp['result']['unixtime']
        start = server_now - 12 * 60 * 60
        logger.debug(f"Server time: {server_now}, Start time: {start}")
    except Exception as e:
        logger.error("Error fetching server time: %s", str(e))
        return

    try:
        logger.debug("Fetching available asset pairs from Kraken...")
        pairs = r.get(PAIRS_URL).json()['result'].keys()
        if selected_pairs:
            logger.debug("Filtering for selected pairs only: %s", selected_pairs)
            pairs = [p for p in pairs if p in selected_pairs]
        logger.info("Downloading data for %d pairs", len(pairs))
    except Exception as e:
        logger.error("Error fetching asset pairs: %s", str(e))
        return

    today = dt.datetime.now()
    year = today.strftime('%Y')
    month = today.strftime('%m')
    folder = base_path / year / month
    ensure_dir(folder)

    for pair in pairs:
        try:
            logger.debug("Requesting OHLC data for pair: %s", pair)
            resp = r.get(OHLC_URL.format(pair=pair, since=start)).json()
            result = resp['result'].get(pair)
            if not result:
                logger.debug("No data returned for %s", pair)
                continue

            filename = today.strftime('%Y-%m-%d-%H-%M') + f'-{pair}.csv'
            file_path = folder / filename

            logger.debug("Writing data to file: %s", file_path)
            with open(file_path, 'w') as f:
                f.write("time,open,high,low,close,vwap,volume,count,pair\n")
                for row in result:
                    row[0] = dt.datetime.fromtimestamp(row[0]).strftime('%Y-%m-%d %H:%M')
                    row.append(pair)
                    f.write(",".join(map(str, row)) + "\n")
        except Exception as e:
            logger.error("Failed to fetch/write data for %s: %s", pair, str(e))


def process_csvs(input_path, parquet_path, delete_csv, logger):
    all_csvs = list(input_path.rglob("*.csv"))
    logger.debug("Found %d CSV files to process", len(all_csvs))
    for csv_file in all_csvs:
        if csv_file.name.endswith(".copied"):
            continue

        try:
            match = re.search(r"(\d{4})/(\d{2})", str(csv_file.parent))
            if not match:
                continue
            year, month = match.group(1), match.group(2)
            parquet_file = parquet_path / year / month / f"{year}-{month}.parquet"
            ensure_dir(parquet_file.parent)

            if csv_file.stat().st_size == 0:
                logger.warning("Skipping empty file: %s", csv_file)
                continue

            logger.debug("Reading CSV file: %s", csv_file)
            df_new = pd.read_csv(csv_file, low_memory=False)

            if parquet_file.exists():
                if not is_valid_parquet(parquet_file, logger):
                    logger.warning("Deleting corrupted Parquet file: %s", parquet_file)
                    parquet_file.unlink()
                else:
                    df_old = pd.read_parquet(parquet_file)
                    df_new = pd.concat([df_old, df_new], ignore_index=True)

            logger.debug("Writing combined DataFrame to Parquet: %s", parquet_file)
            df_new.to_parquet(parquet_file, index=False, compression="gzip")

            copied = csv_file.with_suffix(csv_file.suffix + ".copied")
            logger.debug("Renaming CSV to: %s", copied)
            csv_file.rename(copied)

            if delete_csv:
                logger.debug("Deleting copied CSV file: %s", copied)
                copied.unlink()
        except Exception as e:
            logger.error("Error processing %s:\n%s", csv_file, traceback.format_exc())


def restore_copied(input_path, logger):
    count = 0
    for f in input_path.rglob("*.csv.copied"):
        original = f.with_suffix(".csv")
        f.rename(original)
        count += 1
    logger.info("Restored %d files.", count)


def migrate_existing(input_path, output_path, logger, delete_csv=False, mark_errors=False):
    all_csvs = list(input_path.rglob("*.csv"))
    logger.debug("Found %d CSV files to migrate", len(all_csvs))
    for csv_file in all_csvs:
        try:
            year = csv_file.parents[1].name
            month = csv_file.parents[0].name
        except IndexError:
            logger.error("Path structure too short: %s", csv_file)
            continue

        parquet_file = output_path / year / month / f"{year}-{month}.parquet"
        ensure_dir(parquet_file.parent)

        try:
            if csv_file.stat().st_size == 0:
                raise pd.errors.EmptyDataError("File is empty")

            df = pd.read_csv(csv_file, low_memory=True)

            if parquet_file.exists():
                if not is_valid_parquet(parquet_file, logger):
                    logger.warning("Deleting invalid Parquet file: %s", parquet_file)
                    parquet_file.unlink()
                    df_combined = df
                else:
                    df_old = pd.read_parquet(parquet_file)
                    df_combined = pd.concat([df_old, df], ignore_index=True)
            else:
                df_combined = df

            logger.debug("Writing migrated Parquet: %s", parquet_file)
            df_combined.to_parquet(parquet_file, index=False, compression="gzip")

            copied = csv_file.with_suffix(csv_file.suffix + ".copied")
            logger.debug("Renaming migrated CSV to: %s", copied)
            csv_file.rename(copied)

            if delete_csv:
                logger.debug("Deleting copied CSV file: %s", copied)
                copied.unlink()

        except pd.errors.EmptyDataError:
            logger.warning("Empty CSV skipped: %s", csv_file)
            if mark_errors:
                error_file = csv_file.with_suffix(csv_file.suffix + ".error")
                csv_file.rename(error_file)
                logger.error("Marked file as error: %s", error_file)
        except Exception as e:
            logger.error("Failed to migrate %s: %s", csv_file, e)
            if mark_errors:
                error_file = csv_file.with_suffix(csv_file.suffix + ".error")
                csv_file.rename(error_file)
                logger.error("Marked file as error: %s", error_file)


def main():
    parser = argparse.ArgumentParser(description="Kraken CSV Downloader and Parquet Archiver")
    parser.add_argument("--input", "-i", type=str, required=True, help="Input data root folder")
    parser.add_argument("--output", "-o", type=str, default="parquet-data", help="Parquet output folder")
    parser.add_argument("--restore", action="store_true", help="Restore .csv.copied files to .csv")
    parser.add_argument("--migrate", action="store_true", help="Migrate all existing archive to parquet")
    parser.add_argument("--delete-csv", action="store_true", help="Delete .csv.copied files after archiving")
    parser.add_argument("--download", action="store_true", help="Download new Kraken data before processing")
    parser.add_argument("--pairs", nargs='+', help="List of asset pairs to download (e.g., XETHZEUR XXBTZUSD)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    parser.add_argument("--mark-errors", action="store_true", help="Rename failed .csv files to .error instead of .copied")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    log_file = input_path / "pipeline.log"
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logger = setup_logging(log_file, log_level)

    if args.download:
        download_data(input_path, logger, selected_pairs=args.pairs)

    if args.migrate:
        migrate_existing(input_path, output_path, logger, delete_csv=args.delete_csv, mark_errors=args.mark_errors)

    if args.restore:
        restore_copied(input_path, logger)

    process_csvs(input_path, output_path, args.delete_csv, logger)
    logger.info("✅ All tasks done.")


if __name__ == "__main__":
    main()
