import argparse
import datetime as dt
import logging
import os
import sys
import traceback
from pathlib import Path
import pandas as pd
import requests as r

PAIRS_URL = 'https://api.kraken.com/0/public/AssetPairs'
TIME_URL = 'https://api.kraken.com/0/public/Time'
OHLC_URL = 'https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1&since={since}'


def setup_logging(log_file, level):
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("kraken_pipeline")
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    fh.setLevel(level)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    ch.setLevel(level)
    logger.addHandler(ch)

    return logger


def ensure_dir(path):
    path.mkdir(parents=True, exist_ok=True)


def is_valid_parquet(path):
    try:
        pd.read_parquet(path)
        return True
    except Exception as e:
        return False


def restore_copied_files(logger, input_path):
    restored = 0
    for file in input_path.rglob("*.csv.copied"):
        original = file.with_suffix("")
        file.rename(original)
        logger.info(f"Restored: {file} -> {original}")
        restored += 1
    logger.info(f"🔁 Restored {restored} .copied files.")


def restore_error_files(logger, input_path):
    restored = 0
    for file in input_path.rglob("*.csv.error"):
        original = file.with_suffix("")
        file.rename(original)
        logger.info(f"Restored: {file} -> {original}")
        restored += 1
    logger.info(f"🔁 Restored {restored} .error files.")


def download_recent_data(logger, output_path, pairs=None, keep_csv=True):
    try:
        server_time = r.get(TIME_URL).json()['result']['unixtime']
        since = server_time - 12 * 60 * 60
        logger.info(f"Kraken server time: {server_time}, fetching since: {since}")
    except Exception as e:
        logger.error("Error fetching Kraken time: %s", e)
        return

    try:
        all_pairs = r.get(PAIRS_URL).json()['result'].keys()
        if pairs:
            all_pairs = [p for p in all_pairs if p in pairs]
        logger.info(f"Fetching data for {len(all_pairs)} pairs")
    except Exception as e:
        logger.error("Error fetching Kraken pairs: %s", e)
        return

    today = dt.datetime.utcnow()
    year, month = today.strftime('%Y'), today.strftime('%m')
    target_file = output_path / year / month / f"{year}-{month}.parquet"
    ensure_dir(target_file.parent)

    all_data = []

    for pair in all_pairs:
        try:
            logger.debug(f"Downloading OHLC data for {pair}")
            url = OHLC_URL.format(pair=pair, since=since)
            resp = r.get(url).json()['result'].get(pair, [])
            for row in resp:
                all_data.append([
                    dt.datetime.fromtimestamp(row[0]).strftime('%Y-%m-%d %H:%M'),
                    *row[1:8],
                    pair
                ])
        except Exception as e:
            logger.warning("Failed for %s: %s", pair, e)

    if not all_data:
        logger.warning("No data downloaded.")
        return

    df = pd.DataFrame(all_data, columns=[
        "time", "open", "high", "low", "close", "vwap", "volume", "count", "pair"
    ])

    if keep_csv:
        csv_folder = output_path / year / month
        ensure_dir(csv_folder)
        for pair in df['pair'].unique():
            pair_df = df[df['pair'] == pair].copy()
            timestamp = today.replace(minute=0, second=0, microsecond=0).strftime('%Y-%m-%d-%H-%M')
            csv_file = csv_folder / f"{timestamp}-{pair}.csv"
            pair_df.to_csv(csv_file, index=False)
            logger.info(f"📝 Saved CSV to {csv_file}")

    if target_file.exists():
        try:
            if is_valid_parquet(target_file):
                old = pd.read_parquet(target_file)
                df = pd.concat([old, df], ignore_index=True)
            else:
                raise ValueError("Invalid Parquet file")
        except Exception as e:
            logger.warning(f"Replacing invalid Parquet file: {target_file} due to error: {e}")
            target_file.unlink()

    df.drop_duplicates(subset=["time", "pair"], inplace=True)
    df.to_parquet(target_file, index=False, compression="gzip")
    logger.info(f"✅ Stored {len(df)} rows in {target_file}")


def migrate_csv_archive(logger, input_path, output_path, mark_errors=False, delete_csv=False):
    csvs = list(input_path.rglob("*.csv"))
    logger.info(f"Found {len(csvs)} CSV files to migrate")
    migrated, skipped, failed = 0, 0, 0


    for csv_file in csvs:
        try:
            year, month = csv_file.parents[1].name, csv_file.parents[0].name
            target_file = output_path / year / month / f"{year}-{month}.parquet"
            ensure_dir(target_file.parent)

            if csv_file.stat().st_size == 0:
                raise pd.errors.EmptyDataError("Empty file")

            try:
                df = pd.read_csv(csv_file, low_memory=False)
                if len(df.columns) == 1:
                    raise ValueError(f"Likely delimiter issue in {csv_file.name}: found only one column: {df.columns[0]}")
                # Custom logic
                if 'countpair' in df.columns and 'count' not in df.columns and 'pair' not in df.columns:
                    df[['count', 'pair']] = df['countpair'].astype(str).str.extract(r'(\d+)([A-Z0-9]+)')
                    df.drop(columns=['countpair'], inplace=True)
                expected_columns = {"time", "pair"}
                missing_columns = expected_columns - set(df.columns)
                if missing_columns:
                    raise ValueError(f"Missing expected columns in {csv_file.name}: {missing_columns}. Found columns: {df.columns.tolist()}")
            except (pd.errors.ParserError, UnicodeDecodeError) as e:
                raise ValueError(f"Invalid CSV format: {e}")

            if target_file.exists():
                try:
                    if is_valid_parquet(target_file):
                        old = pd.read_parquet(target_file)
                        df = pd.concat([old, df], ignore_index=True)
                    else:
                        raise ValueError("Invalid Parquet file")
                except Exception as e:
                    logger.warning(f"Deleting invalid Parquet file: {target_file} due to: {e}")
                    target_file.unlink()

            df.drop_duplicates(subset=["time", "pair"], inplace=True)
            df.to_parquet(target_file, index=False, compression="gzip")

            copied = csv_file.with_suffix(csv_file.suffix + ".copied")
            csv_file.rename(copied)

            if delete_csv:
                copied.unlink()

            migrated += 1
            logger.info(f"Migrated: {csv_file} → {target_file}")

        except (pd.errors.EmptyDataError, ValueError) as ve:
            logger.warning(f"File skipped: {csv_file} due to: {ve}")
            skipped += 1
            if mark_errors:
                csv_file.rename(csv_file.with_suffix(".error"))
        except Exception as e:
            logger.error(f"Error migrating {csv_file}: {e}\n{traceback.format_exc()}")
            failed += 1
            if mark_errors:
                csv_file.rename(csv_file.with_suffix(".error"))

    logger.info(f"📦 Migration summary: {migrated} migrated, {skipped} skipped, {failed} failed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", type=str, required=True)
    parser.add_argument("--output", "-o", type=str, default="parquet")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--migrate", action="store_true")
    parser.add_argument("--restore-copied", action="store_true")
    parser.add_argument("--restore-errors", action="store_true")
    parser.add_argument("--pairs", nargs='+', help="Asset pairs to include")
    parser.add_argument("--mark-errors", action="store_true")
    parser.add_argument("--delete-csv", action="store_true")
    parser.add_argument("--keep-csv", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    input_path.mkdir(parents=True, exist_ok=True)
    log_file = input_path / "pipeline.log"
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logger = setup_logging(log_file, log_level)

    if args.restore_copied:
        restore_copied_files(logger, input_path)

    if args.restore_errors:
        restore_error_files(logger, input_path)

    if args.download:
        download_recent_data(logger, output_path, args.pairs, keep_csv=args.keep_csv)

    if args.migrate:
        migrate_csv_archive(logger, input_path, output_path, mark_errors=args.mark_errors, delete_csv=args.delete_csv)

    logger.info("✅ Pipeline complete.")


if __name__ == "__main__":
    main()
