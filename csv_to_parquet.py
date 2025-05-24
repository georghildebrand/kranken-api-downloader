import pandas as pd
from pathlib import Path
import re
import argparse
import sys

def restore_copied_files(input_path: Path):
    print(f"Restoring .csv.copied files under: {input_path}")
    count = 0
    for copied_file in input_path.rglob("*.csv.copied"):
        original = copied_file.with_name(copied_file.name[:-7])  # remove ".copied"
        copied_file.rename(original)
        print(f"Restored: {copied_file} → {original}")
        count += 1
    print(f"Done. {count} files restored.")
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="Convert CSVs to yearly Parquet files once, renaming after copy.")
    parser.add_argument("--input", "-i", required=True, help="CSV root folder (e.g. test-data/)")
    parser.add_argument("--output", "-o", default="parquet-data", help="Output folder for yearly Parquet files")
    parser.add_argument("--restore", action="store_true", help="Restore all .csv.copied files to .csv and exit")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    if args.restore:
        restore_copied_files(input_path)

    all_csv_files = list(input_path.rglob("*.csv"))
    print(f"Found {len(all_csv_files)} CSV files")

    for csv_file in all_csv_files:
        if csv_file.name.endswith(".copied"):
            continue

        copied_marker = csv_file.with_name(csv_file.name + ".copied")
        if copied_marker.exists():
            continue
        # Extract the year from file parent folder
        match = re.search(r"(\d{4})", str(csv_file.parent))
        if not match:
            print(f"Skipping (no year in path): {csv_file}")
            continue
        year = match.group(1)
        parquet_path = output_path / f"{year}.parquet"

        try:
            df = pd.read_csv(csv_file, low_memory=False)

            if parquet_path.exists():
                df_existing = pd.read_parquet(parquet_path)
                df_combined = pd.concat([df_existing, df], ignore_index=True)
            else:
                df_combined = df

            df_combined.to_parquet(parquet_path, index=False, compression="gzip")
            csv_file.rename(copied_marker)
            print(f"Appended and marked copied: {csv_file}")
        except Exception as e:
            print(f"Failed: {csv_file} → {e}")

if __name__ == "__main__":
    main()
