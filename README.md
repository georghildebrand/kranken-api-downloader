# Kraken Pipeline

This project provides a modular, command-line pipeline for downloading, processing, and archiving historical OHLC (Open-High-Low-Close) market data from the [Kraken](https://www.kraken.com/) cryptocurrency exchange.

The pipeline supports:

* Scheduled downloads of OHLC data in CSV format
* Efficient appending to Parquet files, organized by year/month
* Optional deletion of processed CSVs
* Migration of existing CSV archives
* Restore functionality for previously archived files
* Selective pair downloads and customizable logging

---

## Requirements

* Python 3.7+
* `pandas`, `requests`, `pyarrow`

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
python kraken-pipeline.py -i <input_folder> -o <output_folder> [options]
```

### Arguments

| Flag           | Description                                                            |
| -------------- | ---------------------------------------------------------------------- |
| `-i, --input`  | **Required.** Path to input folder where CSVs are stored or downloaded |
| `-o, --output` | Folder where Parquet files will be stored (default: `parquet-data`)    |
| `--download`   | Trigger fresh Kraken OHLC downloads                                    |
| `--migrate`    | Migrate existing CSV archive into Parquet format                       |
| `--delete-csv` | Delete `.csv.copied` files after successful conversion                 |
| `--restore`    | Restore `.csv.copied` files back to `.csv`                             |
| `--pairs`      | Optional. List of Kraken trading pairs to download (e.g. `XETHZEUR`)   |
| `--log-level`  | Set logging verbosity: DEBUG, INFO, WARNING, ERROR (default: INFO)     |

### Example

```bash
python kraken-pipeline.py \
  -i ./data \
  -o ./parquet \
  --download \
  --migrate \
  --delete-csv \
  --pairs XETHZEUR XXBTZUSD \
  --log-level DEBUG
```

---

## File Behavior

* Downloads are saved as: `YYYY/MM/YYYY-MM-DD-HH-MM-<PAIR>.csv`
* Processed CSVs are renamed: `*.csv` -> `*.csv.copied`
* Parquet files are written to: `<output>/<year>/<month>/<year>-<month>.parquet`
* Logs are appended to: `<input>/pipeline.log`

---

## Tips

* Add to `cron` for scheduled downloads (e.g., every 12 hours)
* Use `--migrate` once on existing folders
* Use `--restore` if you want to reprocess `.csv.copied`
* Use `--mark-errors` if you want corrupted csv files to be marked as `.csv.error`

---

## License

MIT License

---

## Author

Developed by ghildebrand, 2025.

Pull requests welcome!
