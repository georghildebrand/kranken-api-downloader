# kranken-api-downloader

simple scripts to download from kranke api and compress the output into a parquet

python csv_to_parquet.py -i test-data/ -o parquet-data/

###
kraken_pipeline.py
├─ 1. Check if ./<yyyy>/<mm> exists
|- 2. Download data into ./<yyyy>/<mm>/<yyyy-mm-dd-HH-MM>-<pair>.csv
├─ 2. Move all new .csv into ./<year>/<month>/
├─ 3. For each unprocessed .csv:
│    ├─ Append to parquet-data/<year>/<month>.parquet (gzip)
│    └─ Rename .csv → .csv.copied
     |_ Optional --delete-csv to delete copied files
└─ 4. Done ✅

Optional: restore copied files with -i <input path> --restore

Optional: python csv_to_parquet.py -i test-data/ -o parquet-data/  migrates existing archive into yyyy/mm/parquet

tries to be as memory savy as possible. especially when appending an existing parquet file its not blowing up the memory
