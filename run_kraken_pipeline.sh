#!/bin/bash

# Exit on error
set -e

# Define paths
VENV_PATH="<venv path here>"
PROJECT_DIR="<path to this project dir"
PYTHON_SCRIPT="kraken-pipeline.py"

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Move to project directory
cd "$PROJECT_DIR" || exit 1

# Run pipeline: download, process, log everything
python "$PYTHON_SCRIPT" \
    --input "$PROJECT_DIR" \
    --output "$PROJECT_DIR/parquet-data" \
    --download \
    --log-level INFO
    --delete

# Done
echo "✅ Kraken pipeline finished successfully at $(date)"
