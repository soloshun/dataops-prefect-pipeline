from pathlib import Path

# Resolve project root relative to this file
# src/dataops_prefect_pipeline/utils/config.py -> .../dataops-prefect-pipeline
PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_DIR / "raw"
CACHE_PATH = RAW_DATA_PATH / ".cache"
PROCESSED_DATA_PATH = DATA_DIR / "processed"

# Ensure directories exist
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
CACHE_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)