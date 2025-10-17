# helpers/paths.py
from __future__ import annotations
from pathlib import Path

# Base: .../DataProcessing
BASE_DIR = Path(__file__).resolve().parents[1]

# Output root: DataProcessing/output/alternative
ALT_OUT = BASE_DIR / "output" / "alternative"
ALT_OUT.mkdir(parents=True, exist_ok=True)

# Subfolders
ZIP_DIR = ALT_OUT / "temp"   # zips land here
RAW_DIR = ZIP_DIR / "raw"    # raw CSV/TXT/extracted files land here
CSVS_DIR = ALT_OUT / "csvs"  # cleaned CSVs land here
SRC_DIR = ALT_OUT / "src"    # master.csv lives here
STATE_DIR = ZIP_DIR / "state"

for p in (ZIP_DIR, RAW_DIR, CSVS_DIR, SRC_DIR, STATE_DIR):
    p.mkdir(parents=True, exist_ok=True)

# JSON ledger of downloaded ZIPs
DOWNLOADED_JSON = STATE_DIR / "downloaded.json"

# SEC page we scrape for FTD ZIPs
SEC_INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"

# ---- New / convenience names ----
MASTER_CSV = SRC_DIR / "master.csv"
RAW_CSV_DIR = RAW_DIR            # alias used by cleaner
CLEANED_CSV_DIR = CSVS_DIR       # alias used by cleaner
LAST_RUN_JSON = DOWNLOADED_JSON.with_suffix(".last_run.json")
