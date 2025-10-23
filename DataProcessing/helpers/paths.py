from __future__ import annotations
from pathlib import Path
import os
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")

SEC_URL = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"

UA = "FailToDeliver Ingestion"

HDRS = {"User-Agent": UA, "Accept": "application/zip,text/plain,*/*;q=0.8", "Referer": SEC_URL}

MAX_WORKERS = 8

base = Path(__file__).resolve().parents[1]
dirs = {
    "BASE":  base,
    "OUT":   base / "output",
    "SRC":   base / "output" / "src",
    "TEMP":  base / "output" / "temp",
    "RAW":   base / "output" / "temp" / "raw",
    "CSV":   base / "output" / "temp" / "csv",
    "STATE": base / "output" / "temp" / "state",
}

for m in dirs.values():
    if m.exists() == False:
        m.mkdir(parents=True, exist_ok=True)
        logging.info(f"Found - {m} missing, Building now")        
logging.info("All Directories Found")    



