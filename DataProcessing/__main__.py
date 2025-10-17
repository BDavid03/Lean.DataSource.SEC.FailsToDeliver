# DataProcessing/__main__.py
from __future__ import annotations
from pathlib import Path

from helpers.paths import (
    ZIP_DIR,
    RAW_CSV_DIR,        # raw TXT/CSV land here after unzip
    CLEANED_CSV_DIR,    # cleaned CSVs output
    MASTER_CSV,         # final merged master CSV
    DOWNLOADED_JSON,
)
from helpers.extract import extract_new_zips
from helpers.unzip import extract_zip_to_dir
from helpers.clean import clean_tree, build_master

LAST_RUN_JSON = DOWNLOADED_JSON.with_suffix(".last_run.json")

def main():
    print(">> Extract: checking SEC for new ZIPs…")
    new_zip_paths, new_count = extract_new_zips()

    if new_count > 0:
        print(f">> Extract: {new_count} new ZIP(s) saved.")
    else:
        print(">> Extract: no new ZIPs; continuing…")

    # Unzip if we actually downloaded any this run
    if new_zip_paths:
        print(">> Unzip: extracting newly-downloaded ZIPs…")
        extracted_files = 0
        for zp in new_zip_paths:
            extracted_files += len(extract_zip_to_dir(zp, RAW_CSV_DIR))
        print(f">> Unzip: extracted {extracted_files} files from {len(new_zip_paths)} ZIP(s).")
    else:
        print(">> Unzip: no ZIPs to extract; continuing to cleaning…")

    # Always clean + build master (even if nothing new was downloaded)
    print(">> Clean: normalizing & fixing raw files…")
    cleaned_files = clean_tree(RAW_CSV_DIR, CLEANED_CSV_DIR)
    print(f">> Clean: wrote {len(cleaned_files)} cleaned CSV(s).")

    print(">> Master: building master CSV…")
    master_rows = build_master(CLEANED_CSV_DIR, MASTER_CSV)
    print(f">> Master: {master_rows} rows written to: {MASTER_CSV}")

    print("Done.")


if __name__ == "__main__":
    main()
