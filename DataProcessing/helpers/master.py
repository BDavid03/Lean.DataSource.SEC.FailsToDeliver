from pathlib import Path
import csv
from paths import dirs  # dirs["CSV"], dirs["SRC"]

HEADERS = [
    "SETTLEMENT DATE",
    "CUSIP",
    "SYMBOL",
    "QUANTITY (FAILS)",
    "DESCRIPTION",
    "PRICE",
]

def build_master() -> int:
    csv_dir = Path(dirs["CSV"])
    out_path = Path(dirs["SRC"]) / "master.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(p for p in csv_dir.rglob("*.csv") if p.is_file())
    if not files:
        out_path.write_text(",".join(HEADERS) + "\n", encoding="utf-8")
        return 0

    rows_written = 0
    with out_path.open("w", newline="", encoding="utf-8") as out_f:
        w = csv.writer(out_f)
        w.writerow(HEADERS)

        for f in files:
            with f.open("r", newline="", encoding="utf-8", errors="ignore") as in_f:
                r = csv.reader(in_f)
                for row in r:
                    if not row:
                        continue
                    # normalise to exactly 6 fields
                    if len(row) < 6:
                        row = row + [""] * (6 - len(row))
                    elif len(row) > 6:
                        row = row[:6]
                    w.writerow(row)
                    rows_written += 1

    return rows_written
