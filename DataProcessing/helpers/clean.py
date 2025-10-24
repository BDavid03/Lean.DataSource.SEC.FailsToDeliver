from pathlib import Path
import shutil
from paths import dirs  # dirs["RAW"], dirs["CSV"]

def convert_to_csv() -> int:
    raw_dir = Path(dirs["RAW"])
    csv_dir = Path(dirs["CSV"])
    csv_dir.mkdir(parents=True, exist_ok=True)

    files = [p for p in raw_dir.rglob("*") if p.is_file()]
    written = 0

    for src in files:
        rel = src.relative_to(raw_dir)
        dst = (csv_dir / rel).with_suffix(".csv")
        dst.parent.mkdir(parents=True, exist_ok=True)

        if src.suffix.lower() == ".csv":
            shutil.copy2(src, dst)
            src.unlink(missing_ok=True)
            written += 1
            continue

        # Raw text pass-through; standardise delimiters only
        text = src.read_text(encoding="utf-8", errors="ignore")
        text = text.replace("\t", ",").replace("|", ",")
        dst.write_text(text, encoding="utf-8")
        src.unlink(missing_ok=True)
        written += 1

    return written
