# helpers/unzip.py
from __future__ import annotations
from pathlib import Path
import zipfile

def extract_zip_to_dir(zip_path: Path, out_dir: Path, delete_zip: bool = False) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            target = out_dir / member.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as src, open(target, "wb") as dst:
                dst.write(src.read())
            extracted.append(target)
    if delete_zip:
        try:
            zip_path.unlink()
        except Exception:
            pass
    return extracted
