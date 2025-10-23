from __future__ import annotations
from pathlib import Path
import polars as pl

from paths import dirs

def convert_txt_csv() -> int:
    src_root = dirs["RAW"]
    out_root = dirs["CSV"]
    state_dir = dirs["STATE"]
    empty_log = state_dir / "EMPTY"

    candidates = [p for p in src_root.rglob("*") if p.is_file() and (p.suffix.lower() == ".txt" or p.suffix == "")]
    written = 0

    for src in candidates:
        # read pipe-delimited forgiving
        try:
            df = pl.read_csv(
                src,
                separator="|",
                has_header=True,
                ignore_errors=True,
                truncate_ragged_lines=True,
                encoding="utf8-lossy",
            )
        except Exception:
            # unreadable → log as empty and continue (don’t delete)
            rel = src.relative_to(src_root)
            empty_log.write_text("", encoding="utf-8") if not empty_log.exists() else None
            with empty_log.open("a", encoding="utf-8") as f:
                f.write(str(rel).replace("\\", "/") + "\n")
            continue

        if df.is_empty():
            rel = src.relative_to(src_root)
            empty_log.write_text("", encoding="utf-8") if not empty_log.exists() else None
            with empty_log.open("a", encoding="utf-8") as f:
                f.write(str(rel).replace("\\", "/") + "\n")
            continue

        # rename to c1..cN
        df = df.rename({c: f"c{i+1}" for i, c in enumerate(df.columns)})

        # ensure at least c1..c6
        if df.width < 6:
            pads = [pl.lit(None).alias(f"c{i}") for i in range(df.width + 1, 7)]
            df = df.with_columns(pads)

        # c7 rule
        if "c7" in df.columns:
            combo = (
                (pl.col("c5").cast(pl.Utf8).fill_null("") + "|" + pl.col("c6").cast(pl.Utf8).fill_null(""))
                .str.replace(r"^\|+|\|+$", "", literal=False)
            )
            df = (
                df.with_columns(
                    c5=pl.when(pl.col("c7").is_not_null()).then(combo).otherwise(pl.col("c5")),
                    c6=pl.when(pl.col("c7").is_not_null()).then(pl.col("c7")).otherwise(pl.col("c6")),
                )
                .drop("c7")
            )
            keep = [f"c{i}" for i in range(1, 7)] + [
                c for c in df.columns if c.startswith("c") and c[1:].isdigit() and int(c[1:]) >= 8
            ]
            df = df.select([c for c in keep if c in df.columns])

        # drop empty/null c2
        if "c2" in df.columns:
            df = df.filter(
                pl.col("c2").is_not_null()
                & (pl.col("c2").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0)
            )

        if df.is_empty():
            # log empty, do not delete source
            rel = src.relative_to(src_root)
            empty_log.write_text("", encoding="utf-8") if not empty_log.exists() else None
            with empty_log.open("a", encoding="utf-8") as f:
                f.write(str(rel).replace("\\", "/") + "\n")
            continue

        # write CSV then delete source
        out_path = out_root.with_suffix(".csv")
        try:
            df.write_csv(out_path)
            src.unlink()
            written += 1
        except Exception:
            # leave source in place if anything goes wrong
            pass

    return written

