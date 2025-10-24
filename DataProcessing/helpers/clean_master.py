from pathlib import Path
import polars as pl

from paths import dirs, COLS

INP = Path(dirs["SRC"]) / "master.csv"
OUT = Path(dirs["SRC"]) / "mc.csv"

df = pl.read_csv(INP)

df = df.filter(pl.col("SETTLEMENT DATE") != "SETTLEMENT DATE")

df = df.with_columns(
    # keep as Date for proper sorting
    pl.col("SETTLEMENT DATE")
      .str.strip_chars()
      .str.strptime(pl.Date, format="%Y%m%d", strict=False)
      .alias("SETTLEMENT DATE"),
    pl.col("QUANTITY (FAILS)")
      .str.replace_all(r"[^\d-]", "")
      .cast(pl.Int64, strict=False)
      .alias("QUANTITY (FAILS)"),
    pl.col("PRICE")
      .str.replace_all(r"[^\d\.\-]", "")
      .cast(pl.Float64, strict=False)
      .alias("PRICE"),
)

df = df.with_columns(
    (pl.col("QUANTITY (FAILS)").cast(pl.Float64) * pl.col("PRICE")).alias("weight")
)

# sort by date
df = df.sort("SETTLEMENT DATE")

# write with date formatted as YYYY-mm-dd
df = df.with_columns(pl.col("SETTLEMENT DATE").dt.strftime("%Y-%m-%d"))
df.select(COLS + ["weight"]).write_csv(OUT)

print(f"rows: {df.height}, wrote: {OUT}")
 
