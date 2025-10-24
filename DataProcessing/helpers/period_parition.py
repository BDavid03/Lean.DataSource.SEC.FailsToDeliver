# periods.py
from pathlib import Path
from datetime import date
import polars as pl
from paths import dirs  # dirs["SRC"]

SRC = Path(dirs["SRC"])
INP = SRC / "mc.csv"
OUT_FULL = SRC / "master.periods.csv"
OUT_TOTAL = SRC / "master.periods_weight.csv"

# load + types
df = pl.read_csv(INP, infer_schema_length=0).with_columns(
    pl.col("SETTLEMENT DATE").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
    pl.col("QUANTITY (FAILS)").cast(pl.Int64, strict=False).alias("qty"),
    pl.col("PRICE").cast(pl.Float64, strict=False).alias("price"),
)

# ensure weight exists
df = df.with_columns((pl.col("qty").cast(pl.Float64) * pl.col("price")).alias("weight"))

cutover = date(2009, 7, 1)

# helpers for period boundaries (version-safe)
first_of_month = pl.col("date").dt.replace(day=1)
mid_month = pl.col("date").dt.replace(day=15)
sixteenth = pl.col("date").dt.replace(day=16)
end_of_month = first_of_month.dt.offset_by("1mo").dt.offset_by("-1d")

# period metadata
df = df.with_columns(
    pl.when(pl.col("date") < cutover)
      .then(pl.lit("monthly_pre_2009_07"))
      .otherwise(pl.lit("semi_monthly_post_2009_07"))
      .alias("scheme"),

    pl.when(pl.col("date") < cutover)
      .then(pl.col("date").dt.strftime("%Y-%m"))
      .otherwise(
          pl.when(pl.col("date").dt.day() <= 15)
            .then(pl.col("date").dt.strftime("%Y-%m") + "-H1")
            .otherwise(pl.col("date").dt.strftime("%Y-%m") + "-H2")
      )
      .alias("period"),

    pl.when(pl.col("date") < cutover)
      .then(first_of_month)
      .otherwise(
          pl.when(pl.col("date").dt.day() <= 15).then(first_of_month).otherwise(sixteenth)
      )
      .alias("period_start"),

    pl.when(pl.col("date") < cutover)
      .then(end_of_month)
      .otherwise(
          pl.when(pl.col("date").dt.day() <= 15).then(mid_month).otherwise(end_of_month)
      )
      .alias("period_end"),
)

# release date estimate
df = df.with_columns(
    pl.when(pl.col("scheme") == "semi_monthly_post_2009_07")
      .then(
        pl.when(pl.col("date").dt.day() <= 15)
          .then(end_of_month)                                  # H1 -> end of month
          .otherwise(first_of_month.dt.offset_by("1mo").dt.replace(day=15))  # H2 -> 15th next month
      )
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("release_date_est")
)

# aggregate (use .sum())
full = (
    df.group_by(["scheme", "period", "period_start", "period_end", "release_date_est"])
      .agg(
          pl.len().alias("rows"),
          pl.col("qty").sum().alias("sum_qty"),
          pl.col("price").mean().alias("mean_price"),
          pl.col("weight").sum().alias("sum_weight"),
      )
      .sort("period_start")
)

full.write_csv(OUT_FULL)

totals = full.select(["period", "period_start", "period_end", "sum_weight"]).sort("period_start")
totals.write_csv(OUT_TOTAL)

print(f"wrote {OUT_FULL} ({full.height} periods)")
print(f"wrote {OUT_TOTAL} ({totals.height} totals)")
