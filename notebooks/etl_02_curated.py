"""
Stage 2 of the pipeline: join the raw tables together, attach external ABS
data, apply the business rules from config.py, and write the curated dataset.

Nothing is deleted here. Rows that fail a business rule are *flagged*, not
dropped, and an `is_valid` column marks the rows that downstream ranking should
use. This matters for two reasons: the Sprint 2 checkpoint asks what happened
to the rows that were removed, and a merchant's fraud rate is only meaningful
if the denominator still contains the transactions we chose to exclude.

Run:  python scripts/etl_02_curated.py
"""

import json
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config
from spark_session import create_spark


def attach_external(spark, consumers):
    """
    Join postcode-level ABS attributes onto consumers.

    Runs only if the external files exist. The pipeline stays runnable on a
    fresh clone before anyone has downloaded ABS data - the columns come
    through as null and the ranking system degrades rather than crashing.
    """
    if not config.POSTCODE_SA2_FILE.exists():
        print("  external ABS data not found - skipping geo attachment.")
        print("  run `python scripts/download_external.py` to enable it.")
        return consumers.withColumn("sa2_code", F.lit(None).cast("string"))

    import pandas as pd

    import geo

    correspondence = geo.load_correspondence()

    frames = [geo.dominant_sa2(correspondence)[["postcode", "sa2_code", "sa2_name"]]]

    for path, columns in (
        (config.SA2_INCOME_FILE, ["median_income"]),
        (config.SA2_POPULATION_FILE, ["population"]),
    ):
        if path.exists():
            sa2_data = pd.read_csv(path)
            sa2_data["sa2_code"] = sa2_data["sa2_code"].astype(str)
            frames.append(
                geo.postcode_attributes(correspondence, sa2_data, columns)
            )
        else:
            print(f"  {path.name} missing - those columns will be null.")

    lookup = frames[0]
    for frame in frames[1:]:
        lookup = lookup.merge(frame, on="postcode", how="left")

    return consumers.join(
        spark.createDataFrame(lookup), on="postcode", how="left"
    )


def main():
    spark = create_spark("BNPL ETL - curated")
    spark.sparkContext.setLogLevel("ERROR")

    transactions = spark.read.parquet(str(config.RAW_DIR / "transactions"))
    merchants = spark.read.parquet(str(config.RAW_DIR / "merchants"))
    consumers = spark.read.parquet(str(config.RAW_DIR / "consumers"))

    quality = {}

    # --- Join to merchants ------------------------------------------------
    # LEFT join, not inner. A sizeable block of transactions references
    # merchant ABNs that do not appear in tbl_merchants, and an inner join
    # would silently delete them along with the evidence that they existed.
    joined = transactions.join(
        merchants, on="merchant_abn", how="left"
    ).withColumn("has_merchant_record", F.col("category").isNotNull())

    # --- Join to consumers ------------------------------------------------
    print("Attaching consumer and external data...")
    consumers = attach_external(spark, consumers)
    joined = joined.join(
        consumers.drop("name", "address"), on="user_id", how="left"
    ).withColumn("has_consumer_record", F.col("consumer_id").isNotNull())

    # --- Business rule: implausibly small transactions ---------------------
    joined = joined.withColumn(
        "below_min_value", F.col("dollar_value") < config.MIN_TRANSACTION_VALUE
    )

    # --- Business rule: per-category upper outliers -----------------------
    # A global dollar threshold would treat every large furniture purchase as
    # an outlier while letting an impossible florist transaction through, so
    # the threshold is computed within each merchant category.
    thresholds = (
        joined.filter(F.col("category").isNotNull())
        .groupBy("category")
        .agg(
            F.expr(
                f"percentile_approx(dollar_value, {config.CATEGORY_OUTLIER_QUANTILE}, "
                f"{config.CATEGORY_OUTLIER_ACCURACY})"
            ).alias("category_upper_threshold")
        )
    )
    joined = joined.join(thresholds, on="category", how="left").withColumn(
        "above_category_threshold",
        F.col("dollar_value") > F.col("category_upper_threshold"),
    )

    # --- Derived fields ---------------------------------------------------
    # The BNPL firm's revenue on a transaction, which is what the whole
    # ranking system is ultimately built on.
    take_rate_divisor = 100.0 if config.TAKE_RATE_IS_PERCENT else 1.0
    joined = (
        joined.withColumn(
            "bnpl_revenue", F.col("dollar_value") * F.col("take_rate") / take_rate_divisor
        )
        .withColumn("order_year_month", F.date_format("order_datetime", "yyyy-MM"))
        .withColumn(
            "is_valid",
            F.col("has_merchant_record")
            & F.col("has_consumer_record")
            & ~F.col("below_min_value")
            & ~F.coalesce(F.col("above_category_threshold"), F.lit(False)),
        )
    )

    joined = joined.cache()

    # --- Record what the rules did ----------------------------------------
    print("Computing data-quality summary...")
    totals = joined.agg(
        F.count("*").alias("n_transactions"),
        F.sum(F.col("dollar_value")).alias("total_dollars"),
        F.sum(F.when(~F.col("has_merchant_record"), 1).otherwise(0)).alias(
            "n_no_merchant_record"
        ),
        F.sum(F.when(~F.col("has_consumer_record"), 1).otherwise(0)).alias(
            "n_no_consumer_record"
        ),
        F.sum(F.when(F.col("below_min_value"), 1).otherwise(0)).alias("n_below_min"),
        F.sum(F.when(F.col("above_category_threshold"), 1).otherwise(0)).alias(
            "n_above_category_threshold"
        ),
        F.sum(F.when(F.col("is_valid"), 1).otherwise(0)).alias("n_valid"),
        F.sum(F.when(F.col("sa2_code").isNull(), 1).otherwise(0)).alias("n_no_sa2"),
    ).collect()[0]
    quality["transactions"] = totals.asDict()

    output = config.CURATED_DIR / "transactions"
    (
        joined.write.mode("overwrite")
        .partitionBy("order_datetime")
        .parquet(str(output))
    )

    with open(config.CURATED_DIR / "data_quality.json", "w") as handle:
        json.dump(quality, handle, indent=2, default=str)

    print(f"\nStage 2 complete. Curated data at {output}")
    for key, value in quality["transactions"].items():
        print(f"  {key}: {value}")

    spark.stop()


if __name__ == "__main__":
    main()
