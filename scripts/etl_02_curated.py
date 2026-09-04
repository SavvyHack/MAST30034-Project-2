"""
Stage 2 of the pipeline: join the raw tables together, attach external ABS
data and the fraud labels, apply the business rules from config.py, and write
the curated dataset.

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

    Note that the null columns are created explicitly with the right types
    rather than simply omitted, so that stage 3 sees a consistent schema
    either way and does not silently drop demographic features.
    """
    empty = (
        consumers.withColumn("sa2_code", F.lit(None).cast("string"))
        .withColumn("sa2_name", F.lit(None).cast("string"))
        .withColumn("median_income", F.lit(None).cast("double"))
        .withColumn("population", F.lit(None).cast("double"))
    )

    if not config.POSTCODE_SA2_FILE.exists():
        print("  external ABS data not found - skipping geo attachment.")
        print("  run `python scripts/download_external.py` to enable it.")
        return empty

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

    # Any column the manual downloads did not supply is added as a typed null
    # so the schema matches the no-external-data branch above.
    for column in ("median_income", "population"):
        if column not in lookup.columns:
            lookup[column] = pd.NA
        lookup[column] = pd.to_numeric(lookup[column], errors="coerce")

    return consumers.join(
        spark.createDataFrame(lookup), on="postcode", how="left"
    )


def attach_fraud(spark, joined):
    """
    Attach the fraud probability labels and derive the fields the ranking
    system needs.

    Three things are worth stating explicitly, because each is a decision
    rather than a mechanical step.

    1. The labels only cover 2021-02-28 to 2022-02-27, while the transactions
       run to 2022-10-26. A transaction outside that window is UNLABELLED, not
       fraud-free. `in_fraud_label_window` marks the difference so that stage 3
       can compute fraud rates against the labelled denominator only. Without
       it, a merchant who trades mostly in 2022 would look clean purely
       because nobody looked.

    2. The labels are probabilities, not confirmed fraud. `is_fraud` applies
       config.FRAUD_PROBABILITY_THRESHOLD and is a judgement we are making, not
       a fact in the data. `expected_fraud_value` keeps the full probability
       and is what the risk pillar actually uses.

    3. The joins are LEFT and keyed on (id, date). The consumer key had exact
       duplicates in the source file; those are removed in stage 1. Rather
       than counting the 14M-row join before and after - two full passes over
       the whole dataset, which is the single most expensive thing this stage
       could do - uniqueness is checked on the fraud tables themselves. They
       have 34,864 and 114 rows, and a left join cannot add rows if the right
       side's key is unique.
    """
    consumer_fraud = spark.read.parquet(str(config.RAW_DIR / "consumer_fraud"))
    merchant_fraud = spark.read.parquet(str(config.RAW_DIR / "merchant_fraud"))

    for name, frame, key in (
        ("consumer_fraud", consumer_fraud, "user_id"),
        ("merchant_fraud", merchant_fraud, "merchant_abn"),
    ):
        n_rows = frame.count()
        n_keys = frame.select(key, "order_datetime").distinct().count()
        if n_rows != n_keys:
            raise ValueError(
                f"{name} has {n_rows - n_keys} duplicate ({key}, order_datetime) "
                "keys. Joining it would fan out the transaction table and "
                "double-count dollar value. Fix the dedupe in etl_01_raw.py."
            )

    joined = (
        joined.join(
            consumer_fraud.withColumnRenamed(
                "fraud_probability", "consumer_fraud_prob"
            ),
            on=["user_id", "order_datetime"],
            how="left",
        ).join(
            merchant_fraud.withColumnRenamed(
                "fraud_probability", "merchant_fraud_prob"
            ),
            on=["merchant_abn", "order_datetime"],
            how="left",
        )
    )

    return (
        joined.withColumn(
            "in_fraud_label_window",
            F.col("order_datetime").between(
                F.lit(config.FRAUD_LABEL_START).cast("date"),
                F.lit(config.FRAUD_LABEL_END).cast("date"),
            ),
        )
        # The higher of the two signals. A transaction is suspicious if either
        # party to it is, and taking the max avoids inventing a combination
        # rule the data does not support.
        .withColumn(
            "fraud_probability",
            F.greatest(
                F.coalesce(F.col("consumer_fraud_prob"), F.lit(0.0)),
                F.coalesce(F.col("merchant_fraud_prob"), F.lit(0.0)),
            ),
        )
        .withColumn(
            "is_fraud",
            F.col("in_fraud_label_window")
            & (F.col("fraud_probability") > config.FRAUD_PROBABILITY_THRESHOLD),
        )
        # Expected dollars lost on this transaction. This is the quantity a
        # partnerships manager can actually price, and unlike the boolean it
        # uses the whole probability distribution rather than only the 2% of
        # labels that clear the threshold.
        .withColumn(
            "expected_fraud_value",
            F.when(
                F.col("in_fraud_label_window"),
                F.col("dollar_value") * F.col("fraud_probability"),
            ).otherwise(F.lit(None).cast("double")),
        )
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

    # --- Join to fraud labels ---------------------------------------------
    print("Attaching fraud labels...")
    joined = attach_fraud(spark, joined)

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
    ).cache()

    # percentile_approx is partition-dependent, so the exact thresholds used
    # are persisted. Anyone reproducing a run can read the numbers actually
    # applied rather than recomputing slightly different ones.
    thresholds.write.mode("overwrite").parquet(
        str(config.CURATED_DIR / "category_thresholds")
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

    # --- Write, then summarise from the written file ----------------------
    # The obvious thing here is joined.cache() followed by the aggregation and
    # then the write. Don't. Caching a 14M-row, ~30-column frame needs several
    # GB of driver memory and gets the process OOM-killed on a laptop, with no
    # traceback - it just stops.
    #
    # Writing first and reading back costs one extra pass, but parquet is
    # columnar so the summary below touches only the dozen boolean columns it
    # actually needs. That is far cheaper than holding every column in memory,
    # and it means the numbers reported are read from the artefact that was
    # actually written rather than from a separate in-memory copy of it.
    output = config.CURATED_DIR / "transactions"
    (
        joined.write.mode("overwrite")
        .partitionBy("order_datetime")
        .parquet(str(output))
    )

    print("Computing data-quality summary...")
    written = spark.read.parquet(str(output))
    totals = written.agg(
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
        F.sum(F.when(F.col("in_fraud_label_window"), 1).otherwise(0)).alias(
            "n_in_fraud_window"
        ),
        F.sum(F.when(F.col("consumer_fraud_prob").isNotNull(), 1).otherwise(0)).alias(
            "n_consumer_fraud_labelled"
        ),
        F.sum(F.when(F.col("merchant_fraud_prob").isNotNull(), 1).otherwise(0)).alias(
            "n_merchant_fraud_labelled"
        ),
        F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)).alias("n_flagged_fraud"),
    ).collect()[0]
    quality["transactions"] = totals.asDict()

    with open(config.CURATED_DIR / "data_quality.json", "w") as handle:
        json.dump(quality, handle, indent=2, default=str)

    print(f"\nStage 2 complete. Curated data at {output}")
    for key, value in quality["transactions"].items():
        print(f"  {key}: {value}")

    spark.stop()


if __name__ == "__main__":
    main()
