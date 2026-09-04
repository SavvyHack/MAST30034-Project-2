"""
Stage 1 of the pipeline: read every supplied file with an explicitly declared
schema and write a standardised copy to data/raw/.

Why enforce schemas rather than letting Spark infer them?
The transaction data is split across ~241 daily partition folders. Schema
inference reads a sample of those files, so a column that happens to be
integer-valued in the sampled days and decimal elsewhere can be inferred
inconsistently. Declaring the schema removes that class of bug and makes any
change in the source data fail loudly here instead of silently downstream.

This stage does no joining and no filtering. It is a faithful, typed copy of
the source. All business logic lives in stage 2.

Run:  python scripts/etl_01_raw.py
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config
from spark_session import create_spark
from tags import assign_segment, parse_tag

# --- Declared schemas -----------------------------------------------------

TRANSACTION_SCHEMA = StructType(
    [
        StructField("user_id", LongType(), nullable=False),
        StructField("merchant_abn", LongType(), nullable=False),
        StructField("dollar_value", DoubleType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
    ]
)

CONSUMER_SCHEMA = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("postcode", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("consumer_id", LongType(), nullable=False),
    ]
)

FRAUD_SCHEMA = StructType(
    [
        StructField("entity_id", LongType(), nullable=False),
        StructField("order_datetime", DateType(), nullable=False),
        StructField("fraud_probability", DoubleType(), nullable=False),
    ]
)


def load_transactions(spark):
    """
    Read every transaction snapshot folder into one dataframe.

    The snapshots are discovered by glob rather than named explicitly, so
    dropping a second snapshot into tables/ picks it up with no code change.
    A `snapshot` column is retained because the snapshots cover different date
    ranges and knowing which one a row came from matters when reconciling
    against the fraud labels.
    """
    snapshot_dirs = sorted(config.TABLES_DIR.glob(config.TRANSACTION_GLOB))
    if not snapshot_dirs:
        raise FileNotFoundError(
            f"No transaction snapshots matching '{config.TRANSACTION_GLOB}' "
            f"in {config.TABLES_DIR}. Download them from Canvas first."
        )

    frames = []
    for snapshot_dir in snapshot_dirs:
        df = (
            spark.read.schema(TRANSACTION_SCHEMA)
            .option("basePath", str(snapshot_dir))
            .parquet(str(snapshot_dir))
            # order_datetime is the hive partition key, so it is recovered from
            # the directory name rather than declared in the schema above.
            .withColumn("order_datetime", F.to_date(F.col("order_datetime")))
            .withColumn("snapshot", F.lit(snapshot_dir.name))
        )
        frames.append(df)
        print(f"  found snapshot: {snapshot_dir.name}")

    combined = frames[0]
    for df in frames[1:]:
        combined = combined.unionByName(df)
    return combined


MERCHANT_SCHEMA = StructType(
    [
        StructField("merchant_abn", LongType(), nullable=False),
        StructField("merchant_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("revenue_level", StringType(), nullable=True),
        StructField("take_rate", DoubleType(), nullable=True),
        StructField("segment", StringType(), nullable=True),
        StructField("tags_raw", StringType(), nullable=True),
    ]
)


def load_merchants(spark):
    """
    Read the merchant table and unpack the `tags` string into three typed
    columns, then attach the business segment.

    This table has ~4,000 rows, so it is parsed with pandas and handed to Spark
    as a small dataframe rather than run through a Spark UDF. Two reasons:
    a UDF here would cost more in Python worker startup than the parse itself,
    and keeping the logic in plain Python means scripts/tags.py stays directly
    unit-testable without a Spark session.
    """
    import pandas as pd

    merchants = pd.read_parquet(config.MERCHANTS_FILE).reset_index()
    parsed = merchants["tags"].map(parse_tag)

    merchants = pd.DataFrame(
        {
            "merchant_abn": merchants["merchant_abn"].astype("int64"),
            "merchant_name": merchants["name"].astype(str),
            "category": [p[0] for p in parsed],
            "revenue_level": [p[1] for p in parsed],
            "take_rate": [p[2] for p in parsed],
            "tags_raw": merchants["tags"].astype(str),
        }
    )
    merchants["segment"] = merchants["category"].map(assign_segment)

    n_failed = int(merchants["category"].isna().sum())
    if n_failed:
        print(f"  warning: {n_failed} merchant tags failed to parse")
    print(f"  parsed {len(merchants)} merchants into "
          f"{merchants['category'].nunique()} categories, "
          f"{merchants['segment'].nunique()} segments")

    return spark.createDataFrame(merchants[[f.name for f in MERCHANT_SCHEMA]],
                                 schema=MERCHANT_SCHEMA)


def load_consumers(spark):
    """
    Read the pipe-delimited consumer file.

    postcode is read as a string on purpose. Read as an integer, leading zeros
    are stripped and NT/ACT postcodes such as 0800 become 800, which then fail
    to match the ABS correspondence file.
    """
    return (
        spark.read.schema(CONSUMER_SCHEMA)
        .option("header", True)
        .option("sep", "|")
        .csv(str(config.CONSUMER_FILE))
        # Restore the leading zeros lost in the source file.
        .withColumn("postcode", F.lpad(F.col("postcode"), 4, "0"))
    )


def load_fraud(spark, path: Path, id_column: str):
    """Read a fraud-probability delta file and rename its id column."""
    return (
        spark.read.schema(FRAUD_SCHEMA)
        .option("header", True)
        .csv(str(path))
        .withColumnRenamed("entity_id", id_column)
        # Source stores 97.6 to mean 97.6%. Convert to a 0-1 probability.
        .withColumn("fraud_probability", F.col("fraud_probability") / 100.0)
    )


def main():
    spark = create_spark("BNPL ETL - raw")
    spark.sparkContext.setLogLevel("ERROR")

    print("Reading transactions...")
    transactions = load_transactions(spark)
    (
        transactions.write.mode("overwrite")
        .partitionBy("order_datetime")
        .parquet(str(config.RAW_DIR / "transactions"))
    )

    print("Reading merchants...")
    load_merchants(spark).write.mode("overwrite").parquet(
        str(config.RAW_DIR / "merchants")
    )

    print("Reading consumers...")
    consumers = load_consumers(spark)
    user_map = spark.read.parquet(str(config.CONSUMER_USER_FILE))
    # consumer_id is the key in tbl_consumer; user_id is the key in
    # transactions. Joining once here means no downstream script has to know
    # that these are two different identifiers for the same person.
    (
        consumers.join(user_map, on="consumer_id", how="inner")
        .write.mode("overwrite")
        .parquet(str(config.RAW_DIR / "consumers"))
    )

    print("Reading fraud deltas...")
    load_fraud(spark, config.CONSUMER_FRAUD_FILE, "user_id").write.mode(
        "overwrite"
    ).parquet(str(config.RAW_DIR / "consumer_fraud"))
    load_fraud(spark, config.MERCHANT_FRAUD_FILE, "merchant_abn").write.mode(
        "overwrite"
    ).parquet(str(config.RAW_DIR / "merchant_fraud"))

    print(f"\nStage 1 complete. Output written to {config.RAW_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
