"""
Stage 3: collapse the curated transaction table into one row per merchant.

This is the table the ranking system consumes. Every feature here was chosen
because it maps onto a question the BNPL firm would actually ask about a
prospective partner, and each is commented with that question.

Only rows with `is_valid` are aggregated, with two deliberate exceptions:
`n_transactions_excluded` counts what the business rules removed, so a merchant
whose volume is mostly implausible transactions can be spotted rather than
quietly ranked on a shrunken but clean-looking record; and the fraud
aggregates are computed over the labelled window only, for the reason set out
in etl_02_curated.attach_fraud.

Run:  python scripts/etl_03_features.py
"""

import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config
from spark_session import create_spark


def build_features(transactions, window_end):
    """
    Aggregate to merchant level.

    `window_end` is the last date in the dataset. It is passed in rather than
    recomputed here because it is needed to annualise revenue correctly and
    should be the same value everywhere it is used.
    """
    valid = transactions.filter(F.col("is_valid"))

    # The ABS-derived columns only exist once external data has been
    # downloaded. Building the aggregation list conditionally means the feature
    # table can still be produced on a fresh clone - it simply carries fewer
    # demographic features - rather than failing on a missing column.
    optional_aggs = []
    if "median_income" in transactions.columns:
        # Spending power of the merchant's customer base.
        optional_aggs.append(F.avg("median_income").alias("mean_customer_income"))
        optional_aggs.append(F.avg("population").alias("mean_customer_sa2_population"))
    else:
        print("  median_income not present - skipping demographic features.")

    # --- Core volume and value -------------------------------------------
    # "How much money does this merchant put through us, and how much of that
    #  do we keep?"
    base = valid.groupBy("merchant_abn").agg(
        F.first("merchant_name").alias("merchant_name"),
        F.first("category").alias("category"),
        F.first("segment").alias("segment"),
        F.first("revenue_level").alias("revenue_level"),
        F.first("take_rate").alias("take_rate"),
        F.count("*").alias("n_transactions"),
        F.countDistinct("user_id").alias("n_customers"),
        F.sum("dollar_value").alias("total_dollar_value"),
        F.sum("bnpl_revenue").alias("total_bnpl_revenue"),
        F.avg("dollar_value").alias("mean_basket"),
        F.expr("percentile_approx(dollar_value, 0.5)").alias("median_basket"),
        F.stddev("dollar_value").alias("std_basket"),
        F.min("order_datetime").alias("first_transaction"),
        F.max("order_datetime").alias("last_transaction"),
        F.countDistinct("order_datetime").alias("n_active_days"),
        # "Is this merchant national or stuck in one suburb?" A merchant whose
        # customers span many regions is less exposed to a local shock.
        F.countDistinct("state").alias("n_states"),
        F.countDistinct("postcode").alias("n_postcodes"),
        *optional_aggs,
    )

    # --- What the business rules threw away -------------------------------
    excluded = (
        transactions.filter(~F.col("is_valid"))
        .groupBy("merchant_abn")
        .agg(
            F.count("*").alias("n_transactions_excluded"),
            F.sum("dollar_value").alias("dollars_excluded"),
        )
    )

    # --- Fraud -------------------------------------------------------------
    # "How much of this merchant's volume looks fraudulent, and how much money
    #  would that cost us?"
    #
    # The denominator is the LABELLED window, not the whole history. A merchant
    # who did most of their trading after 2022-02-27 has little or no labelled
    # volume, and the correct answer for them is null - we do not know - rather
    # than zero. The percentile() helper in stage 4 scores nulls at the median
    # rather than as a clean record, so an unlabelled merchant is neither
    # rewarded nor punished for the gap in our data.
    labelled = transactions.filter(
        F.col("is_valid") & F.col("in_fraud_label_window")
    )
    fraud = labelled.groupBy("merchant_abn").agg(
        F.count("*").alias("n_transactions_labelled"),
        F.sum("dollar_value").alias("dollars_labelled"),
        F.sum(F.col("is_fraud").cast("int")).alias("n_flagged_fraud"),
        F.sum("expected_fraud_value").alias("expected_fraud_dollars"),
        F.max("fraud_probability").alias("max_fraud_probability"),
    )
    fraud = (
        fraud.withColumn(
            # Share of labelled transactions over the threshold.
            "fraud_rate",
            F.col("n_flagged_fraud") / F.nullif(F.col("n_transactions_labelled"), F.lit(0)),
        ).withColumn(
            # Probability-weighted share of labelled dollars at risk. This is
            # the feature the risk pillar uses: it degrades smoothly instead of
            # depending on where the threshold happens to sit.
            "expected_fraud_share",
            F.col("expected_fraud_dollars") / F.nullif(F.col("dollars_labelled"), F.lit(0)),
        )
    )

    # --- Repeat business ---------------------------------------------------
    # "Do customers come back?" A merchant with 1,000 one-time customers is a
    # worse partner than one with 300 customers who each return four times,
    # because repeat purchases are the thing the take rate compounds on.
    per_customer = valid.groupBy("merchant_abn", "user_id").agg(
        F.count("*").alias("customer_txns"),
        F.sum("bnpl_revenue").alias("customer_revenue"),
    )
    repeat = per_customer.groupBy("merchant_abn").agg(
        F.avg("customer_txns").alias("mean_txns_per_customer"),
        (
            F.sum(F.when(F.col("customer_txns") > 1, 1).otherwise(0)) / F.count("*")
        ).alias("repeat_customer_rate"),
    )

    # --- Revenue concentration --------------------------------------------
    # "If we lost their biggest customer, how much of this revenue evaporates?"
    # Herfindahl index over customer revenue shares: near 0 means revenue is
    # spread widely, near 1 means it rests on very few people.
    customer_window = Window.partitionBy("merchant_abn")
    concentration = (
        per_customer.withColumn(
            "revenue_share",
            F.col("customer_revenue") / F.sum("customer_revenue").over(customer_window),
        )
        .groupBy("merchant_abn")
        .agg(F.sum(F.pow(F.col("revenue_share"), 2)).alias("customer_hhi"))
    )

    # --- Growth and stability ---------------------------------------------
    # "Is this merchant on the way up or the way down?" Monthly revenue is
    # regressed on a month index; the slope is normalised by mean monthly
    # revenue so that a large and a small merchant growing at the same rate
    # score the same.
    monthly = valid.groupBy("merchant_abn", "order_year_month").agg(
        F.sum("bnpl_revenue").alias("monthly_revenue")
    )
    month_index = Window.partitionBy("merchant_abn").orderBy("order_year_month")
    monthly = monthly.withColumn("t", F.row_number().over(month_index))

    growth = monthly.groupBy("merchant_abn").agg(
        F.count("*").alias("n_months_active"),
        F.avg("monthly_revenue").alias("mean_monthly_revenue"),
        F.stddev("monthly_revenue").alias("std_monthly_revenue"),
        # Least-squares slope, computed from the aggregate sums rather than
        # collecting each merchant's series to the driver.
        (
            (
                F.count("*") * F.sum(F.col("t") * F.col("monthly_revenue"))
                - F.sum("t") * F.sum("monthly_revenue")
            )
            / F.nullif(
                F.count("*") * F.sum(F.pow(F.col("t"), 2)) - F.pow(F.sum("t"), 2),
                F.lit(0),
            )
        ).alias("revenue_slope"),
    ).withColumn(
        "revenue_growth_rate",
        F.col("revenue_slope") / F.nullif(F.col("mean_monthly_revenue"), F.lit(0)),
    ).withColumn(
        # "How predictable is this revenue?" High variance is a risk even when
        # the average is good.
        "revenue_volatility",
        F.col("std_monthly_revenue") / F.nullif(F.col("mean_monthly_revenue"), F.lit(0)),
    )

    features = (
        base.join(excluded, "merchant_abn", "left")
        .join(fraud, "merchant_abn", "left")
        .join(repeat, "merchant_abn", "left")
        .join(concentration, "merchant_abn", "left")
        .join(growth.drop("mean_monthly_revenue"), "merchant_abn", "left")
        .fillna({"n_transactions_excluded": 0, "dollars_excluded": 0.0})
    )

    # --- Exposure ----------------------------------------------------------
    # `months_observed` is elapsed months from a merchant's first transaction
    # to the end of the data window - NOT the number of months in which they
    # happened to trade.
    #
    # This distinction is the difference between a sane projection and a
    # nonsense one. Dividing by months *traded* means a merchant with a single
    # transaction has a divisor of 1, so that one purchase is treated as a
    # full month's revenue and multiplied by twelve. That produced $17,000
    # annual revenue projections from single purchases in the previous version
    # and put them at the top of the watchlist.
    features = features.withColumn(
        "months_observed",
        F.greatest(
            F.lit(1.0),
            F.months_between(F.lit(window_end).cast("date"), F.col("first_transaction")),
        ),
    )

    # --- Confidence flag ---------------------------------------------------
    # Merchants with very little history are not ranked alongside established
    # ones. The spec calls these out explicitly as an interesting group, so
    # they are separated rather than dropped.
    features = features.withColumn(
        "sufficient_history",
        F.col("n_transactions") >= config.MIN_TRANSACTIONS_FOR_RANKING,
    ).withColumn(
        "tenure_days",
        F.datediff(F.col("last_transaction"), F.col("first_transaction")) + 1,
    ).withColumn(
        # "How often are they actually trading?" Distinguishes a steady daily
        # business from one with a couple of enormous spikes.
        "transaction_density",
        F.col("n_active_days") / F.nullif(F.col("tenure_days"), F.lit(0)),
    )

    return features


def main():
    spark = create_spark("BNPL ETL - features")
    spark.sparkContext.setLogLevel("ERROR")

    transactions = spark.read.parquet(str(config.CURATED_DIR / "transactions"))

    window_end = transactions.agg(F.max("order_datetime")).collect()[0][0]
    print(f"  data window ends {window_end}")

    features = build_features(transactions, window_end)

    output = config.CURATED_DIR / "merchant_features"
    features.write.mode("overwrite").parquet(str(output))

    # Read the count back off the written file rather than calling .count() on
    # the un-cached `features` dataframe. Doing the latter re-executes the
    # entire DAG - every join, both window functions, the per-customer
    # aggregation - a second time after the write has already finished, which
    # roughly doubled the runtime of this stage.
    n_merchants = spark.read.parquet(str(output)).count()
    print(f"Stage 3 complete. {n_merchants} merchants written to {output}")

    spark.stop()


if __name__ == "__main__":
    main()
