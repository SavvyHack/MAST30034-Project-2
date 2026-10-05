"""Time-aware inputs for the BNPL notebook validation exercises."""
from functools import reduce
from pathlib import Path
from pyspark.sql import functions as F, types as T

def valid_raw_transactions(spark, tables="tables"):
    """Known merchants and valid amounts; retain unusual purchases for fraud analysis."""
    paths = sorted(Path(tables).glob("transactions_*_snapshot"))
    if not paths:
        raise FileNotFoundError(f"No transaction snapshots in {tables}")
    schema = "user_id long, merchant_abn long, dollar_value double, order_id string, order_datetime date"
    raw = reduce(lambda a, b: a.unionByName(b),
                 [spark.read.schema(schema).parquet(str(p)) for p in paths])
    merchants = (spark.read.parquet(str(Path(tables) / "tbl_merchants.parquet"))
                 .select("merchant_abn", F.regexp_extract(F.lower("tags"),
                         r"take rate:\s*([0-9.]+)", 1).cast("double").alias("take_rate")))
    assert merchants.select("merchant_abn").distinct().count() == merchants.count()
    assert merchants.filter(F.col("take_rate").isNull()).count() == 0
    return (raw.join(F.broadcast(merchants), "merchant_abn", "inner")
            .filter(F.col("dollar_value").isNotNull() & ~F.isnan("dollar_value")
                    & (F.col("dollar_value") >= 0.01) & (F.col("dollar_value") < float("inf"))))

def apply_past_fences(transactions, cutoff):
    """Fit 3-IQR fences through cutoff; keep valid rows for merchants without history."""
    fences = (transactions.filter(F.col("order_datetime") <= cutoff)
              .groupBy("merchant_abn")
              .agg(F.percentile("dollar_value", [0.25, 0.75]).alias("q"))
              .select("merchant_abn", (F.col("q")[0] - 3 * (F.col("q")[1] - F.col("q")[0])).alias("lo"),
                      (F.col("q")[1] + 3 * (F.col("q")[1] - F.col("q")[0])).alias("hi")))
    return (transactions.join(fences, "merchant_abn", "left")
            .filter(F.col("lo").isNull() | F.col("dollar_value").between(F.col("lo"), F.col("hi")))
            .drop("lo", "hi"))

def labelled_fraud(spark, transactions, tables="tables"):
    """Attach supplied probability scores; unlisted days use the stated zero proxy."""
    out = transactions
    for name, key in [("consumer", "user_id"), ("merchant", "merchant_abn")]:
        schema = T.StructType([T.StructField(key, T.LongType()),
                               T.StructField("order_datetime", T.DateType()),
                               T.StructField("fraud_probability", T.DoubleType())])
        labels = (spark.read.csv(str(Path(tables) / f"{name}_fraud_probability.csv"),
                                header=True, schema=schema, mode="FAILFAST").dropDuplicates())
        assert labels.count() == labels.select(key, "order_datetime").distinct().count()
        out = out.join(F.broadcast(labels.withColumnRenamed("fraud_probability", f"{name}_p")),
                       [key, "order_datetime"], "left")
    cp = F.coalesce(F.col("consumer_p") / 100, F.lit(0.0))
    mp = F.coalesce(F.col("merchant_p") / 100, F.lit(0.0))
    return out.withColumn("label_probability", 1 - (1 - cp) * (1 - mp))
