"""
A single place to build the Spark session so that every script and notebook
gets identical configuration. Importing this rather than calling
SparkSession.builder in each notebook is what makes the pipeline reproducible.
"""

from pyspark.sql import SparkSession


def create_spark(app_name: str = "MAST30034 BNPL", driver_memory: str = "2g") -> SparkSession:
    """
    Build (or fetch) the project Spark session.

    The timezone is pinned to UTC so that `order_datetime` is not silently
    shifted by whichever machine runs the pipeline - dates are the partition
    key, so a one-hour shift would move transactions between partitions.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.repl.eagerEval.enabled", True)
        .config("spark.sql.parquet.cacheMetadata", "true")
        .config("spark.sql.session.timeZone", "Etc/UTC")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "32")
        # Silences the noisy legacy-datetime warning on the 2021 snapshot,
        # which was written by an older Spark version.
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .getOrCreate()
    )
