"""
A single place to build the Spark session so that every script and notebook
gets identical configuration. Importing this rather than calling
SparkSession.builder in each notebook is what makes the pipeline reproducible.
"""

import os

from pyspark.sql import SparkSession

# Stage 3 aggregates ~14M transactions through two window functions and a
# countDistinct, which does not fit in the 2g default - it gets killed by the
# OS out-of-memory killer with no Python traceback, which looks like a hang
# rather than a failure. 4g is the working default; override per-script or via
# the SPARK_DRIVER_MEMORY environment variable on a smaller machine.
DEFAULT_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "4g")


def create_spark(app_name: str = "MAST30034 BNPL",
                 driver_memory: str = None) -> SparkSession:
    """
    Build (or fetch) the project Spark session.

    The timezone is pinned to UTC so that `order_datetime` is not silently
    shifted by whichever machine runs the pipeline - dates are the partition
    key, so a one-hour shift would move transactions between partitions.
    """
    driver_memory = driver_memory or DEFAULT_DRIVER_MEMORY

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.repl.eagerEval.enabled", True)
        .config("spark.sql.parquet.cacheMetadata", "true")
        .config("spark.sql.session.timeZone", "Etc/UTC")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "32")
        # Spilling is preferable to dying on a laptop-sized driver.
        .config("spark.sql.adaptive.enabled", "true")
        # The 2021 snapshots were written by an older Spark version and carry
        # legacy hybrid-calendar dates; CORRECTED silences the rebase warning
        # and reads them consistently with the 2022 snapshot.
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .getOrCreate()
    )
