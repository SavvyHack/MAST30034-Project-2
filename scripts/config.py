"""
Central configuration for the BNPL merchant ranking pipeline.

Every path in the project is defined here so that no script contains a
hard-coded string. If the data moves, this is the only file that changes.
"""

from pathlib import Path

# --- Project layout -------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

TABLES_DIR = PROJECT_ROOT / "tables"          # supplied data (from Canvas)
DATA_DIR = PROJECT_ROOT / "data"              # everything the pipeline creates
RAW_DIR = DATA_DIR / "raw"                    # schema-enforced copies of source
CURATED_DIR = DATA_DIR / "curated"            # joined, cleaned, business rules applied
EXTERNAL_DIR = DATA_DIR / "external"          # ABS downloads
PLOTS_DIR = PROJECT_ROOT / "plots"

for _d in (RAW_DIR, CURATED_DIR, EXTERNAL_DIR, PLOTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# --- Source files ---------------------------------------------------------
# Transaction snapshots are globbed so that adding a second snapshot folder
# (e.g. the 2021-02-28 to 2022-02-28 one) requires no code change.
TRANSACTION_GLOB = "transactions_*_snapshot"

MERCHANTS_FILE = TABLES_DIR / "tbl_merchants.parquet"
CONSUMER_FILE = TABLES_DIR / "tbl_consumer.csv"
CONSUMER_USER_FILE = TABLES_DIR / "consumer_user_details.parquet"
CONSUMER_FRAUD_FILE = TABLES_DIR / "consumer_fraud_probability.csv"
MERCHANT_FRAUD_FILE = TABLES_DIR / "merchant_fraud_probability.csv"

# --- External (ABS) files -------------------------------------------------
# See scripts/download_external.py for provenance and download instructions.
POSTCODE_SA2_FILE = EXTERNAL_DIR / "postcode_2021_sa2_2021.csv"
SA2_INCOME_FILE = EXTERNAL_DIR / "sa2_income.csv"
SA2_POPULATION_FILE = EXTERNAL_DIR / "sa2_population.csv"
SA2_SHAPEFILE = EXTERNAL_DIR / "SA2_2021_AUST_GDA2020.shp"

# --- Business rules -------------------------------------------------------
# Applied in etl_02_curated.py. Every threshold lives here so that the
# assumptions behind the curated dataset are visible in one place and can be
# defended in the final notebook.

# A transaction below this value cannot be a real BNPL "pay in 5" purchase.
# The generator produces values as small as 7e-07 dollars.
MIN_TRANSACTION_VALUE = 1.00

# Upper bound is applied *per merchant category* rather than globally, because
# $8,000 is unremarkable for furniture and impossible for a florist.
# Transactions above this quantile of their own category are flagged.
CATEGORY_OUTLIER_QUANTILE = 0.999
# percentile_approx defaults to an accuracy of 10,000, which is not enough
# resolution to resolve a 0.999 quantile reliably - it can return the maximum
# and flag nothing at all. Raised explicitly.
CATEGORY_OUTLIER_ACCURACY = 100_000

# Merchants with fewer than this many transactions are ranked separately -
# they are the "new merchant with little information" case in the spec, not
# merchants we can score with confidence.
MIN_TRANSACTIONS_FOR_RANKING = 30

# Take rate is quoted in the merchant tags as a percentage.
TAKE_RATE_IS_PERCENT = True

RANDOM_SEED = 42
