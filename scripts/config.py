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
# Transaction snapshots are globbed. The project ships three, covering
# 2021-02-28 to 2022-10-26 with no gaps and no overlap:
#   transactions_20210228_20210827_snapshot   3,643,266 rows
#   transactions_20210828_20220227_snapshot   4,508,106 rows
#   transactions_20220228_20220828_snapshot   6,044,133 rows
# Adding or removing one requires no code change anywhere.
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
#
# percentile_approx remains partition-dependent, so the count of flagged rows
# can move by a handful between runs on different cluster geometry. The
# thresholds actually used are written to curated/category_thresholds so the
# exact values behind any given run are recoverable.
CATEGORY_OUTLIER_ACCURACY = 100_000

# Merchants with fewer than this many transactions are ranked separately -
# they are the "new merchant with little information" case in the spec, not
# merchants we can score with confidence.
MIN_TRANSACTIONS_FOR_RANKING = 30

# Minimum observation window before revenue is annualised, in months.
#
# `months_observed` is elapsed time since a merchant's first transaction, so a
# merchant who arrived in the final fortnight has a divisor near zero. Without
# a floor, one $1,300 purchase seen five days before the window closes
# annualises to $15,700 and takes the top of the watchlist.
#
# Three months is a quarter: below that we are extrapolating more than 4x from
# what we actually saw, and the firm should not price on it. The floor is
# conservative (it can only reduce a projection) and monotone (it never
# reorders two merchants with the same exposure).
MIN_MONTHS_FOR_PROJECTION = 3.0

# Shrinkage constant for the thin-history watchlist. See rank_cohorts in
# etl_04_ranking.py - a merchant's own score is credited in proportion to
# n / (n + k), with the remainder pulled to the cohort median, so a merchant
# with one transaction cannot outrank one with twenty-nine on noise alone.
# Set equal to the ranking threshold so that a merchant right at the boundary
# is credited with half their own signal.
WATCHLIST_SHRINKAGE_K = MIN_TRANSACTIONS_FOR_RANKING

# Take rate is quoted in the merchant tags as a percentage.
TAKE_RATE_IS_PERCENT = True

# --- Fraud ----------------------------------------------------------------
# The supplied fraud files give a *probability*, not a confirmed fraud flag,
# and they only cover part of the transaction window:
#
#   consumer labels   2021-02-28 to 2022-02-27   34,864 rows (keyed user/day)
#   merchant labels   2021-03-25 to 2022-02-27      114 rows (keyed abn/day)
#   transactions      2021-02-28 to 2022-10-26
#
# Everything after 2022-02-27 is UNLABELLED, not fraud-free. Merchant fraud
# features are therefore computed against the labelled window only, otherwise
# a merchant who trades mostly in 2022 gets an artificially clean fraud rate
# purely because nobody looked.
FRAUD_LABEL_START = "2021-02-28"
FRAUD_LABEL_END = "2022-02-27"

# Threshold for the boolean `is_fraud` field the project spec asks for.
# Stored on the 0-1 scale; the source files quote 0-100 and are rescaled in
# etl_01_raw.py.
#
# This is a conservative cut: only 2.0% of consumer labels and 23.7% of
# merchant labels sit above it. The ranking system therefore leans on the
# probability-weighted `expected_fraud_value` rather than on this boolean,
# which is kept because it is what the brief asks for and because it is the
# easier number to put in front of a non-technical stakeholder.
FRAUD_PROBABILITY_THRESHOLD = 0.50

RANDOM_SEED = 42
