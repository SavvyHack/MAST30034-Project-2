# MAST30034 Project 2 — Buy Now, Pay Later Merchant Ranking

Which 100 merchants should a BNPL firm onboard this year, and why?

This repository contains an automated ETL pipeline and a ranking system that
scores ~4,000 candidate merchants on their expected value to the firm, their
growth trajectory, and the risk attached to both.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python scripts/download_external.py   # ABS data (see note below)
python scripts/etl_01_raw.py          # typed copies of the source files
python scripts/etl_02_curated.py      # joins + fraud labels + business rules
python scripts/etl_03_features.py     # merchant-level feature table
python scripts/etl_04_ranking.py      # scores and rankings
```

Then open the notebooks in order. Requires Python 3.12 and a working Java
installation for PySpark.

The pipeline degrades rather than failing if the ABS data has not been
downloaded — the demographic features come through as null and everything else
still runs. That is intentional so a fresh clone is never dead on arrival.

**Memory.** Stage 3 aggregates 14M transactions through two window functions
and a `countDistinct`. The default driver allocation is 4g; on a smaller
machine set `SPARK_DRIVER_MEMORY=2g` and expect it to be slow. If a stage
stops with no traceback, that is the OS out-of-memory killer, not a hang.

## Data

Three transaction snapshots are supplied on Canvas. All three are required —
the fraud labels only overlap the first two.

| snapshot | dates | rows |
|---|---|---|
| `transactions_20210228_20210827_snapshot` | 2021-02-28 → 2021-08-27 | 3,643,266 |
| `transactions_20210828_20220227_snapshot` | 2021-08-28 → 2022-02-27 | 4,508,106 |
| `transactions_20220228_20220828_snapshot` | 2022-02-28 → 2022-10-26 | 6,044,133 |

They are contiguous, non-overlapping, and share no `order_id`. Stage 1 asserts
that on every run, because an overlapping snapshot would silently double every
revenue figure in the project.

## Repository structure

```
scripts/
  config.py             every path and business-rule threshold, in one place
  spark_session.py      shared Spark configuration
  tags.py               parses the merchant `tags` field; segment definitions
  geo.py                postcode -> SA2 correspondence handling
  download_external.py  fetches ABS datasets
  etl_01_raw.py         source files -> data/raw/ with enforced schemas
  etl_02_curated.py     joins, ABS attachment, fraud labels, business rules
  etl_03_features.py    transaction level -> merchant level features
  etl_04_ranking.py     features -> scores, top 100, top 10 per segment

notebooks/
  01_data_quality.ipynb  nulls, joins, outliers (Sprint 2 checkpoint)
  02_ranking.ipynb       the ranking system and final recommendations

tables/    supplied data from Canvas
data/      pipeline output — gitignored, fully regenerable
```

## Pipeline design

**Three layers.** `tables/` is never written to. `data/raw/` holds typed,
faithful copies of the source. `data/curated/` holds joined data with business
rules applied. Each stage reads the previous one, so any stage can be re-run
without re-running the ones before it.

**Schemas are declared, not inferred.** The transaction data spans ~600 daily
partition folders. Schema inference samples those files, which can produce
inconsistent types across partitions. Declaring the schema makes any change in
the source data fail loudly at stage 1 instead of silently downstream.

**Nothing is deleted.** Rows failing a business rule are flagged, not dropped,
and an `is_valid` column marks what the ranking should use. This keeps the
Sprint 2 question — what happened to the rows you removed? — answerable, and
keeps rates computable against their true denominator.

**Snapshots are discovered by glob.** Dropping an additional transactions
snapshot into `tables/` requires no code change, and a `snapshot` column is
carried through so the sources can be told apart.

## Fraud

The supplied fraud files give a **probability**, not a confirmed flag, and they
only cover **2021-02-28 to 2022-02-27** — roughly the first 58% of the
transaction window. Three consequences shape how the pipeline handles them:

1. **Everything after 2022-02-27 is unlabelled, not fraud-free.** The curated
   table carries `in_fraud_label_window`, and all fraud aggregates use it as
   their denominator. Without that, a merchant who traded mostly in 2022 would
   look spotless purely because nobody looked. 99% of merchants have some
   labelled volume; the remainder score at the median rather than at zero.

2. **The consumer fraud file has 99 pairs of exactly duplicated rows.** Left
   in, they fan out the join and double-count dollar value. Stage 1 dedupes on
   the join key and prints the count; stage 2 asserts the keys are unique
   before joining.

3. **The probabilities are skewed low** — only 2.0% of consumer labels and
   23.7% of merchant labels exceed 50%. A boolean threshold therefore discards
   most of the signal. `is_fraud` exists because the brief asks for it, but the
   risk pillar scores on `expected_fraud_share`: probability-weighted dollars
   at risk over labelled dollars.

Removing the fraud component changes 11 of the top 100. Those 11 have a median
fraud exposure roughly 20x the book median, which is the behaviour intended.

## The ranking system

Merchants are scored on three pillars, each converted to a percentile before
being combined so that no single skewed feature dominates:

| Pillar | Weight | What it captures |
|---|---|---|
| Value | 0.50 | Projected annual revenue to the firm, customer base breadth, basket size |
| Growth | 0.25 | Revenue trend, trading consistency |
| Risk | 0.25 | Fraud exposure, revenue volatility, customer concentration, share of transactions failing business rules, inactivity |

The weights are a business judgement, not a fitted parameter, and are stated
here so they can be argued with.

**Projections are annualised over months observed, not months traded.** A
merchant who traded in 2 of 20 months has 20 months of exposure, not 2.
Dividing by months traded inflates sparse merchants by an order of magnitude.
A three-month minimum observation floor applies, below which we would be
extrapolating more than 4x from what we actually saw.

**Merchants with fewer than 30 transactions are ranked in a separate cohort**
and presented as a watchlist rather than as onboarding recommendations. Within
that cohort, scores are shrunk toward the cohort median in proportion to
`n / (n + 30)`, so a merchant with one transaction cannot outrank one with
twenty-nine on the strength of a single large sale.

## Known limitations

1. **386 merchant ABNs appear in transactions with no merchant record**, worth
   a meaningful share of all transaction value. Without a category or take
   rate they cannot be scored, so they are excluded from ranking and reported
   separately.
2. **Fraud labels cover only the first 58% of the window**, and are
   probabilities rather than confirmed outcomes. The threshold used to derive
   `is_fraud` is our choice, not a fact in the data.
3. **Postcode to SA2 is approximate.** The ABS states that postcode boundaries
   are not authoritative and should not be used for geocoding. Ratio-weighted
   attributes are used for modelling and dominant-SA2 for maps.
4. **Twenty months of data**, so seasonality and trend are only partly
   separable. Growth features remain closer to momentum than to annual growth.
5. **`percentile_approx` is partition-dependent**, so the count of rows flagged
   as category outliers can move by a handful between runs on different
   hardware. The thresholds actually applied are written to
   `data/curated/category_thresholds` so any given run is recoverable.
6. **All data is synthetic.** Apparent relationships may be generator artefacts.

## Security and privacy

No credentials are committed. `data/` is gitignored in full. The consumer table
contains names and addresses; these are dropped at the curated stage, since
nothing downstream needs to identify an individual to rank a merchant.