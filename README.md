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
python scripts/etl_02_curated.py      # joins + business rules
python scripts/etl_03_features.py     # merchant-level feature table
python scripts/etl_04_ranking.py      # scores and rankings
```

Then open the notebooks in order. Requires Python 3.12 and a working Java
installation for PySpark.

The pipeline degrades rather than failing if the ABS data has not been
downloaded — the demographic features come through as null and everything else
still runs. That is intentional so a fresh clone is never dead on arrival.

## Repository structure

```
scripts/
  config.py             every path and business-rule threshold, in one place
  spark_session.py      shared Spark configuration
  tags.py               parses the merchant `tags` field; segment definitions
  geo.py                postcode -> SA2 correspondence handling
  download_external.py  fetches ABS datasets
  etl_01_raw.py         source files -> data/raw/ with enforced schemas
  etl_02_curated.py     joins, ABS attachment, business rules -> data/curated/
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

**Schemas are declared, not inferred.** The transaction data spans ~241 daily
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

## The ranking system

Merchants are scored on three pillars, each converted to a percentile before
being combined so that no single skewed feature dominates:

| Pillar | Weight | What it captures |
|---|---|---|
| Value | 0.50 | Projected annual revenue to the firm, customer base breadth, basket size |
| Growth | 0.25 | Revenue trend, trading consistency |
| Risk | 0.25 | Revenue volatility, customer concentration, share of transactions failing business rules, inactivity |

The weights are a business judgement, not a fitted parameter, and are stated
here so they can be argued with.

Merchants with fewer than 30 transactions are ranked in a separate cohort and
presented as a watchlist rather than as onboarding recommendations — a
flattering score built on eleven transactions is noise, not evidence.

## Known limitations

1. **The fraud labels do not overlap the transaction data.** The supplied fraud
   files cover 2021-02-28 to 2022-02-27; the transaction snapshot covers
   2022-02-28 to 2022-10-26. The intersection is empty, so no fraud model can
   be trained until the earlier snapshot is obtained. The risk pillar currently
   uses behavioural proxies only.
2. **386 merchant ABNs appear in transactions with no merchant record**, worth
   8.6% of all transaction value. Without a category or take rate they cannot
   be scored, so they are excluded from ranking and reported separately.
3. **Postcode to SA2 is approximate.** The ABS states that postcode boundaries
   are not authoritative and should not be used for geocoding. Ratio-weighted
   attributes are used for modelling and dominant-SA2 for maps.
4. **The window is under a year**, so seasonality cannot be separated from
   trend. Growth features are short-run momentum, not annual growth.
5. **All data is synthetic.** Apparent relationships may be generator artefacts.

## Security and privacy

No credentials are committed. `data/` is gitignored in full. The consumer table
contains names and addresses; these are dropped at the curated stage, since
nothing downstream needs to identify an individual to rank a merchant.