# BNPL merchant-ranking project

This project ranks the supplied merchants for a BNPL firm with capacity to onboard 100 each year.
Start with `summary.ipynb` for the stakeholder results

## Reproduce the results

The verified environment is Ubuntu/WSL, Python 3.12 and OpenJDK 17, with about 8 GB of RAM.
The runner defaults to two worker threads. The first full run creates several GB of intermediate data.

From this directory:

```bash
python3.12 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
python run_pipeline.py
```


## Input data

Course-provided files, under `tables/`:

- `transactions_20210228_20210827_snapshot/`
- `transactions_20210828_20220227_snapshot/`
- `transactions_20220228_20220828_snapshot/` (actually extends to 26 October 2022)
- `tbl_merchants.parquet`, `tbl_consumer.csv`, `consumer_user_details.parquet`
- `consumer_fraud_probability.csv`, `merchant_fraud_probability.csv`

External files:

| Folder under external_data | Required files | Source |
|---|---|---|
| income | Table 1 - Total income, earners and summary statistics by geography, 2018-19 to 2022-23.xlsx | [ABS Personal Income in Australia, 2022-23](https://www.abs.gov.au/statistics/labour/earnings-and-working-conditions/personal-income-australia/latest-release), Table 1, sheet Table 1.4 |
| population | 32180DS0001_2021-22r.xlsx | [ABS Regional population, 2021-22](https://www.abs.gov.au/statistics/people/population/regional-population/2021-22), revised workbook |
| population | 32180DS0001_2022-23.xlsx | [ABS Regional population, 2022-23](https://www.abs.gov.au/statistics/people/population/regional-population/2022-23) |
| shapefile | MB_2021_AUST.xlsx; POA_2021_AUST.xlsx | ABS ASGS Edition 3 mesh-block allocation files |
| shapefile | SA2_2021_AUST_GDA2020.shp, .shx, .dbf, .prj (keep associated files together) | [ABS ASGS 2021 access and downloads](https://www.abs.gov.au/book/export/25822/print) |


## Pipeline and outputs

| Order | Notebook | Main outputs |
|---|---|---|
| 1 | etl.ipynb | curated/transactions.parquet; null, missing-match and outlier reconciliation |
| 2 | fraud.ipynb | transactions_fraud.parquet; fraud_user_days.parquet; fraud_predictions.parquet; fraud_model_validation.csv |
| 3 | features.ipynb | merchant_daily.parquet; merchant_features.parquet; sa2_summary.parquet; separate past-only validation tables; merchant_daily_untrimmed.parquet |
| 4 | forecast.ipynb | merchant_forecast.parquet; forecast_validation.csv; fraud_rate_validation.csv |
| 5 | ranking.ipynb | output/merchant_ranking.csv; top_100_merchants.csv; top_10_per_segment.csv; ranking_robustness.csv |
| 6 | summary.ipynb | stakeholder tables, case studies and maps |

All intermediate tables are under `curated/`; shareable chart images are under `plots/`.
Intermediate datasets, scratch audits and caches are ignored by Git. Final CSVs, chart images and executed
notebooks can be included with the submission. Keep the repository private as the BNPL spec requests;
repository hosting/privacy and Canvas submission must be checked by the group.

## Interpretation and validation

- Sales are consumer purchase amounts; BNPL revenue is sales times take rate divided by 100.
- `expected_profit_12m` is expected contribution before operating, funding, onboarding and credit losses.
  Full fraud liability is a scenario assumption, not a fact established by the supplied data.
- `in_fraud_delta` records a match to either supplied probability file. `is_fraud` is the separate 50% threshold.
  Unlisted days within coverage use an assumed zero proxy; they are not verified genuine transactions.
- Fraud modelling retains valid unusual purchases in behavioural features. Training-only spending baselines
  prevent the January-February validation features from using future spending.
- Forecast-validation outlier thresholds are fitted before each cutoff. Fraud-rate blending is selected against
  held-out supplied scores, not model-generated future scores. Neither test establishes confirmed real-world losses.
- The ranking sensitivity table covers liability, fraud history, forecast choice and retaining valid outliers.
- Customer geography is approximate. [ABS postal areas approximate postcodes](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs/edition-3-july-2021-june-2026/non-abs-structures/postal-areas);
  residential mesh-block counts are not exact dwelling/population weights.
- Forecast confidence labels describe history and transaction volume, not statistical confidence intervals.

The notebooks contain assertions on row preservation, order uniqueness, probability ranges, merchant coverage,
positive/eligible top-100 selections and ten recommendations per segment. The remaining scientific limitations
and presentation tasks are documented in the audit, rather than being presented as completed evidence.