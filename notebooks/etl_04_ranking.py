"""
Stage 4: the ranking system.

The question the BNPL firm is actually asking is not "who is biggest?" but
"which 100 partnerships will make us the most money without exposing us to
unacceptable risk?" Those are different questions, and ranking on raw
transaction volume answers the wrong one.

The score is built from three pillars:

  VALUE   what we expect to earn from this merchant over the next year
  GROWTH  whether that number is heading up or down
  RISK    how likely that number is to be wrong, or to be lost

Every pillar is converted to a percentile before being combined. This is
deliberate. The underlying features are heavily skewed - the largest merchant
processes tens of thousands of times more transactions than the smallest - so a
weighted sum of raw values would be a ranking of transaction volume with three
decorative extra terms. Percentiles put each pillar on the same 0-1 footing so
the weights mean what they say.

Merchants are ranked twice: once across the whole book (the top 100) and once
within each business segment (the top 10 each), because a merchant with a $50
average basket and thousands of orders and one with a $3,000 basket and a
handful of orders are not competing for the same partnership slot.

This runs in pandas rather than Spark. The feature table is ~4,000 rows; a
Spark job here would cost more in startup than the whole computation.

Run:  python scripts/etl_04_ranking.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config

# --- Weights --------------------------------------------------------------
# These are a business judgement, not a fitted parameter, and the final
# presentation should defend them as such. Value carries the most weight
# because it is the thing the firm is directly paid on. Growth and risk are
# equal: a merchant on the way up is worth roughly as much extra as a volatile
# merchant is worth less.
PILLAR_WEIGHTS = {
    "value": 0.50,
    "growth": 0.25,
    "risk": 0.25,
}

# Within the risk pillar. Each component is a percentile where HIGHER MEANS
# WORSE; the pillar is inverted at the end so that a high risk score is good.
RISK_WEIGHTS = {
    "revenue_volatility": 0.30,      # is the revenue steady month to month?
    "customer_hhi": 0.30,            # does it rest on a handful of customers?
    "excluded_share": 0.20,          # how much of their volume failed our rules?
    "inactivity": 0.20,              # do they trade most days or in bursts?
}

# The forward projection applies the observed monthly trend as a single-year
# uplift, clipped. Without a clip, a merchant whose revenue happened to double
# across a short window projects to an absurd figure and takes the top of the
# ranking on nine months of noise.
GROWTH_CLIP = 0.30


def percentile(series: pd.Series) -> pd.Series:
    """
    Rank a feature to [0, 1], where 1 is best.

    NaNs are given the median rank (0.5) rather than 0. A merchant with a
    missing volatility figure has not demonstrated that it is volatile - it has
    demonstrated nothing - and scoring absence as failure would systematically
    push newer merchants down the list for the wrong reason.
    """
    ranked = series.rank(pct=True, na_option="keep")
    return ranked.fillna(0.5)


def build_scores(features: pd.DataFrame) -> pd.DataFrame:
    df = features.copy()

    # --- Projected annual revenue ----------------------------------------
    # The headline number: what we expect this merchant to earn us over the
    # next twelve months, in dollars. This is the figure to put on a slide -
    # it is the only quantity here a non-technical stakeholder can price.
    df["monthly_bnpl_revenue"] = df["total_bnpl_revenue"] / df["n_months"].clip(lower=1)
    growth_factor = 1 + df["revenue_growth_rate"].fillna(0).clip(-GROWTH_CLIP, GROWTH_CLIP)
    df["projected_annual_revenue"] = df["monthly_bnpl_revenue"] * 12 * growth_factor

    # --- VALUE pillar -----------------------------------------------------
    # Projected revenue is the substance. Customer base breadth is included at
    # a lower weight because a merchant bringing many distinct people onto the
    # platform is worth more than their take-rate alone suggests: those
    # customers become available to every other merchant on the book.
    df["value_score"] = (
        0.70 * percentile(df["projected_annual_revenue"])
        + 0.20 * percentile(df["n_customers"])
        + 0.10 * percentile(df["mean_basket"])
    )

    # --- GROWTH pillar ----------------------------------------------------
    # Trend in revenue, plus how consistently they trade. A merchant growing
    # while trading nearly every day is a more convincing prospect than one
    # whose growth comes from two large days.
    df["growth_score"] = (
        0.70 * percentile(df["revenue_growth_rate"])
        + 0.30 * percentile(df["transaction_density"])
    )

    # --- RISK pillar ------------------------------------------------------
    total_seen = df["n_transactions"] + df["n_transactions_excluded"]
    df["excluded_share"] = df["n_transactions_excluded"] / total_seen.clip(lower=1)
    df["inactivity"] = 1 - df["transaction_density"]

    risk_components = pd.DataFrame(
        {name: percentile(df[name]) for name in RISK_WEIGHTS}, index=df.index
    )
    raw_risk = sum(risk_components[name] * weight for name, weight in RISK_WEIGHTS.items())
    # Inverted, so that risk_score is like the others: higher is better.
    df["risk_score"] = 1 - raw_risk

    # --- Combine ----------------------------------------------------------
    df["final_score"] = (
        PILLAR_WEIGHTS["value"] * df["value_score"]
        + PILLAR_WEIGHTS["growth"] * df["growth_score"]
        + PILLAR_WEIGHTS["risk"] * df["risk_score"]
    )

    return df


def rank_cohorts(scored: pd.DataFrame) -> pd.DataFrame:
    """
    Rank established merchants and thin-history merchants separately.

    A merchant with eleven transactions may have a flattering score built on
    almost no evidence. Mixing them into one list would let noise outrank
    demonstrated performance, so they are ranked in their own cohort and
    presented as a watchlist rather than as onboarding recommendations. This is
    the "new merchant with little information" case the project spec calls out.
    """
    scored = scored.copy()
    scored["cohort"] = np.where(
        scored["sufficient_history"], "established", "insufficient_history"
    )

    scored["overall_rank"] = (
        scored[scored["cohort"] == "established"]["final_score"]
        .rank(ascending=False, method="first")
    )
    scored["segment_rank"] = (
        scored[scored["cohort"] == "established"]
        .groupby("segment")["final_score"]
        .rank(ascending=False, method="first")
    )
    scored["watchlist_rank"] = (
        scored[scored["cohort"] == "insufficient_history"]["final_score"]
        .rank(ascending=False, method="first")
    )

    return scored.sort_values("final_score", ascending=False)


REPORT_COLUMNS = [
    "overall_rank",
    "merchant_abn",
    "merchant_name",
    "segment",
    "category",
    "revenue_level",
    "take_rate",
    "projected_annual_revenue",
    "n_transactions",
    "n_customers",
    "mean_basket",
    "value_score",
    "growth_score",
    "risk_score",
    "final_score",
]


def main():
    features = pd.read_parquet(config.CURATED_DIR / "merchant_features")
    print(f"Scoring {len(features)} merchants...")

    scored = rank_cohorts(build_scores(features))
    scored.to_parquet(config.CURATED_DIR / "merchant_rankings.parquet", index=False)

    established = scored[scored["cohort"] == "established"]

    top_100 = established.nsmallest(100, "overall_rank")[REPORT_COLUMNS]
    top_100.to_csv(config.CURATED_DIR / "top_100_merchants.csv", index=False)

    top_by_segment = (
        established[established["segment_rank"] <= 10]
        .sort_values(["segment", "segment_rank"])[["segment_rank"] + REPORT_COLUMNS[1:]]
    )
    top_by_segment.to_csv(config.CURATED_DIR / "top_10_by_segment.csv", index=False)

    watchlist = scored[scored["cohort"] == "insufficient_history"].nsmallest(
        20, "watchlist_rank"
    )[["watchlist_rank"] + REPORT_COLUMNS[1:]]
    watchlist.to_csv(config.CURATED_DIR / "watchlist_merchants.csv", index=False)

    # --- Report -----------------------------------------------------------
    print(f"\n  established cohort:    {len(established)}")
    print(f"  insufficient history:  {len(scored) - len(established)}")
    print(
        f"\n  Top 100 account for "
        f"${top_100['projected_annual_revenue'].sum():,.0f} of projected annual revenue, "
        f"{100 * top_100['projected_annual_revenue'].sum() / established['projected_annual_revenue'].sum():.1f}% "
        f"of the established book from "
        f"{100 * 100 / len(established):.1f}% of merchants."
    )

    print("\n  Segment composition of the top 100:")
    for segment, count in top_100["segment"].value_counts().items():
        print(f"    {segment:<26} {count}")

    print("\n  Top 10 overall:")
    preview = top_100.head(10)[
        ["overall_rank", "merchant_name", "segment", "projected_annual_revenue", "final_score"]
    ]
    print(preview.to_string(index=False, float_format=lambda v: f"{v:,.2f}"))

    print(f"\nStage 4 complete. Outputs in {config.CURATED_DIR}")


if __name__ == "__main__":
    main()
