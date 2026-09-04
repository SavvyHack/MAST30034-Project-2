"""
Mapping consumer postcodes onto ABS Statistical Areas Level 2 (SA2).

This is the part of the project most likely to be done wrong, so the reasoning
is spelled out here.

Consumers are located by postcode. Every ABS demographic dataset worth using
(income, population, census) is published by SA2. Postcodes and SA2s are not
nested: one postcode can span several SA2s and one SA2 can be split across
several postcodes. The ABS itself warns that postcode boundaries are not
authoritative and should not be used for geocoding, so any mapping we build is
an approximation and needs to be stated as an assumption in the final notebook.

The ABS publishes a correspondence file (POA 2021 -> SA2 2021) with a
RATIO_FROM_TO column giving the proportion of each postcode that falls into
each SA2. Two things are built from it:

1. `postcode_attributes` - a ratio-weighted average of SA2-level attributes
   for each postcode. This is the statistically correct way to give a postcode
   a single income or population figure, and it avoids duplicating consumers
   across SA2s (which would inflate every transaction count downstream).

2. `dominant_sa2` - the single highest-ratio SA2 per postcode. Used only for
   choropleth maps, where a region has to be picked.

Neither is perfect. Approach 1 is used for all modelling; approach 2 is used
for visuals only, and that split is deliberate.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config

# The ABS has published this correspondence under several column namings.
# Candidate names are listed so the loader works regardless of which vintage
# of the file was downloaded.
_POA_COLUMNS = ["POA_CODE_2021", "POA_CODE", "POSTCODE", "POA_CODE_2016"]
_SA2_CODE_COLUMNS = ["SA2_CODE_2021", "SA2_MAINCODE_2021", "SA2_CODE"]
_SA2_NAME_COLUMNS = ["SA2_NAME_2021", "SA2_NAME"]
_RATIO_COLUMNS = ["RATIO_FROM_TO", "RATIO"]


def _first_present(df: pd.DataFrame, candidates, label: str) -> str:
    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(
        f"Could not find a {label} column in the correspondence file. "
        f"Looked for {candidates}, file has {list(df.columns)}. "
        f"Add the correct name to scripts/geo.py."
    )


def load_correspondence(path: Path = None) -> pd.DataFrame:
    """
    Load the POA -> SA2 correspondence into a tidy frame with columns
    [postcode, sa2_code, sa2_name, ratio].
    """
    path = Path(path or config.POSTCODE_SA2_FILE)
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run `python scripts/download_external.py` "
            f"or place the ABS correspondence file there manually."
        )

    if path.suffix.lower() in {".xlsx", ".xls"}:
        # The ABS ships this as a workbook with a title block above the table;
        # the data sheet is normally the second one.
        raw = pd.read_excel(path, sheet_name=-1, skiprows=5)
    else:
        raw = pd.read_csv(path)

    raw.columns = [str(c).strip() for c in raw.columns]

    out = pd.DataFrame(
        {
            "postcode": raw[_first_present(raw, _POA_COLUMNS, "postcode")]
            .astype(str)
            .str.extract(r"(\d{3,4})")[0]
            .str.zfill(4),
            "sa2_code": raw[_first_present(raw, _SA2_CODE_COLUMNS, "SA2 code")].astype(str),
            "sa2_name": raw[_first_present(raw, _SA2_NAME_COLUMNS, "SA2 name")].astype(str),
            "ratio": pd.to_numeric(
                raw[_first_present(raw, _RATIO_COLUMNS, "ratio")], errors="coerce"
            ),
        }
    ).dropna(subset=["postcode", "ratio"])

    # Ratios should sum to 1 within a postcode. Renormalise defensively - the
    # published file has rounding drift, and any postcode whose ratios sum to
    # something far from 1 is worth knowing about.
    totals = out.groupby("postcode")["ratio"].transform("sum")
    drift = (totals - 1).abs()
    if (drift > 0.05).any():
        bad = out.loc[drift > 0.05, "postcode"].nunique()
        print(f"  warning: {bad} postcodes have correspondence ratios far from 1")
    out["ratio"] = out["ratio"] / totals

    return out


def postcode_attributes(correspondence: pd.DataFrame, sa2_data: pd.DataFrame,
                        value_columns) -> pd.DataFrame:
    """
    Collapse SA2-level attributes to postcode level using the correspondence
    ratios as weights.

    `sa2_data` must have an `sa2_code` column plus the columns named in
    `value_columns`. The result has one row per postcode.
    """
    merged = correspondence.merge(sa2_data, on="sa2_code", how="left")

    frames = {}
    for column in value_columns:
        weight = merged["ratio"] * merged[column].notna()
        weighted = (merged["ratio"] * merged[column].fillna(0)).groupby(
            merged["postcode"]
        ).sum()
        denominator = weight.groupby(merged["postcode"]).sum()
        # Where no SA2 in a postcode has data, the denominator is zero and the
        # result is left as NaN rather than silently becoming 0.
        frames[column] = weighted / denominator.replace(0, pd.NA)

    return pd.DataFrame(frames).reset_index()


def dominant_sa2(correspondence: pd.DataFrame) -> pd.DataFrame:
    """One row per postcode giving its largest-share SA2. For maps only."""
    idx = correspondence.groupby("postcode")["ratio"].idxmax()
    return (
        correspondence.loc[idx, ["postcode", "sa2_code", "sa2_name", "ratio"]]
        .rename(columns={"ratio": "sa2_share"})
        .reset_index(drop=True)
    )


def coverage_report(consumer_postcodes, correspondence: pd.DataFrame) -> dict:
    """
    How many consumer postcodes fail to match the correspondence file.

    Sprint 2 asks explicitly what was missing after joining to the external
    dataset, so this number should go straight into the notebook.
    """
    consumer_postcodes = pd.Series(consumer_postcodes).astype(str).str.zfill(4)
    known = set(correspondence["postcode"])
    matched = consumer_postcodes.isin(known)
    return {
        "n_consumers": int(len(consumer_postcodes)),
        "n_unique_postcodes": int(consumer_postcodes.nunique()),
        "n_unmatched_consumers": int((~matched).sum()),
        "n_unmatched_postcodes": int(consumer_postcodes[~matched].nunique()),
        "pct_unmatched": round(float((~matched).mean() * 100), 3),
        "unmatched_examples": sorted(consumer_postcodes[~matched].unique())[:20],
    }
