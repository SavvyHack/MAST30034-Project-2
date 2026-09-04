"""
Download the external ABS datasets the project depends on.

The ABS does not publish a direct 2021 Postal Area (POA) -> 2021 SA2
correspondence at the URL previously used by this project. Instead, both POAs
and SA2s are built from 2021 Mesh Blocks. This script therefore downloads the
official ABS Mesh Block and Postal Area allocation files, joins them on
MB_CODE_2021, and creates the postcode -> SA2 correspondence expected by the
rest of the pipeline.

The generated correspondence is area-weighted because the allocation files
provide Mesh Block area. It is an approximation and should be described as
such in the final analysis.

Run:
    python scripts/download_external.py
"""

import io
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config

TIMEOUT = 120
HEADERS = {"User-Agent": "MAST30034-student-project"}

ASGS_BASE = (
    "https://www.abs.gov.au/statistics/standards/"
    "australian-statistical-geography-standard-asgs/"
    "edition-3-july-2021-june-2026/access-and-downloads"
)

ALLOCATION_LANDING = f"{ASGS_BASE}/allocation-files"
BOUNDARY_LANDING = f"{ASGS_BASE}/digital-boundary-files"

MESHBLOCK_FILE = config.EXTERNAL_DIR / "MB_2021_AUST.xlsx"
POA_FILE = config.EXTERNAL_DIR / "POA_2021_AUST.xlsx"

# Official ABS files used by this project.
SOURCES = {
    "meshblock_allocation": {
        "url": f"{ALLOCATION_LANDING}/MB_2021_AUST.xlsx",
        "dest": MESHBLOCK_FILE,
        "why": "Maps each 2021 Mesh Block to its 2021 SA2.",
        "landing": ALLOCATION_LANDING,
    },
    "poa_allocation": {
        "url": f"{ALLOCATION_LANDING}/POA_2021_AUST.xlsx",
        "dest": POA_FILE,
        "why": "Maps each 2021 Mesh Block to its 2021 Postal Area.",
        "landing": ALLOCATION_LANDING,
    },
    "sa2_boundaries": {
        "url": f"{BOUNDARY_LANDING}/SA2_2021_AUST_SHP_GDA2020.zip",
        "dest": config.EXTERNAL_DIR / "SA2_2021_AUST_SHP_GDA2020.zip",
        "unzip": True,
        "why": "SA2 polygons for the geospatial visualisations.",
        "landing": BOUNDARY_LANDING,
    },
}

# These are still manual because the project expects simplified two-column
# files rather than the much wider ABS source products.
MANUAL_SOURCES = {
    "sa2_income": {
        "dest": config.SA2_INCOME_FILE,
        "source": "ABS 'Personal Income in Australia', income by SA2.",
        "why": (
            "Median personal income per SA2. Used as a proxy for the spending "
            "capacity of a merchant's customer base."
        ),
        "needs_columns": ["sa2_code", "median_income"],
    },
    "sa2_population": {
        "dest": config.SA2_POPULATION_FILE,
        "source": "ABS 'Regional population', estimated resident population by SA2.",
        "why": (
            "Population per SA2. Used to normalise merchant customer counts - "
            "20 customers in a small SA2 is stronger penetration than 20 in a "
            "large one."
        ),
        "needs_columns": ["sa2_code", "population"],
    },
}


def _extract_archive(path: Path) -> None:
    """Extract a downloaded ZIP into the external-data directory."""
    with zipfile.ZipFile(path) as archive:
        archive.extractall(config.EXTERNAL_DIR)
    print("         extracted archive")


def download(name: str, spec: dict) -> bool:
    """Download one source, skipping files that are already present."""
    dest = Path(spec["dest"])

    if dest.exists():
        print(f"  [skip] {name}: already at {dest.relative_to(config.PROJECT_ROOT)}")
        if spec.get("unzip"):
            try:
                _extract_archive(dest)
            except zipfile.BadZipFile as error:
                print(f"  [FAIL] {name}: existing archive is invalid: {error}")
                return False
        return True

    print(f"  [get ] {name} <- {spec['url']}")
    try:
        response = requests.get(spec["url"], timeout=TIMEOUT, headers=HEADERS)
        response.raise_for_status()
    except Exception as error:  # noqa: BLE001 - reason should be printed
        print(f"  [FAIL] {name}: {error}")
        print(f"         Download manually from: {spec['landing']}")
        print(f"         Save as: {dest}")
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(response.content)
    print(f"  [ok  ] {name} -> {dest.relative_to(config.PROJECT_ROOT)}")

    if spec.get("unzip"):
        try:
            _extract_archive(dest)
        except zipfile.BadZipFile as error:
            print(f"  [FAIL] {name}: downloaded file is not a valid ZIP: {error}")
            return False

    return True


def _find_data_sheet(path: Path, required_columns: set[str]) -> tuple[str, int]:
    """
    Find the worksheet and header row containing the required ABS columns.

    ABS workbooks commonly contain title/metadata rows above the actual table,
    so assuming header=0 is brittle.
    """
    try:
        workbook = pd.ExcelFile(path, engine="openpyxl")
    except ImportError as error:
        raise RuntimeError(
            "Reading ABS .xlsx files requires openpyxl. Run: pip install openpyxl"
        ) from error

    for sheet_name in workbook.sheet_names:
        preview = pd.read_excel(
            workbook,
            sheet_name=sheet_name,
            header=None,
            nrows=30,
            dtype=str,
        )
        for row_number, row in preview.iterrows():
            values = {str(value).strip() for value in row.dropna().tolist()}
            if required_columns.issubset(values):
                return sheet_name, int(row_number)

    raise ValueError(
        f"Could not find columns {sorted(required_columns)} in {path.name}. "
        "The ABS workbook layout may have changed."
    )


def _read_abs_allocation(path: Path, required_columns: set[str]) -> pd.DataFrame:
    """Read the data table from an ABS allocation workbook."""
    sheet_name, header_row = _find_data_sheet(path, required_columns)
    frame = pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=header_row,
        engine="openpyxl",
    )
    frame.columns = [str(column).strip() for column in frame.columns]

    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{path.name} is missing required columns: {sorted(missing)}")

    return frame


def _clean_code(series: pd.Series) -> pd.Series:
    """Normalise Excel-imported ABS codes without losing leading zeroes."""
    return (
        series.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def build_postcode_sa2_correspondence() -> bool:
    """
    Build the CSV expected by config.POSTCODE_SA2_FILE.

    POA and SA2 are both aggregations of 2021 Mesh Blocks, so joining the two
    official allocation files on MB_CODE_2021 gives a POA -> SA2 relationship.
    RATIO_FROM_TO is the share of the POA's Mesh Block area assigned to each
    SA2. Ratios are normalised to sum to 1 within each POA.
    """
    output = Path(config.POSTCODE_SA2_FILE)

    if not MESHBLOCK_FILE.exists() or not POA_FILE.exists():
        print("  [FAIL] postcode_sa2_correspondence: allocation files are missing")
        return False

    source_mtime = max(MESHBLOCK_FILE.stat().st_mtime, POA_FILE.stat().st_mtime)
    if output.exists() and output.stat().st_mtime >= source_mtime:
        print(
            "  [skip] postcode_sa2_correspondence: already at "
            f"{output.relative_to(config.PROJECT_ROOT)}"
        )
        return True

    print("  [make] postcode_sa2_correspondence from ABS Mesh Block allocations")

    try:
        mb = _read_abs_allocation(
            MESHBLOCK_FILE,
            {"MB_CODE_2021", "SA2_CODE_2021", "SA2_NAME_2021", "AREA_ALBERS_SQKM"},
        )
        poa = _read_abs_allocation(
            POA_FILE,
            {"MB_CODE_2021", "POA_CODE_2021"},
        )

        mb = mb[
            ["MB_CODE_2021", "SA2_CODE_2021", "SA2_NAME_2021", "AREA_ALBERS_SQKM"]
        ].copy()
        poa = poa[["MB_CODE_2021", "POA_CODE_2021"]].copy()

        mb["MB_CODE_2021"] = _clean_code(mb["MB_CODE_2021"])
        mb["SA2_CODE_2021"] = _clean_code(mb["SA2_CODE_2021"])
        mb["SA2_NAME_2021"] = mb["SA2_NAME_2021"].astype("string").str.strip()
        mb["AREA_ALBERS_SQKM"] = pd.to_numeric(
            mb["AREA_ALBERS_SQKM"], errors="coerce"
        )

        poa["MB_CODE_2021"] = _clean_code(poa["MB_CODE_2021"])
        poa["POA_CODE_2021"] = (
            _clean_code(poa["POA_CODE_2021"])
            .str.extract(r"(\d{4})", expand=False)
        )

        mb = mb.dropna(
            subset=[
                "MB_CODE_2021",
                "SA2_CODE_2021",
                "SA2_NAME_2021",
                "AREA_ALBERS_SQKM",
            ]
        ).drop_duplicates(subset=["MB_CODE_2021"])
        poa = poa.dropna(subset=["MB_CODE_2021", "POA_CODE_2021"]).drop_duplicates(
            subset=["MB_CODE_2021"]
        )

        joined = poa.merge(mb, on="MB_CODE_2021", how="inner", validate="one_to_one")
        if joined.empty:
            raise ValueError("Mesh Block join produced zero rows")

        unmatched = len(poa) - len(joined)
        if unmatched:
            print(f"         warning: {unmatched:,} POA Mesh Blocks did not match an SA2")

        grouped = (
            joined.groupby(
                ["POA_CODE_2021", "SA2_CODE_2021", "SA2_NAME_2021"],
                as_index=False,
                dropna=False,
            )["AREA_ALBERS_SQKM"]
            .sum()
        )

        totals = grouped.groupby("POA_CODE_2021")["AREA_ALBERS_SQKM"].transform("sum")
        grouped = grouped.loc[totals > 0].copy()
        grouped["RATIO_FROM_TO"] = grouped["AREA_ALBERS_SQKM"] / totals[totals > 0]

        correspondence = grouped[
            ["POA_CODE_2021", "SA2_CODE_2021", "SA2_NAME_2021", "RATIO_FROM_TO"]
        ].sort_values(["POA_CODE_2021", "RATIO_FROM_TO"], ascending=[True, False])

        # Defensive validation: each postcode's shares should total 1.
        ratio_totals = correspondence.groupby("POA_CODE_2021")["RATIO_FROM_TO"].sum()
        max_error = float((ratio_totals - 1.0).abs().max())
        if max_error > 1e-9:
            raise ValueError(
                f"Generated correspondence ratios do not sum to 1; max error={max_error}"
            )

        output.parent.mkdir(parents=True, exist_ok=True)
        correspondence.to_csv(output, index=False)
        print(
            f"  [ok  ] postcode_sa2_correspondence -> "
            f"{output.relative_to(config.PROJECT_ROOT)} "
            f"({len(correspondence):,} POA-SA2 rows, "
            f"{correspondence['POA_CODE_2021'].nunique():,} POAs)"
        )
        return True

    except Exception as error:  # noqa: BLE001 - give user actionable failure
        print(f"  [FAIL] postcode_sa2_correspondence: {error}")
        return False


def main() -> None:
    print(f"Downloading external data into {config.EXTERNAL_DIR}\n")

    failures = [name for name, spec in SOURCES.items() if not download(name, spec)]

    if "meshblock_allocation" not in failures and "poa_allocation" not in failures:
        if not build_postcode_sa2_correspondence():
            failures.append("postcode_sa2_correspondence")

    print("\nManual downloads required (ABS publishes these via Data by Region):")
    for name, spec in MANUAL_SOURCES.items():
        status = "present" if Path(spec["dest"]).exists() else "MISSING"
        print(f"  [{status}] {name}")
        print(f"           {spec['source']}")
        print(f"           Why: {spec['why']}")
        print(f"           Save as: {spec['dest']}")
        print(f"           Needs columns: {spec['needs_columns']}")

    if failures:
        print(f"\n{len(failures)} automated step(s) failed - see messages above.")
        sys.exit(1)

    print("\nDone.")


if __name__ == "__main__":
    main()
