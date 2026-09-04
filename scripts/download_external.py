"""
Download the external ABS datasets the project depends on.

Why a script and not a manual download: the repository has to be reproducible
by someone who clones it and runs it. A tutor should not have to guess which
of the ABS's many similarly-named files was used. Every source is named here
with its URL and the reason it was chosen.

Caveat, and it is a real one: the ABS reorganises its download URLs regularly
and does not offer a stable API for these files. If a download 404s, the
script prints the landing page to visit and the exact filename to save. That
is a deliberate design choice - failing loudly with instructions beats silently
falling back to a stale cached copy.

Run:  python scripts/download_external.py
"""

import io
import sys
import zipfile
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config

TIMEOUT = 120
HEADERS = {"User-Agent": "MAST30034-student-project"}

# Each entry: what it is, where it comes from, and what to do if the URL breaks.
SOURCES = {
    "postcode_sa2_correspondence": {
        "url": (
            "https://www.abs.gov.au/statistics/standards/"
            "australian-statistical-geography-standard-asgs-edition-3/"
            "jul2021-jun2026/access-and-downloads/correspondences/"
            "CG_POA_2021_SA2_2021.xlsx"
        ),
        "dest": config.EXTERNAL_DIR / "postcode_2021_sa2_2021.xlsx",
        "why": "Maps consumer postcodes onto SA2s so ABS data can be joined.",
        "landing": (
            "https://www.abs.gov.au/statistics/standards/"
            "australian-statistical-geography-standard-asgs-edition-3/"
            "jul2021-jun2026/access-and-downloads/correspondences"
        ),
    },
    "sa2_boundaries": {
        "url": (
            "https://www.abs.gov.au/statistics/standards/"
            "australian-statistical-geography-standard-asgs-edition-3/"
            "jul2021-jun2026/access-and-downloads/digital-boundary-files/"
            "SA2_2021_AUST_SHP_GDA2020.zip"
        ),
        "dest": config.EXTERNAL_DIR / "SA2_2021_AUST_SHP_GDA2020.zip",
        "unzip": True,
        "why": "SA2 polygons for the geospatial visualisations.",
        "landing": (
            "https://www.abs.gov.au/statistics/standards/"
            "australian-statistical-geography-standard-asgs-edition-3/"
            "jul2021-jun2026/access-and-downloads/digital-boundary-files"
        ),
    },
}

# These two are published through the ABS Data by Region tool rather than as a
# direct file link, so they are documented for manual download instead of
# being fetched. Data by Region:
#   https://www.abs.gov.au/statistics/people/people-and-communities/data-region
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


def download(name: str, spec: dict) -> bool:
    dest = Path(spec["dest"])
    if dest.exists():
        print(f"  [skip] {name}: already at {dest.relative_to(config.PROJECT_ROOT)}")
        return True

    print(f"  [get ] {name} <- {spec['url']}")
    try:
        response = requests.get(spec["url"], timeout=TIMEOUT, headers=HEADERS)
        response.raise_for_status()
    except Exception as error:  # noqa: BLE001 - we want the reason printed
        print(f"  [FAIL] {name}: {error}")
        print(f"         Download manually from: {spec['landing']}")
        print(f"         Save as: {dest}")
        return False

    dest.write_bytes(response.content)
    print(f"  [ok  ] {name} -> {dest.relative_to(config.PROJECT_ROOT)}")

    if spec.get("unzip"):
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            archive.extractall(config.EXTERNAL_DIR)
        print("         extracted archive")
    return True


def main():
    print(f"Downloading external data into {config.EXTERNAL_DIR}\n")

    failures = [name for name, spec in SOURCES.items() if not download(name, spec)]

    print("\nManual downloads required (ABS publishes these via Data by Region):")
    for name, spec in MANUAL_SOURCES.items():
        status = "present" if Path(spec["dest"]).exists() else "MISSING"
        print(f"  [{status}] {name}")
        print(f"           {spec['source']}")
        print(f"           Why: {spec['why']}")
        print(f"           Save as: {spec['dest']}")
        print(f"           Needs columns: {spec['needs_columns']}")

    if failures:
        print(f"\n{len(failures)} automated download(s) failed - see messages above.")
        sys.exit(1)
    print("\nDone.")


if __name__ == "__main__":
    main()
