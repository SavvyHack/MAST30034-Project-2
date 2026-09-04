"""
Parsing the `tags` field on tbl_merchants, and grouping the resulting
categories into business segments.

The raw field is a single string that packs three pieces of information
together with inconsistent formatting. All four of these are real examples:

    ((furniture, home furnishings and equipment shops, ...), (e), (take rate: 0.18))
    ([cable, satellite, and otHer pay television and radio services], [b], [take rate: 4.22])
    [(gift, card, novelty, and souvenir shops), (a), (take rate: 6.34)]
    [[watch, clock, and jewelry repair shops], [c], [take rate: 2.39]]

Brackets are mixed, capitalisation is random, and there is stray whitespace.
Note that the category itself contains commas, so this cannot be split on ",".
We instead match the three *innermost* bracketed groups.
"""

import re

# Matches a bracketed group that contains no further brackets, i.e. the three
# innermost groups: category, revenue level, take rate.
_INNER_GROUP = re.compile(r"[\(\[]([^\(\)\[\]]*)[\)\]]")
_TAKE_RATE = re.compile(r"([0-9]*\.?[0-9]+)")

# The generator introduces stray spaces inside category names that would
# otherwise create duplicate categories. These are cosmetic fixes applied
# after whitespace collapsing.
_CATEGORY_FIXES = {
    r"\s+,": ",",          # "programming , data" -> "programming, data"
    r"\brent al\b": "rental",
}


def _clean_category(text: str) -> str:
    """Lowercase, collapse whitespace, and repair known generator typos."""
    out = re.sub(r"\s+", " ", text).strip().lower()
    for pattern, replacement in _CATEGORY_FIXES.items():
        out = re.sub(pattern, replacement, out)
    return out


def parse_tag(tag: str):
    """
    Split one raw tag string into (category, revenue_level, take_rate).

    Returns (None, None, None) if the string does not contain exactly three
    bracketed groups, so that malformed rows surface as nulls in the output
    rather than raising and killing the pipeline.
    """
    if tag is None:
        return (None, None, None)

    groups = _INNER_GROUP.findall(tag)
    if len(groups) != 3:
        return (None, None, None)

    category = _clean_category(groups[0])

    revenue_level = groups[1].strip().lower()
    if revenue_level not in {"a", "b", "c", "d", "e"}:
        revenue_level = None

    match = _TAKE_RATE.search(groups[2])
    take_rate = float(match.group(1)) if match else None

    return (category, revenue_level, take_rate)


# --- Segmentation ---------------------------------------------------------
# The spec asks for between three and five segments. The 25 raw categories are
# too granular to rank within (some contain a handful of merchants), so they
# are grouped by the kind of purchase a consumer is making. Merchants are
# ranked within these segments because a grocery-style merchant with many small
# baskets should not be compared directly against a jeweller.

SEGMENT_MAP = {
    "Technology & Telecom": [
        "computers, computer peripheral equipment, and software",
        "computer programming, data processing, and integrated systems design services",
        "digital goods: books, movies, music",
        "telecom",
        "cable, satellite, and other pay television and radio services",
    ],
    "Home & Garden": [
        "furniture, home furnishings and equipment shops, and manufacturers, except appliances",
        "lawn and garden supply outlets, including nurseries",
        "florists supplies, nursery stock, and flowers",
        "equipment, tool, furniture, and appliance rental and leasing",
        "tent and awning shops",
    ],
    "Luxury & Personal Care": [
        "jewelry, watch, clock, and silverware shops",
        "watch, clock, and jewelry repair shops",
        "opticians, optical goods, and eyeglasses",
        "health and beauty spas",
        "shoe shops",
    ],
    "Hobby & Recreation": [
        "books, periodicals, and newspapers",
        "music shops - musical instruments, pianos, and sheet music",
        "hobby, toy and game shops",
        "artist supply and craft shops",
        "bicycle shops - sales and service",
        "antique shops - sales, repairs, and restoration services",
    ],
    "Speciality Retail": [
        "art dealers and galleries",
        "gift, card, novelty, and souvenir shops",
        "stationery, office supplies and printing and writing paper",
        "motor vehicle supplies and new parts",
    ],
}

# Inverted for lookup: category -> segment
CATEGORY_TO_SEGMENT = {
    category: segment
    for segment, categories in SEGMENT_MAP.items()
    for category in categories
}


def assign_segment(category: str) -> str:
    """Map a cleaned category to its business segment."""
    if category is None:
        return "Unknown"
    return CATEGORY_TO_SEGMENT.get(category, "Unknown")
