"""Heuristic value scoring for used car listings."""

from datetime import date

import pandas as pd

from cars.competitors import normalize_variant

# Brand reliability scores (out of 100)
BRAND_RELIABILITY = {
    "maruti": 90,
    "toyota": 92,
    "honda": 88,
    "hyundai": 85,
    "tata": 75,
    "mahindra": 78,
    "kia": 82,
    "skoda": 65,
    "volkswagen": 68,
    "ford": 72,
    "renault": 60,
    "nissan": 62,
    "datsun": 55,
    "mg": 70,
    "jeep": 60,
    "fiat": 50,
    "chevrolet": 55,
    "citroen": 58,
}
DEFAULT_RELIABILITY = 60

# Canonicalize brand names (scrapers return inconsistent names across platforms)
MAKE_ALIASES = {
    "maruti suzuki": "maruti",
    "maruti-suzuki": "maruti",
}


def _canonical_make(make: str) -> str:
    """Normalize make name to canonical form."""
    key = make.strip().lower()
    return MAKE_ALIASES.get(key, key)


def _percentile_score(series: pd.Series, lower_is_better: bool = True) -> pd.Series:
    """Convert a series to 0-100 percentile scores.

    lower_is_better=True means the lowest value gets 100.
    """
    ranks = series.rank(pct=True, method="average")
    if lower_is_better:
        return ((1 - ranks) * 100).clip(0, 100)
    return (ranks * 100).clip(0, 100)


def score_cars(df: pd.DataFrame, preferences: dict | None = None) -> pd.DataFrame:
    """Score each car with a composite value score (0-100).

    Components:
        - Segment price percentile (40%): cheapest in (make, model, year) group = best
        - Age-price ratio (20%): newer + cheaper = better
        - KM-price ratio (15%): low km + cheap = better
        - Owner score (10%): fewer owners = better
        - Brand reliability (10%): static lookup table
        - Preference bonus (5%): matches user preferences from config
    """
    if preferences is None:
        preferences = {}

    df = df.copy()
    current_year = date.today().year

    # --- Clean data ---
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["odometer_km"] = pd.to_numeric(df["odometer_km"], errors="coerce")
    df["num_owners"] = pd.to_numeric(df["num_owners"], errors="coerce")
    df = df.dropna(subset=["price", "year"])
    df["car_age"] = (current_year - df["year"]).clip(lower=1)
    df["odometer_km"] = df["odometer_km"].fillna(df["odometer_km"].median())
    df["num_owners"] = df["num_owners"].fillna(2)

    # --- 1. Segment price percentile (40%) ---
    # Group by (make, model, year), rank price within group
    # Canonicalize make to merge variants like "Maruti" / "Maruti Suzuki"
    df["canonical_make"] = df["make"].apply(_canonical_make)
    df["normalized_variant"] = df["variant"].fillna("").apply(normalize_variant)
    df["segment_key"] = df["canonical_make"] + "|" + df["model"].str.strip().str.lower() + "|" + df["normalized_variant"].str.lower() + "|" + df["year"].astype(str)
    segment_size = df.groupby("segment_key")["price"].transform("count")
    segment_rank = df.groupby("segment_key")["price"].rank(pct=True, method="average")
    # Single-car segments: rank(pct=True) returns 1.0 → (1-1)*100 = 0, but the
    # only car in a segment is the cheapest by definition, so give it 100.
    df["score_segment"] = ((1 - segment_rank) * 100).clip(0, 100)
    df.loc[segment_size == 1, "score_segment"] = 100.0

    # --- 2. Age-price ratio (20%) ---
    # price / car_age — lower is better (cheap + new)
    df["age_price_ratio"] = df["price"] / df["car_age"]
    df["score_age_price"] = _percentile_score(df["age_price_ratio"], lower_is_better=True)

    # --- 3. KM-price ratio (15%) ---
    # price / (odometer_km + 1) — lower km + cheaper = better, but we want low km for price
    # Actually: odometer * price — low on both = good
    df["km_price_product"] = df["odometer_km"] * df["price"]
    df["score_km_price"] = _percentile_score(df["km_price_product"], lower_is_better=True)

    # --- 4. Owner score (10%) ---
    owner_map = {1: 100, 2: 60, 3: 30}
    df["score_owners"] = df["num_owners"].map(owner_map).fillna(10)

    # --- 5. Brand reliability (10%) ---
    df["score_brand"] = df["canonical_make"].map(BRAND_RELIABILITY).fillna(DEFAULT_RELIABILITY)

    # --- 6. Preference bonus (5%) ---
    df["score_pref"] = 0.0
    pref_transmission = preferences.get("transmission", "").lower()
    raw_body = preferences.get("body_type", "")
    pref_body_type = [raw_body.lower()] if isinstance(raw_body, str) else [b.lower() for b in raw_body]
    raw_fuel = preferences.get("fuel_type", "")
    pref_fuel_type = [raw_fuel.lower()] if isinstance(raw_fuel, str) else [f.lower() for f in raw_fuel]
    pref_max_owners = preferences.get("max_owners")

    if pref_transmission:
        df.loc[df["transmission"].str.lower() == pref_transmission, "score_pref"] += 40
    if any(pref_body_type):
        df.loc[df["body_type"].str.lower().isin(pref_body_type), "score_pref"] += 30
    if any(pref_fuel_type):
        df.loc[df["fuel_type"].str.lower().isin(pref_fuel_type), "score_pref"] += 20
    if pref_max_owners:
        df.loc[df["num_owners"] <= int(pref_max_owners), "score_pref"] += 10
    # Normalize preference score to 0-100
    max_pref = df["score_pref"].max()
    if max_pref > 0:
        df["score_pref"] = (df["score_pref"] / max_pref) * 100

    # --- Composite score ---
    df["value_score"] = (
        df["score_segment"] * 0.40
        + df["score_age_price"] * 0.20
        + df["score_km_price"] * 0.15
        + df["score_owners"] * 0.10
        + df["score_brand"] * 0.10
        + df["score_pref"] * 0.05
    ).round(2)

    return df


def select_candidates(df: pd.DataFrame, max_candidates: int = 80) -> pd.DataFrame:
    """Select top candidates: top N overall + top 3 per segment for diversity.

    Returns ~max_candidates unique cars sorted by value_score descending.
    """
    # Top 60 overall
    top_overall = df.nlargest(min(60, len(df)), "value_score")

    # Top 3 per body_type
    top_body = df.groupby("body_type", group_keys=False).apply(
        lambda g: g.nlargest(3, "value_score")
    )
    # Top 3 per fuel_type
    top_fuel = df.groupby("fuel_type", group_keys=False).apply(
        lambda g: g.nlargest(3, "value_score")
    )
    # Top 3 per transmission
    top_trans = df.groupby("transmission", group_keys=False).apply(
        lambda g: g.nlargest(3, "value_score")
    )

    # Combine and deduplicate
    combined = pd.concat([top_overall, top_body, top_fuel, top_trans])
    combined = combined.drop_duplicates(subset=["platform_id"])
    combined = combined.nlargest(max_candidates, "value_score")

    return combined.reset_index(drop=True)


def compute_segment_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Compute average price per (make, model, year) segment."""
    return (
        df.groupby(["make", "model", "year"])
        .agg(
            avg_price=("price", "mean"),
            min_price=("price", "min"),
            max_price=("price", "max"),
            count=("price", "count"),
        )
        .reset_index()
        .round(0)
    )
