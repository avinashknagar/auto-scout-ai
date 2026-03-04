"""Competitive landscape analysis — cross-model variant comparison."""

import logging
import os
import re

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Words to strip from variant strings (engine specs, fuel, tech branding)
_STRIP_PATTERNS = re.compile(
    r"\b("
    r"1\.\d|0\.\d|"  # engine displacement like 1.2, 1.0
    r"PETROL|DIESEL|CNG|ELECTRIC|HYBRID|"
    r"KAPPA|VTVT|I-VTEC|IVTEC|REVOTRON|REVOTORQ|DUALJET|"
    r"K-SERIES|K SERIES|MULTIJET|DDIS|CRDI|DOHC|BSIV|BSVI|BS6|BS4|"
    r"TURBO|TURBOCHARGED|DT|DUAL TONE|"
    r"OPTIONAL|OPT|OPTION PACK|"
    r"STD|STANDARD"
    r")\b",
    re.IGNORECASE,
)

# Keep transmission markers
_TRANSMISSION_MARKERS = {"AMT", "CVT", "AT", "DCT", "IMT"}


def normalize_variant(variant: str) -> str:
    """Strip engine/fuel/tech words from variant, keep trim name and transmission."""
    if not variant or not isinstance(variant, str):
        return ""
    cleaned = _STRIP_PATTERNS.sub("", variant.upper())
    # Collapse multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def load_competitive_config() -> dict:
    """Load competitive_segments.yaml and competitive_variants.yaml.

    Returns dict with keys:
        - model_to_segment: {lowered_model: segment_name}
        - trim_to_tier: {(segment, lowered_make, lowered_model, uppered_trim): tier_name}
        - segment_models: {segment_name: [model_names]}
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    segments_path = os.path.join(project_root, "competitive_segments.yaml")
    variants_path = os.path.join(project_root, "competitive_variants.yaml")

    with open(segments_path) as f:
        seg_data = yaml.safe_load(f)
    with open(variants_path) as f:
        var_data = yaml.safe_load(f)

    # Build model_to_segment: every model name (lowered) -> segment
    model_to_segment = {}
    segment_models = {}
    for seg_name, seg_info in seg_data.get("segments", {}).items():
        models = seg_info.get("models", [])
        segment_models[seg_name] = [m.lower() for m in models]
        for model in models:
            model_to_segment[model.lower()] = seg_name

    # Build trim_to_tier from competitive_variants.yaml
    trim_to_tier = {}
    for seg_name, tiers in var_data.items():
        if not isinstance(tiers, dict):
            continue
        for tier_name, tier_info in tiers.items():
            if not isinstance(tier_info, dict):
                continue
            for entry in tier_info.get("variants", []):
                make = entry.get("make", "").lower()
                model = entry.get("model", "").lower()
                for trim in entry.get("trims", []):
                    key = (seg_name, make, model, trim.upper())
                    trim_to_tier[key] = tier_name

    logger.info(
        f"Competitive config loaded: {len(model_to_segment)} models, "
        f"{len(trim_to_tier)} trim mappings across {len(segment_models)} segments"
    )
    return {
        "model_to_segment": model_to_segment,
        "trim_to_tier": trim_to_tier,
        "segment_models": segment_models,
    }


def _fuzzy_model_match(model: str, config: dict) -> str | None:
    """Try to find segment for a model using fuzzy matching.

    Handles aliases like 'New Wagon-R' -> 'Wagon R', 'WagonR' -> 'Wagon R'.
    """
    model_lower = model.lower()

    # Exact match first
    if model_lower in config["model_to_segment"]:
        return config["model_to_segment"][model_lower]

    # Normalize: strip 'new ', replace hyphens with spaces
    normalized = model_lower.replace("-", " ").replace("new ", "")
    if normalized in config["model_to_segment"]:
        return config["model_to_segment"][normalized]

    # Try without spaces (wagonr -> wagon r)
    no_spaces = model_lower.replace(" ", "").replace("-", "")
    for known_model, seg in config["model_to_segment"].items():
        known_no_spaces = known_model.replace(" ", "").replace("-", "")
        if no_spaces == known_no_spaces:
            return seg

    # Substring match: check if model contains a known model name or vice versa
    for known_model, seg in config["model_to_segment"].items():
        if known_model in model_lower or model_lower in known_model:
            return seg

    return None


def classify_car(
    model: str, make: str, variant: str, config: dict
) -> tuple[str | None, str | None]:
    """Classify a car into (segment_name, tier_name).

    Tier lookup strategy (in order):
    1. Exact normalized variant match
    2. Without parenthesized content
    3. First 2 tokens
    4. First token
    5. Default to tier_2 if segment found
    """
    segment = _fuzzy_model_match(model, config)
    if segment is None:
        return (None, None)

    norm = normalize_variant(variant)
    make_lower = make.strip().lower()
    # Canonicalize make for lookup
    if make_lower in ("maruti suzuki", "maruti-suzuki"):
        make_lower = "maruti"

    # Find the model key that matched in the segment
    model_lower = model.strip().lower()
    # Try to find exact model in trim_to_tier keys
    candidate_models = set()
    for (seg, mk, md, _tr) in config["trim_to_tier"]:
        if seg == segment and mk == make_lower:
            candidate_models.add(md)

    # Pick best matching model from candidates
    matched_model = None
    if model_lower in candidate_models:
        matched_model = model_lower
    else:
        normalized_model = model_lower.replace("-", " ").replace("new ", "")
        for cm in candidate_models:
            if cm == normalized_model:
                matched_model = cm
                break
            cm_no_spaces = cm.replace(" ", "").replace("-", "")
            model_no_spaces = model_lower.replace(" ", "").replace("-", "")
            if cm_no_spaces == model_no_spaces:
                matched_model = cm
                break
            if cm in model_lower or model_lower in cm:
                matched_model = cm
                break

    if matched_model is None and candidate_models:
        # Last resort: pick first candidate model in this segment for this make
        matched_model = next(iter(candidate_models))

    if matched_model is None:
        return (segment, "tier_2")

    # Try tier lookup with various trim normalization strategies
    def _try_tier(trim_str: str) -> str | None:
        key = (segment, make_lower, matched_model, trim_str.upper())
        return config["trim_to_tier"].get(key)

    # Strategy 1: exact normalized variant
    tier = _try_tier(norm)
    if tier:
        return (segment, tier)

    # Strategy 2: without parenthesized content e.g. "VXI(O)" -> "VXI"
    no_parens = re.sub(r"\([^)]*\)", "", norm).strip()
    if no_parens != norm:
        tier = _try_tier(no_parens)
        if tier:
            return (segment, tier)

    # Strategy 3: first 2 tokens
    tokens = norm.split()
    if len(tokens) >= 2:
        tier = _try_tier(" ".join(tokens[:2]))
        if tier:
            return (segment, tier)

    # Strategy 4: first token only
    if tokens:
        tier = _try_tier(tokens[0])
        if tier:
            return (segment, tier)

    # Default: mid tier if segment found
    return (segment, "tier_2")


def build_competitive_sets(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Enrich df with competitive_segment and competitive_tier, then build competitive sets.

    Returns a DataFrame with one row per (segment, tier, year) group containing:
    segment, tier, year, avg_price, min_price, cheapest_car, max_price, count, models
    """
    # Classify each car
    segments = []
    tiers = []
    for _, row in df.iterrows():
        seg, tier = classify_car(
            model=str(row.get("model", "")),
            make=str(row.get("make", "")),
            variant=str(row.get("variant", "")),
            config=config,
        )
        segments.append(seg)
        tiers.append(tier)

    df = df.copy()
    df["competitive_segment"] = segments
    df["competitive_tier"] = tiers

    # Filter to cars with segment assignment
    classified = df.dropna(subset=["competitive_segment"])
    if classified.empty:
        logger.warning("No cars classified into competitive segments")
        return pd.DataFrame()

    # Group by (segment, tier, year)
    groups = []
    for (seg, tier, year), group in classified.groupby(
        ["competitive_segment", "competitive_tier", "year"]
    ):
        if len(group) < 2:
            continue
        prices = group["price"]
        cheapest_idx = prices.idxmin()
        cheapest_row = group.loc[cheapest_idx]
        cheapest_car = f"{cheapest_row['make']} {cheapest_row['model']}"
        distinct_models = sorted(group.apply(
            lambda r: f"{r['make']} {r['model']}", axis=1
        ).unique())
        groups.append({
            "segment": seg,
            "tier": tier,
            "year": int(year),
            "avg_price": int(prices.mean()),
            "min_price": int(prices.min()),
            "cheapest_car": cheapest_car,
            "max_price": int(prices.max()),
            "count": len(group),
            "models": ", ".join(distinct_models),
        })

    if not groups:
        logger.warning("No competitive sets with 2+ cars found")
        return pd.DataFrame()

    sets_df = pd.DataFrame(groups)
    logger.info(f"Built {len(sets_df)} competitive sets")
    return sets_df


def format_competitive_context(sets_df: pd.DataFrame) -> str:
    """Format competitive sets as pipe-delimited table for LLM prompt."""
    if sets_df.empty:
        return ""

    lines = ["segment|tier|year|avg_price|min_price|cheapest_car|max_price|count|models"]
    for _, row in sets_df.iterrows():
        lines.append(
            f"{row['segment']}|{row['tier']}|{row['year']}|"
            f"{row['avg_price']}|{row['min_price']}|{row['cheapest_car']}|"
            f"{row['max_price']}|{row['count']}|{row['models']}"
        )
    return "\n".join(lines)
