"""Orchestrator: scoring -> LLM -> output."""

import logging
import os

import pandas as pd

from cars.competitors import (
    build_competitive_sets,
    classify_car,
    format_competitive_context,
    load_competitive_config,
)
from cars.llm_client import analyze_with_llm
from cars.report_writer import write_markdown_report, write_ranked_csv, print_terminal_summary
from cars.scoring import compute_segment_averages, score_cars, select_candidates

logger = logging.getLogger(__name__)


def _apply_hard_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Apply hard filters from the central `filters` config section.

    These are the same filters used at scrape time, reapplied here so the
    analyzer honors them even on stale CSV data.
    """
    if not filters:
        return df

    before = len(df)
    applied = []

    max_price = filters.get("max_price")
    if max_price is not None:
        df = df[pd.to_numeric(df["price"], errors="coerce") <= max_price]
        applied.append(f"price <= Rs {max_price:,}")

    min_year = filters.get("min_year")
    if min_year is not None:
        df = df[pd.to_numeric(df["year"], errors="coerce") >= min_year]
        applied.append(f"year >= {min_year}")

    max_owners = filters.get("max_owners")
    if max_owners is not None:
        owners = pd.to_numeric(df["num_owners"], errors="coerce")
        df = df[owners.isna() | (owners <= max_owners)]
        applied.append(f"owners <= {max_owners}")

    transmission = filters.get("transmission")
    if transmission is not None:
        df = df[df["transmission"].str.lower() == transmission.lower()]
        applied.append(f"transmission = {transmission}")

    fuel_type = filters.get("fuel_type")
    if fuel_type is not None:
        allowed = [fuel_type] if isinstance(fuel_type, str) else fuel_type
        allowed_lower = [f.lower() for f in allowed]
        df = df[df["fuel_type"].str.lower().isin(allowed_lower)]
        applied.append(f"fuel = {allowed}")

    body_type = filters.get("body_type")
    if body_type is not None:
        allowed = [body_type] if isinstance(body_type, str) else body_type
        allowed_lower = [b.lower() for b in allowed]
        df = df[df["body_type"].str.lower().isin(allowed_lower)]
        applied.append(f"body = {allowed}")

    max_odometer = filters.get("max_odometer_km")
    if max_odometer is not None:
        odo = pd.to_numeric(df["odometer_km"], errors="coerce")
        df = df[odo.isna() | (odo <= max_odometer)]
        applied.append(f"km <= {max_odometer:,}")

    dropped = before - len(df)
    if applied:
        logger.info(f"Hard filters [{', '.join(applied)}]: {before} → {len(df)} ({dropped} dropped)")

    return df


def _resolve_csv_path(config: dict) -> str:
    """Find the latest CSV data file."""
    db_path = config["database"]["path"]
    data_dir = os.path.dirname(db_path)
    csv_path = os.path.join(data_dir, "cars_latest.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Data file not found: {csv_path}\n"
            "Run 'python run.py scrape' first to collect car data."
        )
    return csv_path


def run_analysis(
    config: dict,
    dry_run: bool = False,
    preference_overrides: list[str] | None = None,
    top_count: int | None = None,
    platform: str = "all",
) -> dict:
    """Run the full two-stage analysis pipeline.

    Stage 1: Heuristic scoring (always runs, free)
    Stage 2: LLM analysis (skipped if dry_run=True)

    Returns dict with results summary.
    """
    analyze_config = config.get("analyze", {})
    max_candidates = analyze_config.get("max_candidates", 80)
    output_dir = analyze_config.get("output_dir", "data/analysis")
    filters = config.get("filters", {})

    # Build scoring preferences from the central filters section
    preferences = {}
    for key in ("transmission", "fuel_type", "body_type", "max_owners"):
        if filters.get(key) is not None:
            preferences[key] = filters[key]
    custom_note = analyze_config.get("custom_note")
    if custom_note:
        preferences["custom_note"] = custom_note

    if top_count:
        analyze_config["top_deals_count"] = top_count

    # Apply CLI preference overrides (these also update the central filters for this run)
    if preference_overrides:
        for pref in preference_overrides:
            pref_lower = pref.lower()
            if pref_lower in ("automatic", "manual"):
                preferences["transmission"] = pref_lower
                filters["transmission"] = pref_lower
            elif pref_lower in ("petrol", "diesel", "cng", "electric"):
                preferences["fuel_type"] = pref_lower
                filters["fuel_type"] = pref_lower
            elif pref_lower in ("suv", "hatchback", "sedan", "muv", "mpv"):
                preferences["body_type"] = pref_lower
                filters["body_type"] = pref_lower
            else:
                existing = preferences.get("custom_note", "")
                preferences["custom_note"] = f"{existing} {pref}".strip()

    # Resolve output dir relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(project_root, output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # --- Load data ---
    csv_path = _resolve_csv_path(config)
    df = pd.read_csv(csv_path)
    total_listings = len(df)
    logger.info(f"Loaded {total_listings} listings from {csv_path}")

    # Apply platform filter
    if platform and platform != "all":
        df = df[df["platform"] == platform]
        logger.info(f"Filtered to platform '{platform}': {len(df)} listings")

    # Apply hard filters from central `filters` section (same as scrape-time filters)
    filters = config.get("filters", {})
    df = _apply_hard_filters(df, filters)

    price_range = (int(df["price"].min()), int(df["price"].max()))

    # --- Stage 1: Heuristic Scoring ---
    print(f"\n--- Stage 1: Heuristic Scoring ---")
    print(f"  Listings loaded: {total_listings}")
    print(f"  Price range: Rs {price_range[0]:,} - Rs {price_range[1]:,}")

    scored_df = score_cars(df, preferences)
    candidates = select_candidates(scored_df, max_candidates)
    segment_averages = compute_segment_averages(scored_df)

    # --- Competitive landscape ---
    try:
        comp_config = load_competitive_config()
        comp_sets_df = build_competitive_sets(scored_df, comp_config)
        competitive_context = format_competitive_context(comp_sets_df)
        # Enrich candidates with segment/tier columns
        seg_list, tier_list = [], []
        for _, row in candidates.iterrows():
            seg, tier = classify_car(
                model=str(row.get("model", "")),
                make=str(row.get("make", "")),
                variant=str(row.get("variant", "")),
                config=comp_config,
            )
            seg_list.append(seg or "")
            tier_list.append(tier or "")
        candidates["competitive_segment"] = seg_list
        candidates["competitive_tier"] = tier_list
        comp_sets_count = len(comp_sets_df) if not comp_sets_df.empty else 0
        print(f"  Competitive sets: {comp_sets_count}")
    except Exception as e:
        logger.warning(f"Competitive analysis failed, continuing without: {e}")
        competitive_context = ""
        candidates["competitive_segment"] = ""
        candidates["competitive_tier"] = ""

    print(f"  Candidates selected: {len(candidates)}")
    print(f"  Score range: {candidates['value_score'].min():.1f} - {candidates['value_score'].max():.1f}")

    if dry_run:
        print(f"\n--- Dry Run: Top 15 by Heuristic Score ---")
        top = candidates.head(15)
        for i, (_, row) in enumerate(top.iterrows(), 1):
            print(
                f"  {i:2d}. [{row['value_score']:.1f}] {row['year']} {row['make']} {row['model']} "
                f"{row.get('variant', '')} - Rs {int(row['price']):,} | "
                f"{int(row.get('odometer_km', 0)):,} km | "
                f"{row.get('transmission', '')} | {row.get('fuel_type', '')} | "
                f"Owner#{int(row.get('num_owners', 0))}"
            )
        print(f"\n  (--dry-run: skipping LLM analysis, no API cost)")

        # Still write ranked CSV for dry run
        from datetime import date
        today = date.today().isoformat()
        file_tag = f"{today}_{platform}" if platform != "all" else today
        csv_out = write_ranked_csv(candidates, output_dir, file_tag)
        print(f"  Ranked CSV: {csv_out}")

        return {
            "stage": "heuristic_only",
            "total_listings": total_listings,
            "candidates": len(candidates),
            "output_csv": csv_out,
        }

    # --- Stage 2: LLM Analysis ---
    print(f"\n--- Stage 2: LLM Analysis ---")
    top_deals_count = analyze_config.get("top_deals_count", 15)
    print(f"  Sending {len(candidates)} candidates to {analyze_config.get('model', 'gpt-4o')}...")

    llm_result = analyze_with_llm(
        candidates_df=candidates,
        segment_averages=segment_averages,
        total_listings=total_listings,
        price_range=price_range,
        config=config,
        competitive_context=competitive_context,
    )

    usage = llm_result.get("_usage", {})
    print(f"  Tokens: {usage.get('prompt_tokens', '?')} in / {usage.get('completion_tokens', '?')} out")
    print(f"  Estimated cost: ${usage.get('estimated_cost_usd', '?')}")

    # --- Validate LLM deals against actual candidates ---
    valid_urls = set(candidates["listing_url"].dropna().tolist())
    raw_deals = llm_result.get("top_deals", [])
    validated_deals = []
    for deal in raw_deals:
        url = deal.get("listing_url", "")
        if url and url in valid_urls:
            validated_deals.append(deal)
        else:
            logger.warning(f"Deal filtered: listing_url not in candidates: {deal.get('make', '?')} {deal.get('model', '?')} — {url}")
    if len(validated_deals) < len(raw_deals):
        print(f"  Deals validated: {len(validated_deals)}/{len(raw_deals)} (filtered {len(raw_deals) - len(validated_deals)} with invalid URLs)")
    llm_result["top_deals"] = validated_deals

    # --- Validate segment_picks against candidates ---
    candidate_keys = set()
    for _, row in candidates.iterrows():
        key = f"{row.get('make', '')}|{row.get('model', '')}|{row.get('year', '')}|{row.get('price', '')}".lower()
        candidate_keys.add(key)
        # Also add by URL for flexible matching
        url = row.get("listing_url", "")
        if url:
            candidate_keys.add(url)

    raw_picks = llm_result.get("segment_picks", {})
    for category, pick in raw_picks.items():
        if pick is None:
            continue
        pick_key = f"{pick.get('make', '')}|{pick.get('model', '')}|{pick.get('year', '')}|{pick.get('price', '')}".lower()
        pick_url = pick.get("listing_url", "")
        if pick_key not in candidate_keys and pick_url not in candidate_keys:
            logger.warning(f"Segment pick '{category}' not in candidates: {pick.get('make', '?')} {pick.get('model', '?')} {pick.get('year', '?')}")
            raw_picks[category] = None
    llm_result["segment_picks"] = raw_picks

    # --- Output ---
    from datetime import date
    today = date.today().isoformat()
    file_tag = f"{today}_{platform}" if platform != "all" else today

    print_terminal_summary(llm_result, top_deals_count)
    md_path = write_markdown_report(llm_result, candidates, total_listings, price_range, output_dir, file_tag)
    csv_out = write_ranked_csv(candidates, output_dir, file_tag)

    print(f"\n--- Output Files ---")
    print(f"  Report:     {md_path}")
    print(f"  Ranked CSV: {csv_out}")

    return {
        "stage": "full",
        "total_listings": total_listings,
        "candidates": len(candidates),
        "top_deals": len(llm_result.get("top_deals", [])),
        "output_report": md_path,
        "output_csv": csv_out,
        "cost_usd": usage.get("estimated_cost_usd"),
    }
