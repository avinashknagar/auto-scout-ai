"""OpenAI API wrapper and prompt templates for car value analysis."""

import json
import logging
import os

import pandas as pd
from openai import OpenAI

logger = logging.getLogger(__name__)


def _build_system_prompt() -> str:
    return (
        "You are an expert used car market analyst in India. "
        "You evaluate used car listings for value-for-money, considering price, "
        "age, mileage, brand reliability, ownership history, and market conditions. "
        "You give practical, actionable advice for budget-conscious buyers. "
        "Always respond with valid JSON."
    )


def _format_candidates(df: pd.DataFrame) -> str:
    """Format candidate cars as compact pipe-delimited text."""
    cols = [
        "platform", "make", "model", "variant", "year", "price",
        "transmission", "fuel_type", "body_type", "odometer_km",
        "num_owners", "city", "value_score",
        "competitive_segment", "competitive_tier",
        "listing_url",
    ]
    available_cols = [c for c in cols if c in df.columns]
    lines = ["|".join(available_cols)]
    for _, row in df.iterrows():
        vals = [str(row.get(c, "")) for c in available_cols]
        lines.append("|".join(vals))
    return "\n".join(lines)


def _format_segment_averages(seg_df: pd.DataFrame) -> str:
    """Format segment averages as compact text."""
    lines = ["make|model|year|avg_price|min_price|max_price|count"]
    for _, row in seg_df.iterrows():
        lines.append(
            f"{row['make']}|{row['model']}|{int(row['year'])}|"
            f"{int(row['avg_price'])}|{int(row['min_price'])}|"
            f"{int(row['max_price'])}|{int(row['count'])}"
        )
    return "\n".join(lines)


def _build_user_prompt(
    candidates_df: pd.DataFrame,
    segment_averages: pd.DataFrame,
    total_listings: int,
    price_range: tuple,
    top_deals_count: int,
    preferences: dict | None = None,
    competitive_context: str = "",
) -> str:
    candidates_text = _format_candidates(candidates_df)
    segments_text = _format_segment_averages(segment_averages)

    prefs_text = "No specific preferences."
    if preferences:
        prefs_parts = []
        for k, v in preferences.items():
            if v is not None and v != "":
                prefs_parts.append(f"- {k}: {v}")
        if prefs_parts:
            prefs_text = "\n".join(prefs_parts)

    competitive_section = ""
    if competitive_context:
        competitive_section = f"""
## Competitive Landscape

Each candidate belongs to a competitive segment (models buyers cross-shop) and a feature
tier (base/mid/top based on equipment level). Cars in the same segment + tier + year are
direct alternatives. Use this to judge whether a price is truly good vs cross-model options.

{competitive_context}
"""

    return f"""Analyze these used car listings from the Delhi-NCR market and identify the best value deals.

## Market Context
- Total listings in dataset: {total_listings}
- Price range: Rs {price_range[0]:,} - Rs {price_range[1]:,}
- All prices in Indian Rupees (Rs)
- Data scraped from Cars24 and Spinny platforms

## Segment Averages (avg price per make/model/year)
{segments_text}
{competitive_section}
## Top {len(candidates_df)} Candidates (pre-filtered by heuristic scoring)
{candidates_text}

## Buyer Preferences
{prefs_text}

## Instructions
Consider BOTH the segment average (same make/model/variant/year) AND the competitive landscape (cross-model alternatives at the same feature tier). A car priced below its own model average but ABOVE its competitive set average may not be a true bargain.
Return a JSON object with exactly this structure:
{{
  "top_deals": [
    {{
      "rank": 1,
      "platform": "cars24 or spinny",
      "make": "...",
      "model": "...",
      "variant": "...",
      "year": 2020,
      "price": 350000,
      "listing_url": "...",
      "value_rating": 9.2,
      "reasoning": "One sentence why this is a good deal",
      "risk_factors": "One sentence about potential concerns",
      "estimated_fair_price": 400000,
      "savings_vs_market": 50000
    }}
  ],
  "segment_picks": {{
    "best_hatchback": {{"make": "...", "model": "...", "year": 2020, "price": 300000, "reason": "..."}},
    "best_suv": {{"make": "...", "model": "...", "year": 2020, "price": 450000, "reason": "..."}},
    "best_automatic_under_5L": {{"make": "...", "model": "...", "year": 2020, "price": 480000, "reason": "..."}},
    "best_first_car": {{"make": "...", "model": "...", "year": 2020, "price": 250000, "reason": "..."}}
  }},
  "market_insights": "A paragraph summarizing the current market conditions, pricing trends, and general advice for buyers in this price segment."
}}

Return exactly {top_deals_count} cars in top_deals, ranked by value (best first).
For segment_picks, pick the single best car for each category from the candidates. If no car fits a category, set it to null.
value_rating is 1-10 (10 = incredible deal). estimated_fair_price is what you think the car should cost based on market data. savings_vs_market = estimated_fair_price - actual_price."""


def analyze_with_llm(
    candidates_df: pd.DataFrame,
    segment_averages: pd.DataFrame,
    total_listings: int,
    price_range: tuple,
    config: dict,
    competitive_context: str = "",
) -> dict:
    """Send candidates to LLM for expert analysis.

    Supports both standard models (gpt-4o) and reasoning models (gpt-5.2).
    Returns parsed JSON response dict.
    """
    analyze_config = config.get("analyze", {})
    model = analyze_config.get("model", "gpt-5.2")
    max_tokens = analyze_config.get("max_response_tokens", 4096)
    top_deals_count = analyze_config.get("top_deals_count", 15)
    # Preferences come from central filters section
    filters = config.get("filters", {})
    preferences = {}
    for key in ("transmission", "fuel_type", "body_type", "max_owners"):
        if filters.get(key) is not None:
            preferences[key] = filters[key]
    custom_note = analyze_config.get("custom_note")
    if custom_note:
        preferences["custom_note"] = custom_note
    reasoning_effort = analyze_config.get("reasoning_effort")
    temperature = analyze_config.get("temperature")

    api_key = os.environ.get("OPENAI_API_KEY") or analyze_config.get("api_key")
    if not api_key:
        raise ValueError(
            "OpenAI API key not found. Set OPENAI_API_KEY env var or add "
            "api_key under 'analyze' in config.yaml"
        )

    client = OpenAI(api_key=api_key)

    user_prompt = _build_user_prompt(
        candidates_df=candidates_df,
        segment_averages=segment_averages,
        total_listings=total_listings,
        price_range=price_range,
        top_deals_count=top_deals_count,
        preferences=preferences,
        competitive_context=competitive_context,
    )

    logger.info(f"Sending {len(candidates_df)} candidates to {model}")
    logger.info(f"Prompt length: ~{len(user_prompt)} chars")

    # Build API kwargs — reasoning models don't support temperature
    api_kwargs = {
        "model": model,
        "messages": [
            {"role": "system", "content": _build_system_prompt()},
            {"role": "user", "content": user_prompt},
        ],
        "max_completion_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }

    if reasoning_effort:
        # Pass as extra_body for SDK versions that don't have native support
        api_kwargs.setdefault("extra_body", {})
        api_kwargs["extra_body"]["reasoning_effort"] = reasoning_effort
    if temperature is not None and not reasoning_effort:
        api_kwargs["temperature"] = temperature

    response = client.chat.completions.create(**api_kwargs)

    result_text = response.choices[0].message.content or ""
    finish_reason = response.choices[0].finish_reason
    usage = response.usage

    if finish_reason == "length":
        logger.warning("Response truncated — hit max_completion_tokens limit")
        raise ValueError(
            f"LLM response was truncated ({usage.completion_tokens} tokens). "
            f"Increase max_response_tokens in config.yaml (currently {max_tokens})."
        )
    logger.info(
        f"LLM response: {usage.prompt_tokens} input tokens, "
        f"{usage.completion_tokens} output tokens"
    )

    # GPT-5.2 pricing: $2/M input, $8/M output (approximate)
    estimated_cost = (usage.prompt_tokens * 2 / 1_000_000) + (
        usage.completion_tokens * 8 / 1_000_000
    )
    logger.info(f"Estimated cost: ${estimated_cost:.4f}")

    result = json.loads(result_text)
    result["_usage"] = {
        "prompt_tokens": usage.prompt_tokens,
        "completion_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
        "estimated_cost_usd": round(estimated_cost, 4),
    }
    return result
