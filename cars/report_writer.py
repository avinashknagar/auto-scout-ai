"""Output generation: terminal summary, markdown report, ranked CSV."""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def print_terminal_summary(llm_result: dict, top_count: int = 15):
    """Print top deals to terminal."""
    deals = llm_result.get("top_deals", [])[:top_count]
    if not deals:
        print("\n  No deals returned by LLM.")
        return

    print(f"\n{'=' * 70}")
    print(f"  TOP {len(deals)} VALUE DEALS")
    print(f"{'=' * 70}")

    for deal in deals:
        rank = deal.get("rank", "?")
        rating = deal.get("value_rating", "?")
        make = deal.get("make", "?")
        model = deal.get("model", "?")
        variant = deal.get("variant", "")
        year = deal.get("year", "?")
        price = deal.get("price", 0)
        savings = deal.get("savings_vs_market", 0)
        reasoning = deal.get("reasoning", "")
        url = deal.get("listing_url", "")

        print(f"\n  #{rank} [{rating}/10] {year} {make} {model} {variant}")
        print(f"     Price: Rs {price:,}  |  Save: Rs {savings:,} vs market")
        print(f"     {reasoning}")
        if url:
            print(f"     {url}")

    # Segment picks
    picks = llm_result.get("segment_picks", {})
    if picks:
        print(f"\n{'=' * 70}")
        print(f"  SEGMENT PICKS")
        print(f"{'=' * 70}")
        for category, pick in picks.items():
            label = category.replace("_", " ").title()
            if pick is None:
                print(f"\n  {label}: No suitable candidate")
                continue
            print(
                f"\n  {label}: {pick.get('year', '?')} {pick.get('make', '?')} "
                f"{pick.get('model', '?')} - Rs {pick.get('price', 0):,}"
            )
            print(f"     {pick.get('reason', '')}")

    # Market insights
    insights = llm_result.get("market_insights", "")
    if insights:
        print(f"\n{'=' * 70}")
        print(f"  MARKET INSIGHTS")
        print(f"{'=' * 70}")
        # Wrap text at ~68 chars
        words = insights.split()
        line = "  "
        for word in words:
            if len(line) + len(word) + 1 > 70:
                print(line)
                line = "  " + word
            else:
                line += " " + word if line.strip() else "  " + word
        if line.strip():
            print(line)

    print(f"\n{'=' * 70}")


def write_markdown_report(
    llm_result: dict,
    candidates: pd.DataFrame,
    total_listings: int,
    price_range: tuple,
    output_dir: str,
    today: str,
) -> str:
    """Write full markdown analysis report.

    Deals are pre-validated by analyzer (listing_url checked against candidates).
    All LLM fields accessed via .get() with safe defaults.
    """
    valid_urls = set(candidates["listing_url"].dropna().tolist()) if "listing_url" in candidates.columns else set()

    lines = []
    lines.append(f"# Car Value Analysis Report — {today}")
    lines.append("")
    lines.append(f"**Total listings analyzed:** {total_listings}  ")
    lines.append(f"**Price range:** Rs {price_range[0]:,} – Rs {price_range[1]:,}  ")
    lines.append(f"**Candidates scored:** {len(candidates)}  ")

    usage = llm_result.get("_usage", {})
    if usage:
        lines.append(f"**LLM tokens:** {usage.get('total_tokens', '?')} (cost: ${usage.get('estimated_cost_usd', '?')})  ")
    lines.append("")

    # Top deals (filter any that slipped through without valid URL)
    raw_deals = llm_result.get("top_deals", [])
    deals = []
    for deal in raw_deals:
        url = deal.get("listing_url", "")
        if url and url in valid_urls:
            deals.append(deal)
        elif not valid_urls:
            deals.append(deal)  # no URL validation possible
        else:
            logger.warning(f"Report skipping deal with unmatched URL: {deal.get('make', '?')} {deal.get('model', '?')}")

    lines.append(f"## Top {len(deals)} Value Deals")
    lines.append("")
    lines.append("| # | Rating | Car | Price | Savings | Reasoning |")
    lines.append("|---|--------|-----|-------|---------|-----------|")
    for deal in deals:
        rank = deal.get("rank", "?")
        rating = deal.get("value_rating", "?")
        car = f"{deal.get('year', '?')} {deal.get('make', '?')} {deal.get('model', '?')} {deal.get('variant', '')}"
        price = f"Rs {deal.get('price', 0):,}"
        savings = f"Rs {deal.get('savings_vs_market', 0):,}"
        reasoning = deal.get("reasoning", "")
        lines.append(f"| {rank} | {rating}/10 | {car} | {price} | {savings} | {reasoning} |")
    lines.append("")

    # Deal details
    lines.append("### Deal Details")
    lines.append("")
    for deal in deals:
        rank = deal.get("rank", "?")
        car = f"{deal.get('year', '?')} {deal.get('make', '?')} {deal.get('model', '?')} {deal.get('variant', '')}"
        lines.append(f"**#{rank} — {car}**  ")
        lines.append(f"- **Price:** Rs {deal.get('price', 0):,}  ")
        lines.append(f"- **Estimated fair price:** Rs {deal.get('estimated_fair_price', 'N/A')}  ")
        lines.append(f"- **Savings:** Rs {deal.get('savings_vs_market', 0):,}  ")
        lines.append(f"- **Value rating:** {deal.get('value_rating', '?')}/10  ")
        lines.append(f"- **Why:** {deal.get('reasoning', 'N/A')}  ")
        lines.append(f"- **Risks:** {deal.get('risk_factors', 'N/A')}  ")
        url = deal.get("listing_url", "")
        if url:
            lines.append(f"- **Link:** [{deal.get('platform', 'link')}]({url})  ")
        lines.append("")

    # Segment picks
    picks = llm_result.get("segment_picks", {})
    if picks:
        lines.append("## Segment Picks")
        lines.append("")
        for category, pick in picks.items():
            label = category.replace("_", " ").title()
            if pick is None:
                lines.append(f"**{label}:** No suitable candidate  ")
                continue
            lines.append(
                f"**{label}:** {pick.get('year', '?')} {pick.get('make', '?')} "
                f"{pick.get('model', '?')} — Rs {pick.get('price', 0):,}  "
            )
            lines.append(f"> {pick.get('reason', '')}  ")
            lines.append("")

    # Market insights
    insights = llm_result.get("market_insights", "")
    if insights:
        lines.append("## Market Insights")
        lines.append("")
        lines.append(insights)
        lines.append("")

    filepath = os.path.join(output_dir, f"analysis_{today}.md")
    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, "w") as f:
        f.write("\n".join(lines))
    return filepath


def write_ranked_csv(candidates: pd.DataFrame, output_dir: str, today: str) -> str:
    """Write all scored candidates to a ranked CSV."""
    output_cols = [
        "value_score", "platform", "make", "model", "variant", "year",
        "price", "transmission", "fuel_type", "body_type", "odometer_km",
        "num_owners", "city", "listing_url",
        "score_segment", "score_age_price", "score_km_price",
        "score_owners", "score_brand", "score_pref",
    ]
    available = [c for c in output_cols if c in candidates.columns]
    out = candidates[available].sort_values("value_score", ascending=False)

    filepath = os.path.join(output_dir, f"analysis_{today}_ranked.csv")
    os.makedirs(output_dir, exist_ok=True)
    out.to_csv(filepath, index=False)
    return filepath
