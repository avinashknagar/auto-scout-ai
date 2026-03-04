#!/usr/bin/env python3
"""Car Scraping Pipeline - CLI entry point."""

import argparse
import csv
import logging
import os
import sys
from datetime import date, datetime

from cars.config import load_config, load_tokens
from cars.db import Database
from cars.tracker import ChangeTracker
from cars.utils import AuthExpiredError, setup_logging

logger = logging.getLogger("run")


def _write_csv(rows: list, filepath: str) -> int:
    """Write list of dicts to CSV. Returns row count."""
    if not rows:
        return 0
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def auto_export_csvs(db: Database, today: str, data_dir: str):
    """Auto-export CSVs after a scrape run."""
    # 1. All active listings
    cars = db.export_active_cars()
    latest_path = os.path.join(data_dir, "cars_latest.csv")
    count = _write_csv(cars, latest_path)
    if count:
        print(f"\n--- Auto-exported CSVs ---")
        print(f"  {latest_path}  ({count} active listings)")

    # 2. Daily snapshot
    daily_path = os.path.join(data_dir, f"cars_{today}.csv")
    _write_csv(cars, daily_path)
    print(f"  {daily_path}  (daily snapshot)")

    # 3. Price changes (only if there are any)
    changes = db.export_price_changes(today)
    if changes:
        changes_path = os.path.join(data_dir, "price_changes.csv")
        _write_csv(changes, changes_path)
        print(f"  {changes_path}  ({len(changes)} price changes)")


def get_scraper(platform: str, config: dict, tokens: dict):
    """Factory for platform scrapers."""
    if platform == "cars24":
        from cars.cars24_scraper import Cars24Scraper
        return Cars24Scraper(config, tokens)
    elif platform == "spinny":
        from cars.spinny_scraper import SpinnyScraper
        return SpinnyScraper(config, tokens)
    else:
        raise ValueError(f"Unknown platform: {platform}")


def cmd_scrape(args, config, tokens):
    """Run scrapers for enabled platforms."""
    db = Database(config["database"]["path"])
    tracker = ChangeTracker(db)
    today = date.today().isoformat()

    platforms = []
    if args.platform == "all":
        for p in ("cars24", "spinny"):
            if config["platforms"].get(p, {}).get("enabled", False):
                platforms.append(p)
    else:
        platforms = [args.platform]

    for platform in platforms:
        started_at = datetime.now().isoformat()
        logger.info(f"{'=' * 50}")
        logger.info(f"Starting scrape: {platform}")
        logger.info(f"{'=' * 50}")

        try:
            scraper = get_scraper(platform, config, tokens)

            if args.dry_run:
                cars = scraper.scrape_first_page()
                print(f"\n--- {platform.upper()} Dry Run ---")
                print(f"Cars on page 1: {len(cars)}")
                for car in cars[:5]:
                    print(f"  {car.summary()}")
                if len(cars) > 5:
                    print(f"  ... and {len(cars) - 5} more on this page")
                continue

            # Full scrape
            scraped_ids = set()
            cars_new = 0
            cars_updated = 0

            for car in scraper.scrape_all():
                is_new = db.upsert_car(car, today)
                scraped_ids.add(car.platform_id)
                if is_new:
                    cars_new += 1
                else:
                    cars_updated += 1

            # Detect delistings and price changes
            changes = tracker.process_scrape(platform, scraped_ids, today)

            finished_at = datetime.now().isoformat()
            db.record_scrape_run(
                platform=platform,
                scrape_date=today,
                started_at=started_at,
                finished_at=finished_at,
                status="completed",
                cars_found=len(scraped_ids),
                cars_new=cars_new,
                cars_updated=cars_updated,
                cars_delisted=changes["delisted"],
                cars_filtered=scraper.cars_filtered,
                pages_fetched=scraper.pages_fetched,
            )

            print(f"\n--- {platform.upper()} Results ---")
            print(f"  Cars scraped:   {len(scraped_ids)}")
            print(f"  New listings:   {cars_new}")
            print(f"  Updated:        {cars_updated}")
            print(f"  Delisted:       {changes['delisted']}")
            print(f"  Price changes:  {changes['price_changes']}")
            print(f"  Filtered out:   {scraper.cars_filtered}")
            print(f"  Pages fetched:  {scraper.pages_fetched}")

        except AuthExpiredError as e:
            logger.error(f"{platform}: {e}")
            print(f"\n[ERROR] {platform}: Auth expired. Update tokens.yaml and re-run.")
            db.record_scrape_run(
                platform=platform,
                scrape_date=today,
                started_at=started_at,
                finished_at=datetime.now().isoformat(),
                status="failed",
                error_message=str(e),
            )

        except Exception as e:
            logger.error(f"{platform} scrape failed: {e}", exc_info=True)
            print(f"\n[ERROR] {platform}: {e}")
            db.record_scrape_run(
                platform=platform,
                scrape_date=today,
                started_at=started_at,
                finished_at=datetime.now().isoformat(),
                status="failed",
                error_message=str(e),
            )

    # Auto-export CSVs after scraping (skip on dry-run)
    if not args.dry_run:
        data_dir = os.path.dirname(config["database"]["path"])
        auto_export_csvs(db, today, data_dir)

    db.close()


def cmd_report(args, config, _tokens):
    """Show summary of scraped data."""
    db = Database(config["database"]["path"])
    report_date = args.date or date.today().isoformat()

    if args.type == "summary":
        summary = db.get_summary(report_date)
        print(f"\n=== Car Scrape Summary ===")
        print(f"Total active listings: {summary['total_active']}")
        for platform, count in summary["by_platform"].items():
            print(f"  {platform}: {count}")

    elif args.type == "changes":
        print(f"\n=== Price Changes for {report_date} ===")
        for platform in ("cars24", "spinny"):
            changes = db.get_price_changes(platform, report_date)
            if changes:
                print(f"\n{platform.upper()}:")
                for pid, old_price, new_price in changes:
                    diff = new_price - old_price
                    sign = "+" if diff > 0 else ""
                    print(f"  {pid}: Rs {old_price:,} -> Rs {new_price:,} ({sign}{diff:,})")
            else:
                print(f"\n{platform.upper()}: No price changes")

    db.close()


def cmd_export(args, config, _tokens):
    """Export active listings to CSV."""
    db = Database(config["database"]["path"])
    cars = db.export_active_cars()

    if not cars:
        print("No active listings to export.")
        db.close()
        return

    output = args.output or f"data/export_{date.today().isoformat()}.csv"
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cars[0].keys())
        writer.writeheader()
        writer.writerows(cars)

    print(f"Exported {len(cars)} cars to {output}")
    db.close()


def cmd_analyze(args, config):
    """Find best value deals using heuristic scoring + LLM analysis."""
    from cars.analyzer import run_analysis

    result = run_analysis(
        config=config,
        dry_run=args.dry_run,
        preference_overrides=args.prefer,
        top_count=args.top,
        platform=args.platform,
    )
    if result.get("cost_usd"):
        print(f"\nTotal cost: ${result['cost_usd']}")


def cmd_findcars(args, config, tokens):
    """Full pipeline: scrape -> export -> analyze in one shot."""
    from cars.analyzer import run_analysis

    # Step 1: Scrape (unless --skip-scrape)
    if not args.skip_scrape:
        print("=" * 50)
        print("Step 1/2: Scraping")
        print("=" * 50)

        # Build a scrape-compatible args namespace
        scrape_args = argparse.Namespace(
            platform=args.platform,
            dry_run=False,  # Always do a full scrape for findcars
        )
        cmd_scrape(scrape_args, config, tokens)
    else:
        print("=" * 50)
        print("Step 1/2: Scraping (SKIPPED --skip-scrape)")
        print("=" * 50)

    # Step 2: Analyze
    print()
    print("=" * 50)
    print(f"Step 2/2: Analysis{' (heuristic only --dry-run)' if args.dry_run else ''}")
    print("=" * 50)

    result = run_analysis(
        config=config,
        dry_run=args.dry_run,
        preference_overrides=args.prefer,
        top_count=args.top,
        platform=args.platform,
    )

    # Summary
    print()
    print("=" * 50)
    print("Done!")
    print("=" * 50)
    print(f"  Listings analyzed: {result.get('total_listings', '?')}")
    print(f"  Candidates scored: {result.get('candidates', '?')}")
    if result.get("top_deals"):
        print(f"  Top deals found:   {result['top_deals']}")
    if result.get("output_csv"):
        print(f"  Ranked CSV:        {result['output_csv']}")
    if result.get("output_report"):
        print(f"  Full report:       {result['output_report']}")
    if result.get("cost_usd"):
        print(f"  LLM cost:          ${result['cost_usd']}")


def cmd_cleanup(args, config):
    """Run cleanup and maintenance tasks."""
    from cars.cleanup import run_cleanup

    run_cleanup(
        config=config,
        retention_days=args.older_than,
        dry_run=args.dry_run,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Car Scraping Pipeline - Cars24 & Spinny"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # scrape
    scrape_parser = subparsers.add_parser("scrape", help="Run scrapers")
    scrape_parser.add_argument(
        "--platform", choices=["cars24", "spinny", "all"], default="all"
    )
    scrape_parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch first page only, print results, don't store"
    )

    # report
    report_parser = subparsers.add_parser("report", help="Show data summary")
    report_parser.add_argument("--date", help="Date (YYYY-MM-DD), default: today")
    report_parser.add_argument(
        "--type", choices=["summary", "changes"], default="summary"
    )

    # export
    export_parser = subparsers.add_parser("export", help="Export to CSV")
    export_parser.add_argument("--output", help="Output CSV path")
    export_parser.add_argument(
        "--platform", choices=["cars24", "spinny", "all"], default="all"
    )

    # analyze
    analyze_parser = subparsers.add_parser(
        "analyze", help="Find best value deals using heuristic scoring + LLM"
    )
    analyze_parser.add_argument(
        "--platform", choices=["cars24", "spinny", "all"], default="all",
        help="Analyze only one platform (default: all)"
    )
    analyze_parser.add_argument(
        "--dry-run", action="store_true",
        help="Heuristic scoring only, skip LLM (no API cost)"
    )
    analyze_parser.add_argument(
        "--prefer", nargs="+", metavar="PREF",
        help="Preference overrides (e.g. automatic suv petrol)"
    )
    analyze_parser.add_argument(
        "--top", type=int, default=None,
        help="Number of top deals to return (default: 15)"
    )

    # findcars — full pipeline in one shot
    findcars_parser = subparsers.add_parser(
        "findcars", help="Full pipeline: scrape + analyze in one command"
    )
    findcars_parser.add_argument(
        "--platform", choices=["cars24", "spinny", "all"], default="all",
        help="Platform to scrape/analyze (default: all)"
    )
    findcars_parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape + heuristic scoring only, skip LLM (no API cost)"
    )
    findcars_parser.add_argument(
        "--skip-scrape", action="store_true",
        help="Skip scraping, reuse existing data and just re-analyze"
    )
    findcars_parser.add_argument(
        "--prefer", nargs="+", metavar="PREF",
        help="Preference overrides (e.g. automatic suv petrol)"
    )
    findcars_parser.add_argument(
        "--top", type=int, default=None,
        help="Number of top deals to return (default: 15)"
    )

    # cleanup — maintenance
    cleanup_parser = subparsers.add_parser(
        "cleanup", help="Clean up old data, logs, and DB rows"
    )
    cleanup_parser.add_argument(
        "--older-than", type=int, default=30,
        help="Retention period in days (default: 30)"
    )
    cleanup_parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be cleaned without deleting"
    )

    args = parser.parse_args()
    config = load_config()
    setup_logging(config)

    tokens = None
    needs_tokens = (
        args.command == "scrape"
        or (args.command == "findcars" and not args.skip_scrape)
    )
    if needs_tokens:
        tokens = load_tokens()

    if args.command == "scrape":
        cmd_scrape(args, config, tokens)
    elif args.command == "report":
        cmd_report(args, config, tokens)
    elif args.command == "export":
        cmd_export(args, config, tokens)
    elif args.command == "analyze":
        cmd_analyze(args, config)
    elif args.command == "findcars":
        cmd_findcars(args, config, tokens)
    elif args.command == "cleanup":
        cmd_cleanup(args, config)


if __name__ == "__main__":
    main()
