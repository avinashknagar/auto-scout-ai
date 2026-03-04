"""Cleanup and maintenance for the car scraping pipeline."""

import glob
import logging
import os
from datetime import date, timedelta

from cars.db import Database

logger = logging.getLogger(__name__)


def _cutoff_date(retention_days: int) -> str:
    """Return ISO date string N days ago."""
    return (date.today() - timedelta(days=retention_days)).isoformat()


def _delete_old_files(pattern: str, before_date: str, dry_run: bool = False) -> list[str]:
    """Delete files matching glob pattern whose date stamp is older than before_date.

    Expects filenames containing a YYYY-MM-DD date substring.
    Returns list of deleted (or would-be-deleted) file paths.
    """
    import re
    date_re = re.compile(r"\d{4}-\d{2}-\d{2}")
    deleted = []
    for filepath in glob.glob(pattern):
        match = date_re.search(os.path.basename(filepath))
        if match and match.group() < before_date:
            deleted.append(filepath)
            if not dry_run:
                os.remove(filepath)
    return deleted


def _rotate_log(log_path: str, max_size_mb: float, dry_run: bool = False) -> bool:
    """Rotate log file if it exceeds max_size_mb. Returns True if rotated."""
    if not os.path.exists(log_path):
        return False
    size_mb = os.path.getsize(log_path) / (1024 * 1024)
    if size_mb <= max_size_mb:
        return False
    if not dry_run:
        backup = log_path + ".old"
        if os.path.exists(backup):
            os.remove(backup)
        os.rename(log_path, backup)
    return True


def run_cleanup(config: dict, retention_days: int = 30, dry_run: bool = False) -> dict:
    """Run all cleanup tasks. Returns summary dict."""
    cleanup_config = config.get("cleanup", {})
    if retention_days is None:
        retention_days = cleanup_config.get("retention_days", 30)
    log_max_size_mb = cleanup_config.get("log_max_size_mb", 5)

    cutoff = _cutoff_date(retention_days)
    db_path = config["database"]["path"]
    data_dir = os.path.dirname(db_path)
    log_path = config.get("logging", {}).get("file", "data/scraper.log")
    analysis_dir = config.get("analyze", {}).get("output_dir", "data/analysis")

    # Make paths absolute relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not os.path.isabs(data_dir):
        data_dir = os.path.join(project_root, data_dir)
    if not os.path.isabs(log_path):
        log_path = os.path.join(project_root, log_path)
    if not os.path.isabs(analysis_dir):
        analysis_dir = os.path.join(project_root, analysis_dir)

    prefix = "[DRY RUN] " if dry_run else ""
    results = {}

    print(f"\n{'=' * 40}")
    print(f"{prefix}Cleanup (retention: {retention_days} days, cutoff: {cutoff})")
    print(f"{'=' * 40}")

    # 1. Database cleanup
    db = Database(db_path)

    count = db.purge_delisted(cutoff, dry_run)
    results["delisted_purged"] = count
    print(f"  {prefix}Delisted cars purged: {count}")

    count = db.trim_price_history(cutoff, dry_run)
    results["price_history_trimmed"] = count
    print(f"  {prefix}Price history rows trimmed: {count}")

    count = db.prune_scrape_runs(cutoff, dry_run)
    results["scrape_runs_pruned"] = count
    print(f"  {prefix}Scrape run records pruned: {count}")

    # 2. Delete old daily CSVs (keep cars_latest.csv)
    csv_pattern = os.path.join(data_dir, "cars_????-??-??.csv")
    deleted_csvs = _delete_old_files(csv_pattern, cutoff, dry_run)
    results["csvs_deleted"] = len(deleted_csvs)
    print(f"  {prefix}Daily CSVs deleted: {len(deleted_csvs)}")
    for f in deleted_csvs:
        print(f"    {os.path.basename(f)}")

    # 3. Delete old analysis reports
    md_pattern = os.path.join(analysis_dir, "analysis_*.md")
    csv_rank_pattern = os.path.join(analysis_dir, "*_ranked.csv")
    deleted_reports = _delete_old_files(md_pattern, cutoff, dry_run)
    deleted_reports += _delete_old_files(csv_rank_pattern, cutoff, dry_run)
    results["reports_deleted"] = len(deleted_reports)
    print(f"  {prefix}Analysis reports deleted: {len(deleted_reports)}")
    for f in deleted_reports:
        print(f"    {os.path.basename(f)}")

    # 4. Rotate log
    rotated = _rotate_log(log_path, log_max_size_mb, dry_run)
    results["log_rotated"] = rotated
    if rotated:
        size_mb = os.path.getsize(log_path) / (1024 * 1024) if os.path.exists(log_path) else 0
        print(f"  {prefix}Log rotated ({size_mb:.1f} MB > {log_max_size_mb} MB limit)")
    else:
        print(f"  Log rotation: not needed")

    # 5. VACUUM database
    if not dry_run:
        db.vacuum()
        print(f"  Database vacuumed")
    else:
        print(f"  {prefix}Would vacuum database")

    db.close()

    total_actions = (
        results["delisted_purged"] + results["price_history_trimmed"]
        + results["scrape_runs_pruned"] + results["csvs_deleted"]
        + results["reports_deleted"] + int(results["log_rotated"])
    )
    if total_actions == 0:
        print(f"\n  Nothing to clean up.")
    elif dry_run:
        print(f"\n  Re-run without --dry-run to execute cleanup.")

    return results
