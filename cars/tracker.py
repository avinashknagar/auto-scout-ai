import logging
from cars.db import Database

logger = logging.getLogger(__name__)


class ChangeTracker:
    """Detects new listings, price changes, and delistings."""

    def __init__(self, db: Database):
        self.db = db

    def process_scrape(self, platform: str, scraped_ids: set, scrape_date: str) -> dict:
        """Compare scraped IDs against DB to detect changes.

        Call this after all cars for a platform are upserted.
        Returns a summary dict.
        """
        active_ids = self.db.get_active_ids(platform)

        # Cars in DB but not in today's scrape = delisted
        delisted_ids = active_ids - scraped_ids
        # Cars in today's scrape but not previously in DB = new
        new_ids = scraped_ids - active_ids

        if delisted_ids:
            logger.info(f"{platform}: marking {len(delisted_ids)} cars as delisted")
            self.db.mark_delisted(platform, delisted_ids, scrape_date)

        # Detect price changes
        price_changes = self.db.get_price_changes(platform, scrape_date)
        if price_changes:
            logger.info(f"{platform}: {len(price_changes)} price changes detected")
            for pid, old_price, new_price in price_changes[:5]:
                diff = new_price - old_price
                sign = "+" if diff > 0 else ""
                logger.info(f"  {pid}: Rs {old_price:,} -> Rs {new_price:,} ({sign}{diff:,})")
            if len(price_changes) > 5:
                logger.info(f"  ... and {len(price_changes) - 5} more")

        return {
            "new": len(new_ids),
            "delisted": len(delisted_ids),
            "price_changes": len(price_changes),
            "total_active": len(scraped_ids),
        }
