import os
import sqlite3
import logging
from datetime import date
from cars.models import NormalizedCar

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cars (
    platform        TEXT NOT NULL,
    platform_id     TEXT NOT NULL,
    make            TEXT NOT NULL,
    model           TEXT NOT NULL,
    variant         TEXT,
    year            INTEGER NOT NULL,
    price           INTEGER NOT NULL,
    transmission    TEXT,
    fuel_type       TEXT,
    body_type       TEXT,
    odometer_km     INTEGER,
    color           TEXT,
    num_owners      INTEGER,
    rto_code        TEXT,
    city            TEXT,
    seller_type     TEXT,
    listing_url     TEXT,
    image_url       TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date  TEXT NOT NULL,
    is_active       INTEGER NOT NULL DEFAULT 1,
    raw_json        TEXT,
    PRIMARY KEY (platform, platform_id)
);

CREATE INDEX IF NOT EXISTS idx_cars_make_model ON cars(make, model);
CREATE INDEX IF NOT EXISTS idx_cars_price ON cars(price);
CREATE INDEX IF NOT EXISTS idx_cars_year ON cars(year);
CREATE INDEX IF NOT EXISTS idx_cars_last_seen ON cars(last_seen_date);
CREATE INDEX IF NOT EXISTS idx_cars_active ON cars(is_active);

CREATE TABLE IF NOT EXISTS price_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT NOT NULL,
    platform_id     TEXT NOT NULL,
    price           INTEGER NOT NULL,
    scrape_date     TEXT NOT NULL,
    UNIQUE(platform, platform_id, scrape_date)
);

CREATE INDEX IF NOT EXISTS idx_ph_car ON price_history(platform, platform_id);
CREATE INDEX IF NOT EXISTS idx_ph_date ON price_history(scrape_date);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT NOT NULL,
    scrape_date     TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL,
    cars_found      INTEGER DEFAULT 0,
    cars_new        INTEGER DEFAULT 0,
    cars_updated    INTEGER DEFAULT 0,
    cars_delisted   INTEGER DEFAULT 0,
    cars_filtered   INTEGER DEFAULT 0,
    pages_fetched   INTEGER DEFAULT 0,
    error_message   TEXT
);
"""


class Database:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA_SQL)

    def upsert_car(self, car: NormalizedCar, scrape_date: str) -> bool:
        """Insert or update a car. Returns True if this is a new car."""
        with self.conn:
            existing = self.conn.execute(
                "SELECT first_seen_date FROM cars WHERE platform=? AND platform_id=?",
                (car.platform, car.platform_id),
            ).fetchone()

            first_seen = existing[0] if existing else scrape_date
            is_new = existing is None

            # to_db_tuple() returns: platform, platform_id, make, model,
            # variant, year, price, transmission, fuel_type, body_type,
            # odometer_km, color, num_owners, rto_code, city, seller_type,
            # listing_url, image_url, raw_json
            # We need to insert first_seen_date and last_seen_date between
            # image_url and raw_json in the SQL
            t = car.to_db_tuple()
            # Split: fields before raw_json (indices 0-17), raw_json (index 18)
            params = t[:18] + (first_seen, scrape_date) + (t[18],)

            self.conn.execute(
                """
                INSERT INTO cars (
                    platform, platform_id, make, model, variant, year, price,
                    transmission, fuel_type, body_type, odometer_km, color,
                    num_owners, rto_code, city, seller_type, listing_url,
                    image_url, first_seen_date, last_seen_date, is_active, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(platform, platform_id) DO UPDATE SET
                    price = excluded.price,
                    odometer_km = excluded.odometer_km,
                    last_seen_date = excluded.last_seen_date,
                    is_active = 1,
                    raw_json = excluded.raw_json
                """,
                params,
            )

            # Price history (one entry per car per day)
            self.conn.execute(
                """
                INSERT INTO price_history (platform, platform_id, price, scrape_date)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(platform, platform_id, scrape_date) DO UPDATE SET
                    price = excluded.price
                """,
                (car.platform, car.platform_id, car.price, scrape_date),
            )

            return is_new

    def get_active_ids(self, platform: str) -> set:
        """Get all currently active platform_ids for a platform."""
        rows = self.conn.execute(
            "SELECT platform_id FROM cars WHERE platform=? AND is_active=1",
            (platform,),
        ).fetchall()
        return {r[0] for r in rows}

    def mark_delisted(self, platform: str, platform_ids: set, scrape_date: str):
        """Mark cars as no longer active."""
        with self.conn:
            for pid in platform_ids:
                self.conn.execute(
                    "UPDATE cars SET is_active=0, last_seen_date=? "
                    "WHERE platform=? AND platform_id=?",
                    (scrape_date, platform, pid),
                )

    def get_price_changes(self, platform: str, scrape_date: str) -> list:
        """Find cars whose price changed compared to previous scrape."""
        rows = self.conn.execute(
            """
            SELECT ph1.platform_id, ph1.price as old_price, ph2.price as new_price
            FROM price_history ph1
            JOIN price_history ph2
                ON ph1.platform = ph2.platform
                AND ph1.platform_id = ph2.platform_id
            WHERE ph1.platform = ?
                AND ph2.scrape_date = ?
                AND ph1.scrape_date = (
                    SELECT MAX(scrape_date) FROM price_history
                    WHERE platform = ph1.platform
                        AND platform_id = ph1.platform_id
                        AND scrape_date < ?
                )
                AND ph1.price != ph2.price
            """,
            (platform, scrape_date, scrape_date),
        ).fetchall()
        return rows

    def get_summary(self, scrape_date: str = None) -> dict:
        """Get summary stats for reporting."""
        if scrape_date is None:
            scrape_date = date.today().isoformat()

        total_active = self.conn.execute(
            "SELECT COUNT(*) FROM cars WHERE is_active=1"
        ).fetchone()[0]

        by_platform = {}
        for platform in ("cars24", "spinny"):
            count = self.conn.execute(
                "SELECT COUNT(*) FROM cars WHERE platform=? AND is_active=1",
                (platform,),
            ).fetchone()[0]
            by_platform[platform] = count

        return {
            "scrape_date": scrape_date,
            "total_active": total_active,
            "by_platform": by_platform,
        }

    def export_active_cars(self) -> list:
        """Get all active cars as list of dicts for CSV export."""
        self.conn.row_factory = sqlite3.Row
        rows = self.conn.execute(
            """
            SELECT platform, platform_id, make, model, variant, year, price,
                   transmission, fuel_type, body_type, odometer_km, color,
                   num_owners, rto_code, city, seller_type, listing_url,
                   first_seen_date, last_seen_date
            FROM cars WHERE is_active=1
            ORDER BY price ASC
            """
        ).fetchall()
        self.conn.row_factory = None
        return [dict(r) for r in rows]

    def export_price_changes(self, scrape_date: str) -> list:
        """Get price changes with car details as list of dicts for CSV."""
        self.conn.row_factory = sqlite3.Row
        rows = self.conn.execute(
            """
            SELECT c.platform, c.platform_id, c.make, c.model, c.variant,
                   c.year, ph1.price as old_price, ph2.price as new_price,
                   (ph2.price - ph1.price) as price_diff,
                   c.transmission, c.fuel_type, c.body_type, c.odometer_km,
                   c.listing_url
            FROM price_history ph1
            JOIN price_history ph2
                ON ph1.platform = ph2.platform
                AND ph1.platform_id = ph2.platform_id
            JOIN cars c
                ON c.platform = ph1.platform
                AND c.platform_id = ph1.platform_id
            WHERE ph2.scrape_date = ?
                AND ph1.scrape_date = (
                    SELECT MAX(scrape_date) FROM price_history
                    WHERE platform = ph1.platform
                        AND platform_id = ph1.platform_id
                        AND scrape_date < ?
                )
                AND ph1.price != ph2.price
            ORDER BY (ph2.price - ph1.price) ASC
            """,
            (scrape_date, scrape_date),
        ).fetchall()
        self.conn.row_factory = None
        return [dict(r) for r in rows]

    def record_scrape_run(self, platform: str, scrape_date: str, started_at: str,
                          finished_at: str, status: str, cars_found: int = 0,
                          cars_new: int = 0, cars_updated: int = 0,
                          cars_delisted: int = 0, cars_filtered: int = 0,
                          pages_fetched: int = 0, error_message: str = None):
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO scrape_runs (
                    platform, scrape_date, started_at, finished_at, status,
                    cars_found, cars_new, cars_updated, cars_delisted,
                    cars_filtered, pages_fetched, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (platform, scrape_date, started_at, finished_at, status,
                 cars_found, cars_new, cars_updated, cars_delisted,
                 cars_filtered, pages_fetched, error_message),
            )

    # ---- Cleanup methods ----

    def purge_delisted(self, before_date: str, dry_run: bool = False) -> int:
        """Delete is_active=0 rows where last_seen_date < before_date.
        Returns number of rows affected."""
        count = self.conn.execute(
            "SELECT COUNT(*) FROM cars WHERE is_active=0 AND last_seen_date < ?",
            (before_date,),
        ).fetchone()[0]
        if not dry_run and count:
            with self.conn:
                self.conn.execute(
                    "DELETE FROM cars WHERE is_active=0 AND last_seen_date < ?",
                    (before_date,),
                )
        return count

    def trim_price_history(self, before_date: str, dry_run: bool = False) -> int:
        """Delete price_history rows older than before_date for inactive cars.
        Returns number of rows affected."""
        count = self.conn.execute(
            """SELECT COUNT(*) FROM price_history ph
               WHERE ph.scrape_date < ?
               AND EXISTS (
                   SELECT 1 FROM cars c
                   WHERE c.platform = ph.platform AND c.platform_id = ph.platform_id
                   AND c.is_active = 0
               )""",
            (before_date,),
        ).fetchone()[0]
        if not dry_run and count:
            with self.conn:
                self.conn.execute(
                    """DELETE FROM price_history
                       WHERE scrape_date < ?
                       AND EXISTS (
                           SELECT 1 FROM cars c
                           WHERE c.platform = price_history.platform
                           AND c.platform_id = price_history.platform_id
                           AND c.is_active = 0
                       )""",
                    (before_date,),
                )
        return count

    def prune_scrape_runs(self, before_date: str, dry_run: bool = False) -> int:
        """Delete scrape_run records older than before_date.
        Returns number of rows affected."""
        count = self.conn.execute(
            "SELECT COUNT(*) FROM scrape_runs WHERE scrape_date < ?",
            (before_date,),
        ).fetchone()[0]
        if not dry_run and count:
            with self.conn:
                self.conn.execute(
                    "DELETE FROM scrape_runs WHERE scrape_date < ?",
                    (before_date,),
                )
        return count

    def vacuum(self):
        """VACUUM the database to reclaim space after deletions."""
        self.conn.execute("VACUUM")

    def close(self):
        self.conn.close()
