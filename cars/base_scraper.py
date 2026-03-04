from abc import ABC, abstractmethod
from typing import Iterator
import logging

import requests

from cars.models import NormalizedCar
from cars.filters import filter_car
from cars.utils import RateLimiter

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/144.0.0.0 Safari/537.36"
)


class BaseScraper(ABC):
    """Common interface for all platform scrapers."""

    def __init__(self, config: dict, tokens: dict):
        self.config = config
        self.tokens = tokens
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(config["rate_limit"]["delay_between_requests"])
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pages_fetched = 0
        self.cars_filtered = 0
        self._setup_session()

    @property
    @abstractmethod
    def platform_name(self) -> str:
        pass

    @abstractmethod
    def _setup_session(self):
        """Set common headers on self.session."""
        pass

    @abstractmethod
    def fetch_page(self, page_cursor) -> tuple:
        """Fetch one page. Returns (raw_car_dicts, next_cursor, total_count)."""
        pass

    @abstractmethod
    def normalize(self, raw_car: dict) -> NormalizedCar:
        """Convert platform-specific car dict to NormalizedCar."""
        pass

    def scrape_all(self) -> Iterator[NormalizedCar]:
        """Full pagination loop. Yields normalized, filtered cars."""
        cursor = None
        page_num = 0
        max_pages = self.config["scrape"].get("max_pages")
        filters = self.config.get("filters", {})
        self.pages_fetched = 0
        self.cars_filtered = 0

        while True:
            if max_pages and page_num >= max_pages:
                self.logger.info(f"Reached max_pages limit ({max_pages})")
                break

            self.rate_limiter.wait()
            cars_raw, next_cursor, total = self.fetch_page(cursor)
            page_num += 1
            self.pages_fetched = page_num

            self.logger.info(
                f"Page {page_num}: got {len(cars_raw)} cars (total available: {total})"
            )

            for raw_car in cars_raw:
                try:
                    car = self.normalize(raw_car)
                except Exception as e:
                    self.logger.warning(f"Failed to normalize car: {e}", exc_info=True)
                    continue

                if not filter_car(car, filters):
                    self.cars_filtered += 1
                    continue

                yield car

            if not next_cursor or len(cars_raw) == 0:
                break
            cursor = next_cursor

        self.logger.info(
            f"Scrape complete: {page_num} pages, {self.cars_filtered} cars filtered out"
        )

    def scrape_first_page(self) -> list:
        """Dry-run: fetch only the first page, return normalized cars."""
        cars_raw, _, total = self.fetch_page(None)
        self.logger.info(f"Dry run: {len(cars_raw)} cars on page 1, {total} total available")
        results = []
        for raw_car in cars_raw:
            try:
                results.append(self.normalize(raw_car))
            except Exception as e:
                self.logger.warning(f"Failed to normalize: {e}")
        return results
