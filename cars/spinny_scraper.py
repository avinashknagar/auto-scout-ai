import json
from cars.base_scraper import BaseScraper, USER_AGENT
from cars.models import NormalizedCar
from cars.utils import retry_with_backoff


class SpinnyScraper(BaseScraper):

    @property
    def platform_name(self) -> str:
        return "spinny"

    def _setup_session(self):
        tokens = self.tokens["spinny"]
        self.session.cookies.update({
            "csrftoken": tokens["csrftoken"],
            "sessionid": tokens["sessionid"],
            "platform": "web",
        })
        self.session.headers.update({
            "accept": "*/*",
            "content-type": "application/json",
            "platform": "web",
            "procurement-category": "assured,luxury",
            "origin": "https://www.spinny.com",
            "referer": "https://www.spinny.com/",
            "user-agent": USER_AGENT,
        })
        self.base_url = self.config["platforms"]["spinny"]["base_url"]

    @retry_with_backoff(max_retries=3)
    def fetch_page(self, page_cursor) -> tuple:
        page_num = page_cursor or 1
        filters = self.config.get("filters", {})
        params = {
            "city": self.config["platforms"]["spinny"]["city_slug"],
            "product_type": "cars",
            "category": "used",
            "max_price": filters.get("max_price", self.config["scrape"].get("price_max", 10000000)),
            "page": page_num,
            "size": self.config["scrape"]["page_size"],
            "show_max_on_assured": "true",
            "custom_budget_sort": "true",
            "high_intent_required": "false",
            "active_banner": "true",
        }
        min_price = filters.get("min_price", self.config["scrape"].get("price_min", 0))
        if min_price > 0:
            params["min_price"] = min_price
        min_year = filters.get("min_year")
        if min_year:
            params["min_year"] = min_year

        resp = self.session.get(
            self.base_url,
            params=params,
            timeout=self.config["rate_limit"]["request_timeout"],
        )
        resp.raise_for_status()
        data = resp.json()

        cars = data.get("results", [])
        total = data.get("count") or 0

        # Spinny's API returns inconsistent count/next on later pages,
        # so track total from page 1 and paginate based on items seen
        if not hasattr(self, "_spinny_total"):
            self._spinny_total = total
            self._spinny_seen = 0

        self._spinny_seen += len(cars)
        has_more = len(cars) > 0 and self._spinny_seen < self._spinny_total
        next_cursor = page_num + 1 if has_more else None

        return cars, next_cursor, self._spinny_total

    def normalize(self, raw: dict) -> NormalizedCar:
        # Build image URL with https: protocol
        image_url = None
        if raw.get("images") and len(raw["images"]) > 0:
            absurl = raw["images"][0].get("file", {}).get("absurl", "")
            if absurl:
                image_url = f"https:{absurl}" if not absurl.startswith("http") else absurl

        # Build listing URL
        perm_url = raw.get("permanent_url")
        listing_url = f"https://www.spinny.com{perm_url}" if perm_url else None

        return NormalizedCar(
            platform="spinny",
            platform_id=str(raw["id"]),
            make=raw["make"].title(),
            model=raw["model"].title(),
            variant=raw.get("variant"),
            year=raw["make_year"],
            price=int(raw["price"]),
            transmission=(raw.get("transmission", "") or "").lower() or None,
            fuel_type=(raw.get("fuel_type", "") or "").lower() or None,
            body_type=(raw.get("body_type", "") or "").lower() or None,
            odometer_km=raw.get("mileage"),
            color=(raw.get("color", "") or "").lower() or None,
            num_owners=raw.get("no_of_owners"),
            rto_code=(raw.get("rto", "") or "").upper() or None,
            city=(raw.get("city", "") or "").title() or None,
            seller_type=(raw.get("seller_type", "") or "").lower() or None,
            listing_url=listing_url,
            image_url=image_url,
            raw_json=json.dumps(raw),
        )
