import json
from cars.base_scraper import BaseScraper, USER_AGENT
from cars.models import NormalizedCar
from cars.utils import retry_with_backoff


class Cars24Scraper(BaseScraper):

    @property
    def platform_name(self) -> str:
        return "cars24"

    def _setup_session(self):
        token = self.tokens["cars24"]["bearer_token"]
        user_id = self.tokens["cars24"]["user_id"]
        cfg = self.config["platforms"]["cars24"]

        self.session.headers.update({
            "authorization": f"Bearer {token}",
            "userid": user_id,
            "x_tenant_id": cfg["x_tenant_id"],
            "x_user_city_id": cfg["x_user_city_id"],
            "content-type": "application/json",
            "accept": "application/json, text/plain, */*",
            "source": "WebApp",
            "user-agent": USER_AGENT,
            "origin": "https://www.cars24.com",
            "referer": "https://www.cars24.com/",
        })
        self.base_url = cfg["base_url"]

    @retry_with_backoff(max_retries=3)
    def fetch_page(self, page_cursor) -> tuple:
        filters = self.config.get("filters", {})
        search_filters = [
            f"listingPrice:bw:{filters.get('min_price', self.config['scrape'].get('price_min', 0))},"
            f"{filters.get('max_price', self.config['scrape'].get('price_max', 10000000))}"
        ]
        min_year = filters.get("min_year")
        if min_year:
            search_filters.append(f"year:bw:{min_year},2026")

        body = {
            "searchFilter": search_filters,
            "cityId": self.config["platforms"]["cars24"]["city_id"],
            "sort": "bestmatch",
            "size": self.config["scrape"]["page_size"],
            "searchAfter": page_cursor or [],
            "filterVersion": 4,
        }
        resp = self.session.post(
            self.base_url,
            json=body,
            timeout=self.config["rate_limit"]["request_timeout"],
        )
        resp.raise_for_status()
        data = resp.json()

        cars = data.get("content", [])
        page_info = data.get("page", {})
        total = page_info.get("totalElements", 0)
        next_cursor = page_info.get("searchAfter")

        if not next_cursor or len(cars) == 0:
            next_cursor = None

        return cars, next_cursor, total

    def normalize(self, raw: dict) -> NormalizedCar:
        # Build listing URL
        base_url = raw.get("cdpBaseUrl", "https://www.cars24.com/")
        rel_url = raw.get("cdpRelativeUrl", "")
        listing_url = base_url + rel_url if rel_url else None

        return NormalizedCar(
            platform="cars24",
            platform_id=str(raw["appointmentId"]),
            make=raw["make"].title(),
            model=raw["model"].title(),
            variant=raw.get("variant"),
            year=raw["year"],
            price=int(raw["listingPrice"]),
            transmission=(raw.get("transmissionType", {}).get("value", "") or "").lower() or None,
            fuel_type=(raw.get("fuelType", "") or "").lower() or None,
            body_type=(raw.get("bodyType", "") or "").lower() or None,
            odometer_km=raw.get("odometer", {}).get("value"),
            color=(raw.get("color", "") or "").lower() or None,
            num_owners=raw.get("ownership"),
            rto_code=(raw.get("cityRto", "") or "").upper() or None,
            city=raw.get("address", {}).get("locality", self.config["scrape"]["city"]),
            seller_type=(raw.get("sellerSubType", "") or "").lower() or None,
            listing_url=listing_url,
            image_url=raw.get("listingImage", {}).get("uri"),
            raw_json=json.dumps(raw),
        )
