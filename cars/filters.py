import logging
from cars.models import NormalizedCar

logger = logging.getLogger(__name__)


def filter_car(car: NormalizedCar, filters: dict) -> bool:
    """Check if a car passes all configured post-scrape filters.

    Returns True if the car should be kept, False if it should be skipped.
    """
    if not filters:
        return True

    # Price filter
    max_price = filters.get("max_price")
    if max_price is not None and car.price > max_price:
        return False

    # Year filter
    min_year = filters.get("min_year")
    if min_year is not None and car.year < min_year:
        return False

    # Transmission filter
    transmission = filters.get("transmission")
    if transmission is not None:
        if car.transmission and car.transmission != transmission.lower():
            return False

    # Fuel type filter (can be string or list)
    fuel_type = filters.get("fuel_type")
    if fuel_type is not None:
        if isinstance(fuel_type, str):
            fuel_type = [fuel_type]
        allowed = [f.lower() for f in fuel_type]
        if car.fuel_type and car.fuel_type not in allowed:
            return False

    # Body type filter (can be string or list)
    body_type = filters.get("body_type")
    if body_type is not None:
        if isinstance(body_type, str):
            body_type = [body_type]
        allowed = [b.lower() for b in body_type]
        if car.body_type and car.body_type not in allowed:
            return False

    # Max owners filter
    max_owners = filters.get("max_owners")
    if max_owners is not None:
        if car.num_owners is not None and car.num_owners > max_owners:
            return False

    # Max odometer filter
    max_odometer = filters.get("max_odometer_km")
    if max_odometer is not None:
        if car.odometer_km is not None and car.odometer_km > max_odometer:
            return False

    return True
