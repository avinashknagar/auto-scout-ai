from dataclasses import dataclass
from typing import Optional
import json


@dataclass
class NormalizedCar:
    """Platform-agnostic representation of a used car listing."""
    platform: str
    platform_id: str
    make: str
    model: str
    variant: Optional[str]
    year: int
    price: int
    transmission: Optional[str]
    fuel_type: Optional[str]
    body_type: Optional[str]
    odometer_km: Optional[int]
    color: Optional[str]
    num_owners: Optional[int]
    rto_code: Optional[str]
    city: Optional[str]
    seller_type: Optional[str]
    listing_url: Optional[str]
    image_url: Optional[str]
    raw_json: Optional[str] = None

    def to_db_tuple(self) -> tuple:
        """Returns tuple matching INSERT column order for cars table."""
        return (
            self.platform, self.platform_id,
            self.make, self.model, self.variant, self.year, self.price,
            self.transmission, self.fuel_type, self.body_type,
            self.odometer_km, self.color, self.num_owners,
            self.rto_code, self.city, self.seller_type,
            self.listing_url, self.image_url, self.raw_json,
        )

    def summary(self) -> str:
        """One-line human-readable summary."""
        km = f"{self.odometer_km:,} km" if self.odometer_km else "? km"
        return (
            f"{self.year} {self.make} {self.model} | "
            f"Rs {self.price:,} | {km} | "
            f"{self.transmission or '?'} | {self.fuel_type or '?'} | "
            f"{self.platform}"
        )
