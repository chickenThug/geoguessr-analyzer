from typing import Dict, Optional
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
import logging


class LocationEnricher:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="geoguessr_analysis")
        self.logger = logging.getLogger(__name__)

    def get_location_data(self, lat_lng: str) -> Optional[Dict[str, str]]:
        """
        Get enriched location data from coordinates.

        Args:
            lat_lng (str): Coordinates in "lat, lng" format

        Returns:
            Optional[Dict[str, str]]: Dictionary containing location details or None if failed
        """
        if not lat_lng:
            return None

        try:
            lat, lng = map(float, lat_lng.split(", "))

            # Rate limiting
            time.sleep(1)

            location = self.geolocator.reverse((lat, lng), language="en")

            if not location:
                return None

            address = location.raw.get("address", {})

            result = {
                "country": address.get("country"),
                "region": address.get("region"),
                "state": address.get("state"),
                "city": address.get("city"),
            }

            return result

        except (ValueError, GeocoderTimedOut) as e:
            self.logger.error(f"Error enriching location {lat_lng}: {str(e)}")
            return None
