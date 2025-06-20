import googlemaps
import time
from config.config import settings

class GoogleMapsFinder:
    """A class to find businesses using the Google Maps Places API."""
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Google Maps API key is required.")
        self.gmaps = googlemaps.Client(key=api_key)

    def find_businesses(self, query: str, max_results: int = 20):
        """
        Finds businesses matching the query, handling pagination and detail fetching.
        """
        places_result = self.gmaps.places(query=query)
        business_list = []

        while len(business_list) < max_results and places_result:
            for place in places_result.get('results', []):
                if len(business_list) >= max_results:
                    break

                place_id = place.get('place_id')
                if not place_id:
                    continue

                try:
                    details = self.gmaps.place(place_id=place_id, fields=['name', 'formatted_address', 'website', 'formatted_phone_number', 'place_id'])
                    place_details = details.get('result', {})
                    
                    business_info = {
                        "place_id": place_details.get('place_id'),
                        "name": place_details.get('name'),
                        "address": place_details.get('formatted_address'),
                        "website": place_details.get('website'),
                        "phone_number": place_details.get('formatted_phone_number')
                    }
                    if business_info["name"] and business_info["website"]:
                        business_list.append(business_info)
                        print(f"Found business: {business_info['name']}")
                except Exception as e:
                    print(f"Error fetching details for place_id {place_id}: {e}")

            next_page_token = places_result.get('next_page_token')
            if next_page_token and len(business_list) < max_results:
                time.sleep(2)
                places_result = self.gmaps.places(query=query, page_token=next_page_token)
            else:
                break
        
        return business_list[:max_results]
