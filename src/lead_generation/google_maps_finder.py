import googlemaps
import time
from config.config import settings

class GoogleMapsFinder:
    """A class to find businesses using the Google Maps Places API."""
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Google Maps API key is required.")
        self.api_key = api_key
        self.gmaps = googlemaps.Client(key=self.api_key)

    def find_businesses(self, query: str, max_results: int = 20) -> list:
        """
        Finds businesses using Google Maps Text Search. This is a simple, non-paginated search.
        Returns a list of business dictionaries.
        """
        try:
            places_result = self.gmaps.places(query=query)
            business_list = []
            
            for place in places_result.get('results', []):
                if len(business_list) >= max_results:
                    break
                
                place_id = place.get('place_id')
                if not place_id:
                    continue
                
                details = self.get_place_details(place_id)
                if details:
                    business_list.append(details)
            
            return business_list

        except Exception as e:
            print(f"An error occurred in find_businesses: {e}")
            return []

    def find_businesses_paginated(self, query: str, page_token: str = None):
        """
        Finds businesses using Google Maps Text Search with pagination support.
        Returns a list of business dictionaries and the next page token.
        """
        try:
            if page_token:
                # API requires a delay before using the next page token
                time.sleep(2)
                results = self.gmaps.places(query=query, page_token=page_token)
            else:
                results = self.gmaps.places(query=query)
            
            businesses = []
            for place in results.get('results', []):
                place_id = place.get('place_id')
                if not place_id:
                    continue

                details = self.get_place_details(place_id)
                if details:
                    businesses.append(details)
            
            next_page_token = results.get('next_page_token')
            return businesses, next_page_token

        except Exception as e:
            print(f"An unexpected error occurred during paginated search: {e}")
            return [], None

    def get_place_details(self, place_id: str) -> dict:
        """
        Fetches detailed information for a specific place ID.
        """
        try:
            # Define the fields you want to retrieve
            fields = ['name', 'formatted_address', 'website', 'formatted_phone_number', 'place_id']
            details = self.gmaps.place(place_id=place_id, fields=fields)
            
            place_details = details.get('result', {})
            
            # Ensure the essential details are present
            if place_details.get('name') and place_details.get('website'):
                return {
                    "place_id": place_details.get('place_id'),
                    "name": place_details.get('name'),
                    "address": place_details.get('formatted_address'),
                    "website": place_details.get('website'),
                    "phone_number": place_details.get('formatted_phone_number')
                }
            return None
        except Exception as e:
            print(f"An error occurred while fetching place details for {place_id}: {e}")
            return None
