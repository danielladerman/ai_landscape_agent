import googlemaps
from config.config import settings

def get_google_reviews(place_id: str):
    """
    Fetches the most relevant Google Reviews for a given business place_id.

    Args:
        place_id (str): The Google Maps Place ID of the business.

    Returns:
        list: A list of review dictionaries, each containing 'text' and 'rating'.
              Returns an empty list if an error occurs or no reviews are found.
    """
    if not place_id:
        return []

    if not settings.GOOGLE_MAPS_API_KEY:
        print("Error: GOOGLE_MAPS_API_KEY is not configured.")
        return []

    try:
        gmaps = googlemaps.Client(key=settings.GOOGLE_MAPS_API_KEY)
        
        # Request the 'review' field for the given place_id
        place_details = gmaps.place(place_id=place_id, fields=['review'])
        
        if 'result' in place_details and 'reviews' in place_details['result']:
            return place_details['result']['reviews']
        return []

    except Exception as e:
        print(f"An error occurred while fetching reviews for {place_id}: {e}")
        return []
