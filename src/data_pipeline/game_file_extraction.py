from typing import Optional
import logging
import requests
from pprint import pprint


class GameFileExtractor:
    """
    Extracts game data from API using player credentials and stores it in database.
    """
    
    def __init__(self, player_id: str, nfsc_cookie: str):
        """
        Initialize the extractor with player credentials.
        
        Args:
            player_id (str): The player's unique identifier
            nfsc_cookie (str): Authentication cookie for API access
        """
        self.player_id = player_id
        self.nfsc_cookie = nfsc_cookie
        self.logger = logging.getLogger(__name__)
        
    def fetch_game_files_from_geoguessr_api(self) -> Optional[dict]:
        """
        Fetches game data from the API.
        
        Returns:
            dict: The API response data if successful, None otherwise
        """
        base_url = "https://www.geoguessr.com/api/v4/feed/private"

        headers = {
            "Cookie": self.nfsc_cookie
        }

        payload = {
            "count": "100"
        }

        try:
            # TODO: Implement API call logic
            response = requests.get(base_url, headers=headers, params=payload)
            response.raise_for_status()
            pprint(response.json())
            return response.json()
        except Exception as e:
            self.logger.error(f"Error fetching data from API: {str(e)}")
            return None
            
    def process_data(self, data: dict) -> Optional[dict]:
        """
        Processes and transforms the raw API data.
        
        Args:
            data (dict): Raw data from API
            
        Returns:
            dict: Processed data ready for database insertion
        """
        try:
            # TODO: Implement data processing logic
            pass
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            return None
            
    def write_to_database(self, processed_data: dict) -> bool:
        """
        Writes the processed data to the database.
        
        Args:
            processed_data (dict): Data to be inserted into database
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # TODO: Implement database writing logic
            pass
        except Exception as e:
            self.logger.error(f"Error writing to database: {str(e)}")
            return False
            
    def run_extraction(self) -> bool:
        """
        Executes the complete extraction pipeline.
        
        Returns:
            bool: True if the entire process was successful, False otherwise
        """
        print("Fetching game files from Geoguessr API...")
        raw_data = self.fetch_game_files_from_geoguessr_api()
        if not raw_data:
            return False
            
        processed_data = self.process_data(raw_data)
        if not processed_data:
            return False
            
        return self.write_to_database(processed_data)
