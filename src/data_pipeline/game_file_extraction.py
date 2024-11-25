from typing import Optional
import logging
import requests
import time
from data_pipeline.utils.db_manager import DatabaseManager
import json


class GameDataValidationError(Exception):
    """Custom exception for game data validation errors."""
    pass


class GameFileExtractor:
    """
    Extracts game data from API using player credentials and stores it in database.
    """
    
    def __init__(self, player_id: str, nfsc_cookie: str, db_path: str = None):
        """
        Initialize the extractor with player credentials.
        
        Args:
            player_id (str): The player's unique identifier
            nfsc_cookie (str): Authentication cookie for API access
            db_path (str): Path to SQLite database file
        """
        self.player_id = player_id
        self.nfsc_cookie = nfsc_cookie
        self.base_url = "https://www.geoguessr.com/api"
        self.logger = logging.getLogger(__name__)
        self.db = DatabaseManager(db_path)
        
    def fetch_game_files_from_geoguessr_api(self) -> Optional[dict]:
        """
        Fetches game data from the API with pagination support.
        
        Returns:
            dict: The API response data if successful, None otherwise
        """
        url = f"{self.base_url}/v4/feed/private"
        headers = {"Cookie": self.nfsc_cookie, "Content-Type": "application/json"}
        all_entries = []
        pagination_token = None

        while True:
            try:
                payload = {"count": "100"}

                # Add pagination token if it exists
                if pagination_token:
                    payload["paginationToken"] = pagination_token

                response = requests.get(url, headers=headers, params=payload)

                # Raise an exception if the response is not successful
                response.raise_for_status()

                # Parse the response as JSON
                data = response.json()
                    
                # Sleep to avoid rate limiting
                time.sleep(1)

                # Accumulate entries 
                if "entries" in data:
                    all_entries.extend(data["entries"])

                # Check for more existence of more entries
                pagination_token = data.get("paginationToken")
                if pagination_token is None:
                    break

            except Exception as e:
                self.logger.error(f"Error fetching data from API: {str(e)}")
                return None

        return {"entries": all_entries}
            
    def process_data(self, data: dict) -> dict:
        """Process and transform the raw API data.
        
        Args:
            data (dict): Raw data from API containing entries with format:
                {
                    "entries": [
                        {
                            "type": int,
                            "payload": [    
                                {
                                    ...
                                }
                            ]
                        },
                        ...
                    ]
                }
            
        Returns:
            dict: Processed data with format:
                {
                    "multiplayer_games": list[dict], 
                    "singleplayer_games": list[dict]
                }
            
        Raises:
            KeyError: If required fields are missing
            GameDataValidationError: If game data processing fails
        """
        # Handle potential JSON string
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON string: {str(e)}")
                raise TypeError(f"Input data is neither a dict nor valid JSON: {data}")

        if not isinstance(data, dict):
            self.logger.error(f"Input data is not a dict: {data}")
            raise TypeError(f"Input data is not a dict: {data}")
        
        entries = data["entries"]
        multiplayer_games = []
        singleplayer_games = []

        if not isinstance(entries, list):
            self.logger.error(f"Entries is not a list: {entries}")
            raise TypeError(f"Entries is not a list: {entries}")

        try:
            for entry in entries:
                # Handle potential JSON string
                if isinstance(entry, str):
                    try:
                        entry = json.loads(entry)
                    except json.JSONDecodeError:
                        self.logger.warning(f"Skipping invalid JSON entry: {entry}")
                        continue

                if not isinstance(entry, dict):
                    self.logger.warning(f"Skipping non-dict entry: {entry}")
                    continue
                    
                if "type" not in entry:
                    self.logger.error(f"Skipping entry without type field: {entry}")
                    raise KeyError(f"Entry is missing type field: {entry}")
                    
                payload = entry.get("payload", [])
                # Handle potential JSON string for payload
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        self.logger.warning(f"Skipping entry with invalid JSON payload: {entry}")
                        continue

                if not isinstance(payload, list):
                    if isinstance(payload, dict):
                        payload = [payload]
                    else:
                        self.logger.warning(f"Skipping entry with invalid payload type: {entry}")
                    continue

                for payload_entry in payload:
                    # Handle potential JSON string in payload entry
                    if isinstance(payload_entry, str):
                        try:
                            payload_entry = json.loads(payload_entry)
                        except json.JSONDecodeError:
                            self.logger.warning(f"Skipping invalid JSON payload entry: {payload_entry}")
                            continue

                    # Process based on entry type
                    if payload_entry["type"] in [6, 7]:
                        multiplayer_games.append(self.process_multiplayer_game(payload_entry))
                    elif payload_entry["type"] in [1, 2]:
                        singleplayer_games.append(self.process_singleplayer_game(payload_entry))
                    
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            raise GameDataValidationError(f"Failed to process data: {str(e)}") from e
        
        return {
            "multiplayer_games": multiplayer_games,
            "singleplayer_games": singleplayer_games
        }
            
    def process_multiplayer_game(self, game_data: dict) -> dict:
        """Process multiplayer game data.
        
        Args:
            game_data (dict): Raw game data from API with format:
                {
                    "time": str,  # ISO format timestamp
                    "type": int, 
                    "payload": {
                        "gameId": str,
                        "gameMode": str,
                        "competitiveGameMode": str
                    }
                }
            
        Returns:
            dict: Processed multiplayer game data in format:
                {
                    "time": str,
                    "game_id": str,
                    "game_mode": str,
                    "competitive_game_mode": str
                }
            
        Raises:
            GameDataValidationError: If the input data fails validation
            KeyError: If required fields are missing
        """

        try:
            if not isinstance(game_data, dict):
                self.logger.error(f"DEBUGGING API Response structure: {game_data}")
                raise TypeError(f"Game data is not a dict: {game_data}")
            
            # Validate required fields
            required_fields = ["time", "type", "payload"]
            missing_fields = [field for field in required_fields if field not in game_data]
            if missing_fields:
                raise KeyError(f"Missing required fields: {missing_fields}")
            
            # Validate payload fields
            payload = game_data["payload"]

            # convert payload to dict if string
            if not isinstance(payload, dict):
                self.logger.error(f"DEBUGGING API Response structure: {payload}")
                raise TypeError(f"Payload is not a dict: {payload}")
            
            payload_fields = ["gameId", "gameMode", "competitiveGameMode"]
            missing_payload_fields = [field for field in payload_fields if field not in payload]
            if missing_payload_fields:
                raise KeyError(f"Missing payload fields: {missing_payload_fields} in {game_data}")

            # Process the data
            return {
                "time": game_data["time"],
                "game_id": payload["gameId"],
                "game_mode": payload["gameMode"],
                "competitive_game_mode": payload["competitiveGameMode"]
            }
            
        except KeyError as e:
            self.logger.error(f"Validation error: {str(e)}")
            raise GameDataValidationError(str(e)) from e
        except Exception as e:
            self.logger.error(f"Unexpected error processing multiplayer game: {str(e)}")
            raise GameDataValidationError(f"Failed to process game data: {str(e)}") from e
            
    def process_singleplayer_game(self, game_data: dict) -> dict:
        """Process singleplayer game data.
        
        Args:
            game_data (dict): Raw game data from API with format:
                {
                    "type": int,  # Should be 1 for singleplayer
                    "time": str,  # ISO format timestamp
                    "payload": {
                        "mapSlug": str,  # Unique map identifier
                        "mapName": str,  # Display name of the map
                        "points": int,  # Score achieved in the game
                        "gameToken": str,  # Unique game identifier
                        "gameMode": str  # Game mode type
                    }
                }
            
        Returns:
            dict: Processed singleplayer game data in format:
                {
                    "time": str,
                    "game_token": str,
                    "map_slug": str,
                    "map_name": str,
                    "points": int,
                    "game_mode": str
                }
            
        Raises:
            GameDataValidationError: If the input data fails validation
        """
        try:
            # Validate input data structure
            if not isinstance(game_data, dict):
                raise TypeError(f"Game data is not a dict: {game_data}")

            required_fields = ["type", "time", "payload"]
            missing_fields = [field for field in required_fields if field not in game_data]
            if missing_fields:
                raise KeyError(f"Missing required fields: {missing_fields}")

            if game_data["type"] != 1:  # Ensure this is a single-player game
                raise ValueError(f"Invalid game type: {game_data['type']} (expected 1)")

            # Validate payload fields
            payload = game_data["payload"]
            payload_required_fields = ["mapSlug", "mapName", "points", "gameToken", "gameMode"]
            missing_payload_fields = [
                field for field in payload_required_fields if field not in payload
            ]
            if missing_payload_fields:
                raise KeyError(f"Missing payload fields: {missing_payload_fields}")

            # Transform data into the desired format
            processed_data = {
                "time": game_data["time"],
                "game_token": payload["gameToken"],
                "map_slug": payload["mapSlug"],
                "map_name": payload["mapName"],
                "points": payload["points"],
                "game_mode": payload["gameMode"],
            }

            return processed_data

        except KeyError as e:
            self.logger.error(f"Validation error in single-player game data: {str(e)}")
            raise GameDataValidationError(str(e)) from e
        except Exception as e:
            self.logger.error(f"Unexpected error processing single-player game: {str(e)}")
            raise GameDataValidationError(f"Failed to process single-player game data: {str(e)}") from e
            
    def run_extraction(self) -> bool:
        """Run the complete extraction pipeline."""
        try:
            # Fetch and process new games
            raw_data = self.fetch_game_files_from_geoguessr_api()
            
            processed_data = self.process_data(raw_data)
            
            self.logger.info(f"Successfully processed {len(processed_data['multiplayer_games'])} multiplayer games and {len(processed_data['singleplayer_games'])} singleplayer games")

            success = self.db.insert_games(self.player_id, processed_data)
            
            # Save to database
            return success
            
        except Exception as e:
            self.logger.error(f"Extraction pipeline failed: {str(e)}")
            return False
