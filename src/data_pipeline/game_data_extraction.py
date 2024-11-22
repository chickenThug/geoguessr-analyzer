from typing import Optional, Dict, List
import logging
import requests
import time
from data_pipeline.utils.db_manager import DatabaseManager


class GameDataValidationError(Exception):
    """Custom exception for game data validation errors."""

    pass


class GameDataExtractor:
    """
    Extracts both single player and duel game data from API and SQLite database.
    """

    def __init__(self, nfsc_cookie: str, db_path: str = None):
        """
        Initialize the game data extractor.

        Args:
            nfsc_cookie (str): Authentication cookie for API access
            db_path (str): Path to SQLite database file
        """
        self.nfsc_cookie = nfsc_cookie
        self.base_url = "https://www.geoguessr.com/api"
        self.game_server_url = "https://game-server.geoguessr.com/api"
        self.logger = logging.getLogger(__name__)
        self.db = DatabaseManager(db_path)

    def fetch_single_player_game(self, game_token: str) -> Optional[dict]:
        """
        Fetches single player game data from the API.

        Args:
            game_token (str): The game token to fetch

        Returns:
            dict: The API response data if successful, None otherwise
        """
        url = f"{self.base_url}/v3/games/{game_token}"
        headers = {"Cookie": self.nfsc_cookie}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            self.logger.error(
                f"Error fetching single player game data from API: {str(e)}"
            )
            return None

    def fetch_duel_game(self, duel_id: str) -> Optional[dict]:
        """
        Fetches duel game data from the API.

        Args:
            duel_id (str): The duel game ID to fetch

        Returns:
            dict: The API response data if successful, None otherwise
        """
        url = f"{self.game_server_url}/duels/{duel_id}"
        headers = {"Cookie": self.nfsc_cookie}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            self.logger.error(f"Error fetching duel game data from API: {str(e)}")
            return None

    def process_single_player_game(self, data: dict) -> dict:
        """
        Process and transform the raw API single player game data.

        Args:
            data (dict): Raw game data from API

        Returns:
            dict: Processed game data

        Raises:
            GameDataValidationError: If data processing fails
        """
        try:
            # TODO: Implement single player game processing
            return

        except Exception as e:
            self.logger.error(f"Error processing single player game data: {str(e)}")
            raise GameDataValidationError(
                f"Failed to process single player game data: {str(e)}"
            ) from e

    def process_duel_game(self, data: dict) -> dict:
        """
        Process and transform the raw API duel game data.

        Args:
            data (dict): Raw duel data from API

        Returns:
            dict: Processed duel data

        Raises:
            GameDataValidationError: If data processing fails
        """
        try:
            # TODO: Implement duel game processing
            return

        except Exception as e:
            self.logger.error(f"Error processing duel game data: {str(e)}")
            raise GameDataValidationError(
                f"Failed to process duel game data: {str(e)}"
            ) from e

    def extract_single_player_games(self, game_tokens: List[str]) -> bool:
        """
        Extract and store single player games.

        Args:
            game_tokens (List[str]): List of game tokens to process

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            processed_games = []

            for game_token in game_tokens:
                raw_data = self.fetch_single_player_game(game_token)
                if raw_data:
                    processed_data = self.process_single_player_game(raw_data)
                    processed_games.append(processed_data)
                    time.sleep(1)  # Rate limiting

            return self.db.insert_single_player_games(processed_games)

        except Exception as e:
            self.logger.error(f"Single player game extraction failed: {str(e)}")
            return False

    def extract_duel_games(self, game_info: List[(str, str)]) -> bool:
        """
        Extract and store duel games
        """
        try:
            processed_duels = []
            processed_single_player_games = []

            for duel_id, game_type in game_info:

                if game_type == "duel":
                    raw_data = self.fetch_duel_game(duel_id)
                    processed_data = self.process_duel_game(raw_data)
                    processed_duels.append(processed_data)
                elif game_type == "single_player":
                    raw_data = self.fetch_single_player_game(duel_id)
                    processed_data = self.process_single_player_game(raw_data)
                    processed_single_player_games.append(processed_data)
                else:
                    self.logger.error(f"Invalid game type: {game_type}")
                    continue
                time.sleep(1)  # Rate limiting

            return self.db.insert_duel_games(
                processed_duels
            ) and self.db.insert_single_player_games(processed_single_player_games)

        except Exception as e:
            self.logger.error(f"Duel game extraction failed: {str(e)}")
            return False
