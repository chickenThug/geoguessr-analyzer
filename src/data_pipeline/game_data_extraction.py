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

    def __init__(
        self,
        nfsc_cookie: str,
        player_id1: str,
        player_id2: str = None,
        db_path: str = None,
    ):
        """
        Initialize the game data extractor.

        Args:
            nfsc_cookie (str): Authentication cookie for API access
            db_path (str): Path to SQLite database file
        """
        self.nfsc_cookie = nfsc_cookie
        self.base_url = "https://www.geoguessr.com/api"
        self.game_server_url = "https://game-server.geoguessr.com/api"
        self.player_id1 = player_id1
        self.player_id2 = player_id2
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

    def process_team_duel_game(self, data: dict) -> dict:
        """
        Process and transform the raw API team duel game data.

        Args:
            data (dict): Raw team duel data from API

        Returns:
            dict: Processed team duel data with round summaries

        Raises:
            GameDataValidationError: If data processing fails
        """
        try:
            # Find the team containing player_id1
            player_team = None
            opponent_team = None
            for team in data["teams"]:
                team_player_ids = [p["playerId"] for p in team["players"]]
                if self.player_id1 in team_player_ids:
                    player_team = team
                    opponent_team = [t for t in data["teams"] if t != team][0]
                    break

            if not player_team:
                self.logger.warning(f"Player {self.player_id1} not found in any team")
                return None

            rounds = []
            for round_data in data["rounds"]:
                round_num = round_data["roundNumber"]

                # Get player guesses for this round
                player1_guess = None
                player2_guess = None
                for player in player_team["players"]:
                    for guess in player["guesses"]:
                        if guess["roundNumber"] == round_num:
                            if player["playerId"] == self.player_id1:
                                player1_guess = f"{guess['lat']}, {guess['lng']}"
                            else:  # Record second player's guess regardless of ID
                                player2_guess = f"{guess['lat']}, {guess['lng']}"

                # Get best opponent guess for this round
                opponent_round_result = next(
                    (
                        r
                        for r in opponent_team["roundResults"]
                        if r["roundNumber"] == round_num
                    ),
                    None,
                )
                best_opponent_guess = None
                opponent_score = None
                if opponent_round_result and opponent_round_result["bestGuess"]:
                    best_guess = opponent_round_result["bestGuess"]
                    best_opponent_guess = f"{best_guess['lat']}, {best_guess['lng']}"
                    opponent_score = best_guess["score"]

                # Get team score for this round
                team_round_result = next(
                    (
                        r
                        for r in player_team["roundResults"]
                        if r["roundNumber"] == round_num
                    ),
                    None,
                )
                team_score = team_round_result["score"] if team_round_result else None

                # Get correct location
                panorama = round_data["panorama"]
                correct_location = f"{panorama['lat']}, {panorama['lng']}"

                round_summary = {
                    "round_number": round_num,
                    "player1_guess": player1_guess,
                    "player2_guess": player2_guess,
                    "best_opponent_guess": best_opponent_guess,
                    "correct_location": correct_location,
                    "country_code": panorama["countryCode"],
                    "opponent_score": opponent_score,
                    "team_score": team_score,
                    "heading": panorama["heading"],
                    "pitch": panorama["pitch"],
                    "zoom": panorama["zoom"],
                }
                rounds.append(round_summary)

            return {
                "game_id": data["gameId"],
                "status": data["status"],
                "player1": team_player_ids[0],
                "player2": team_player_ids[1],
                "rounds": rounds,
            }

        except Exception as e:
            self.logger.error(f"Error processing team duel game data: {str(e)}")
            raise GameDataValidationError(
                f"Failed to process team duel game data: {str(e)}"
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

    def extract_duel_games(self) -> bool:
        """
        Extract and store duel games
        """
        game_meta_data = self.db.get_game_ids()
        for meta_data_dict in game_meta_data:
            game_id = meta_data_dict["game_id"]
            game_mode = meta_data_dict["game_mode"]
            game_data = self.fetch_duel_game(game_id)
            from pprint import pprint

            # pprint(game_data, sort_dicts=False)
            if game_mode == "Duels":
                pass
            elif game_mode == "TeamDuels":
                processed_data = self.process_team_duel_game(game_data)
                pprint(processed_data, sort_dicts=False)
                break
            else:
                self.logger.error(f"Invalid game mode: {game_mode}")
                continue


if __name__ == "__main__":
    extractor = GameDataExtractor(nfsc_cookie="")
    extractor.extract_duel_games()
