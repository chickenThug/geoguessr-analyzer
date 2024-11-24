import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Union, Any
from data_pipeline.utils.location_enricher import LocationEnricher


class DatabaseManager:
    """Manages SQLite database operations for game data."""

    def __init__(self, db_path: Union[str, Path] = None):
        """Initialize database manager.

        Args:
            db_path (Union[str, Path], optional): Path to SQLite database file.
                Defaults to 'data/games.db' in project root.
        """
        if db_path is None:
            # Create default path in project's data directory
            data_dir = Path(__file__).parents[3] / "data"
            data_dir.mkdir(exist_ok=True)
            self.db_path = data_dir / "games.db"
        else:
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Create database and tables if they don't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Create multiplayer games table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS multiplayer_games (
                        game_id TEXT PRIMARY KEY,
                        player_id TEXT NOT NULL,
                        time TEXT NOT NULL,
                        game_mode TEXT NOT NULL,
                        competitive_game_mode TEXT NOT NULL,
                        UNIQUE(game_id, player_id)
                    )
                """
                )

                # Create team duel games table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS team_duel_games (
                        game_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        player1_id TEXT NOT NULL,
                        player2_id TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

                # Create team duel rounds table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS team_duel_rounds (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        game_id TEXT NOT NULL,
                        round_number INTEGER NOT NULL,
                        player1_guess TEXT,
                        player2_guess TEXT,
                        best_opponent_guess TEXT,
                        correct_location TEXT NOT NULL,
                        country_code TEXT NOT NULL,
                        opponent_score INTEGER,
                        team_score INTEGER,
                        heading INTEGER NOT NULL,
                        pitch INTEGER NOT NULL,
                        zoom INTEGER NOT NULL,
                        FOREIGN KEY (game_id) REFERENCES team_duel_games (game_id),
                        UNIQUE(game_id, round_number)
                    )
                """
                )

                # Create enriched team duel rounds table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS team_duel_rounds_enriched (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        game_id TEXT NOT NULL,
                        round_number INTEGER NOT NULL,
                        player1_guess TEXT,
                        player1_guess_country TEXT,
                        player1_guess_region TEXT,
                        player1_guess_state TEXT,
                        player1_guess_city TEXT,
                        player2_guess TEXT,
                        player2_guess_country TEXT,
                        player2_guess_region TEXT,
                        player2_guess_state TEXT,
                        player2_guess_city TEXT,
                        correct_location TEXT NOT NULL,
                        correct_location_country TEXT,
                        correct_location_region TEXT,
                        correct_location_state TEXT,
                        correct_location_city TEXT,
                        country_code TEXT NOT NULL,
                        opponent_score INTEGER,
                        team_score INTEGER,
                        heading INTEGER NOT NULL,
                        pitch INTEGER NOT NULL,
                        zoom INTEGER NOT NULL,
                        FOREIGN KEY (game_id) REFERENCES team_duel_games (game_id),
                        UNIQUE(game_id, round_number)
                    )
                """
                )

                conn.commit()

        except sqlite3.Error as e:
            self.logger.error(f"Database initialization error: {str(e)}")
            raise

    def insert_games(self, player_id: str, data: Dict[str, List[dict]]) -> bool:
        """Insert games into database."""
        try:
            self.logger.info(
                f"Inserting {len(data['multiplayer_games'])} multiplayer games into {self.db_path}"
            )

            if data["multiplayer_games"]:
                self.logger.debug(
                    f"Sample multiplayer game: {data['multiplayer_games'][0]}"
                )

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Insert multiplayer games
                inserted_count = 0
                for game in data["multiplayer_games"]:
                    try:
                        cursor.execute(
                            """
                            INSERT OR REPLACE INTO multiplayer_games 
                            (game_id, player_id, time, game_mode, competitive_game_mode)
                            VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                game["game_id"],
                                player_id,
                                game["time"],
                                game["game_mode"],
                                game["competitive_game_mode"],
                            ),
                        )
                        inserted_count += 1
                    except sqlite3.Error as e:
                        self.logger.error(
                            f"Error inserting multiplayer game {game['game_id']}: {str(e)}"
                        )
                        self.logger.debug(f"Problem game data: {game}")
                        continue

                conn.commit()  # Single commit after all insertions
                self.logger.info(
                    f"Successfully inserted {inserted_count} multiplayer games"
                )
                return True

        except sqlite3.Error as e:
            self.logger.error(f"Database insert error: {str(e)}")
            return False

    def get_game_counts(self, player_id: str) -> Dict[str, int]:
        """Get count of games for a player.

        Args:
            player_id (str): ID of the player

        Returns:
            dict: Counts of games by type
        """
        self.logger.info(f"db path: {self.db_path.resolve()}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                for row in cursor.execute("SELECT COUNT(*) FROM multiplayer_games"):
                    print(row)

        except sqlite3.Error as e:
            self.logger.error(f"Error getting game counts: {str(e)}")
            return 0

    def get_avg_number_of_rounds(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                for row in cursor.execute(
                    "SELECT AVG(max_rounds) as avg_rounds FROM (SELECT MAX(round_number) as max_rounds FROM team_duel_rounds GROUP BY game_id)"
                ):
                    print(row)
        except sqlite3.Error as e:
            self.logger.error(f"Error getting player one guesses by order: {str(e)}")

    def get_game_ids(self) -> List[Dict[str, str]]:
        """
        Get all game IDs and their modes from multiplayer_games table.

        Returns:
            List[Dict[str, str]]: List of dictionaries containing game_id, game_mode,
                and competitive_game_mode for each game
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT game_id, game_mode
                    FROM multiplayer_games
                """
                )

                # Convert rows to list of dictionaries
                rows = cursor.fetchall()
                games = [
                    {
                        "game_id": row[0],
                        "game_mode": row[1],
                    }
                    for row in rows
                ]

                self.logger.info(f"Retrieved {len(games)} game IDs from database")
                return games

        except sqlite3.Error as e:
            self.logger.error(f"Error getting game IDs: {str(e)}")
            return []

    def insert_duel_games(self, data: List[Dict[str, Any]]) -> bool:
        """Insert duel games into database."""
        pass

    def insert_single_player_games(self, data: List[Dict[str, Any]]) -> bool:
        """Insert single player games into database."""
        pass

    def insert_team_duel_game(self, game_data: dict) -> bool:
        """
        Insert team duel game and its rounds into database.

        Args:
            game_data (dict): Processed team duel game data

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Insert game data
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO team_duel_games 
                    (game_id, status, player1_id, player2_id)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        game_data["game_id"],
                        game_data["status"],
                        game_data["player1"],
                        game_data["player2"],
                    ),
                )

                # Insert round data
                for round_data in game_data["rounds"]:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO team_duel_rounds
                        (game_id, round_number, player1_guess, player2_guess, 
                         best_opponent_guess, correct_location, country_code,
                         opponent_score, team_score, heading, pitch, zoom)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            game_data["game_id"],
                            round_data["round_number"],
                            round_data["player1_guess"],
                            round_data["player2_guess"],
                            round_data["best_opponent_guess"],
                            round_data["correct_location"],
                            round_data["country_code"],
                            round_data["opponent_score"],
                            round_data["team_score"],
                            round_data["heading"],
                            round_data["pitch"],
                            round_data["zoom"],
                        ),
                    )

                conn.commit()
                self.logger.info(
                    f"Successfully inserted team duel game {game_data['game_id']} with {len(game_data['rounds'])} rounds"
                )
                return True

        except sqlite3.Error as e:
            self.logger.error(
                f"Error inserting team duel game {game_data['game_id']}: {str(e)}"
            )
            return False

    def insert_enriched_team_duel_game(
        self, game_data: dict, location_enricher: LocationEnricher
    ) -> bool:
        """
        Insert team duel game with enriched location data.

        Args:
            game_data (dict): Processed team duel game data
            location_enricher (LocationEnricher): Instance of LocationEnricher

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Insert base game data (reuse existing method)
                self.insert_team_duel_game(game_data)

                # Insert enriched round data
                for round_data in game_data["rounds"]:
                    # Enrich locations
                    player1_loc = location_enricher.get_location_data(
                        round_data["player1_guess"]
                    )
                    player2_loc = location_enricher.get_location_data(
                        round_data["player2_guess"]
                    )
                    correct_loc = location_enricher.get_location_data(
                        round_data["correct_location"]
                    )

                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO team_duel_rounds_enriched
                        (game_id, round_number, player1_guess, player1_guess_country, 
                         player1_guess_region, player1_guess_state, player1_guess_city,
                         player2_guess, player2_guess_country, player2_guess_region,
                         player2_guess_state, player2_guess_city, correct_location,
                         correct_location_country, correct_location_region,
                         correct_location_state, correct_location_city, country_code,
                         opponent_score, team_score, heading, pitch, zoom)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            game_data["game_id"],
                            round_data["round_number"],
                            round_data["player1_guess"],
                            player1_loc.get("country") if player1_loc else None,
                            player1_loc.get("region") if player1_loc else None,
                            player1_loc.get("state") if player1_loc else None,
                            player1_loc.get("city") if player1_loc else None,
                            round_data["player2_guess"],
                            player2_loc.get("country") if player2_loc else None,
                            player2_loc.get("region") if player2_loc else None,
                            player2_loc.get("state") if player2_loc else None,
                            player2_loc.get("city") if player2_loc else None,
                            round_data["correct_location"],
                            correct_loc.get("country") if correct_loc else None,
                            correct_loc.get("region") if correct_loc else None,
                            correct_loc.get("state") if correct_loc else None,
                            correct_loc.get("city") if correct_loc else None,
                            round_data["country_code"],
                            round_data["opponent_score"],
                            round_data["team_score"],
                            round_data["heading"],
                            round_data["pitch"],
                            round_data["zoom"],
                        ),
                    )

                conn.commit()
                self.logger.info(
                    f"Successfully inserted enriched team duel game {game_data['game_id']} "
                    f"with {len(game_data['rounds'])} rounds"
                )
                return True

        except sqlite3.Error as e:
            self.logger.error(
                f"Error inserting enriched team duel game {game_data['game_id']}: {str(e)}"
            )
            return False

    def enrich_team_duel_rounds(self, location_enricher: LocationEnricher) -> bool:
        """
        Read existing team duel rounds, enrich with location data, and save to enriched table.

        Args:
            location_enricher (LocationEnricher): Instance of LocationEnricher

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Get all rounds
                cursor.execute(
                    """
                    SELECT 
                        game_id, round_number, player1_guess, player2_guess,
                        correct_location, country_code, opponent_score,
                        team_score, heading, pitch, zoom
                    FROM team_duel_rounds
                """
                )

                rounds = cursor.fetchall()
                total_rounds = len(rounds)
                success_count = 0

                # Process each round
                for round_data in rounds:
                    (
                        game_id,
                        round_number,
                        player1_guess,
                        player2_guess,
                        correct_location,
                        country_code,
                        opponent_score,
                        team_score,
                        heading,
                        pitch,
                        zoom,
                    ) = round_data

                    # Enrich locations
                    player1_loc = location_enricher.get_location_data(player1_guess)
                    player2_loc = location_enricher.get_location_data(player2_guess)
                    correct_loc = location_enricher.get_location_data(correct_location)

                    # Insert enriched data
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO team_duel_rounds_enriched
                        (game_id, round_number, player1_guess, player1_guess_country, 
                         player1_guess_region, player1_guess_state, player1_guess_city,
                         player2_guess, player2_guess_country, player2_guess_region,
                         player2_guess_state, player2_guess_city, correct_location,
                         correct_location_country, correct_location_region,
                         correct_location_state, correct_location_city, country_code,
                         opponent_score, team_score, heading, pitch, zoom)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            game_id,
                            round_number,
                            player1_guess,
                            player1_loc.get("country") if player1_loc else None,
                            player1_loc.get("region") if player1_loc else None,
                            player1_loc.get("state") if player1_loc else None,
                            player1_loc.get("city") if player1_loc else None,
                            player2_guess,
                            player2_loc.get("country") if player2_loc else None,
                            player2_loc.get("region") if player2_loc else None,
                            player2_loc.get("state") if player2_loc else None,
                            player2_loc.get("city") if player2_loc else None,
                            correct_location,
                            correct_loc.get("country") if correct_loc else None,
                            correct_loc.get("region") if correct_loc else None,
                            correct_loc.get("state") if correct_loc else None,
                            correct_loc.get("city") if correct_loc else None,
                            country_code,
                            opponent_score,
                            team_score,
                            heading,
                            pitch,
                            zoom,
                        ),
                    )

                    success_count += 1
                    if success_count % 10 == 0:  # Log progress every 10 rounds
                        self.logger.info(
                            f"Processed {success_count} out of {total_rounds} rounds "
                            f"({(success_count/total_rounds)*100:.1f}%)"
                        )

                conn.commit()
                self.logger.info(
                    f"Successfully enriched {success_count} team duel rounds"
                )
                return True

        except sqlite3.Error as e:
            self.logger.error(f"Error enriching team duel rounds: {str(e)}")
            return False
