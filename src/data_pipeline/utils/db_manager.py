import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Union

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
            data_dir = Path(__file__).parents[3] / 'data'
            data_dir.mkdir(exist_ok=True)
            self.db_path = data_dir / 'games.db'
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
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS multiplayer_games (
                        game_id TEXT PRIMARY KEY,
                        player_id TEXT NOT NULL,
                        time TEXT NOT NULL,
                        game_mode TEXT NOT NULL,
                        competitive_game_mode TEXT NOT NULL,
                        UNIQUE(game_id, player_id)
                    )
                """)
                conn.commit()
                
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization error: {str(e)}")
            raise

    def insert_games(self, player_id: str, data: Dict[str, List[dict]]) -> bool:
        """Insert games into database."""
        try:
            self.logger.info(f"Inserting {len(data['multiplayer_games'])} multiplayer games into {self.db_path}")
            
            if data['multiplayer_games']:
                self.logger.debug(f"Sample multiplayer game: {data['multiplayer_games'][0]}")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Insert multiplayer games
                inserted_count = 0
                for game in data["multiplayer_games"]:
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO multiplayer_games 
                            (game_id, player_id, time, game_mode, competitive_game_mode)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            game["game_id"],
                            player_id,
                            game["time"],
                            game["game_mode"],
                            game["competitive_game_mode"]
                        ))
                        inserted_count += 1
                    except sqlite3.Error as e:
                        self.logger.error(f"Error inserting multiplayer game {game['game_id']}: {str(e)}")
                        self.logger.debug(f"Problem game data: {game}")
                        continue
                
                conn.commit()  # Single commit after all insertions
                self.logger.info(f"Successfully inserted {inserted_count} multiplayer games")
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
