from flask import Flask, jsonify
from flask_cors import CORS
import sqlite3
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

print("Starting Flask application...")  # Initial startup print

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "http://localhost:5173"}})

print("Flask app created and CORS configured")  # Configuration print


@app.route("/api/team-duel-rounds")
def get_team_duel_rounds():
    logging.info("Route accessed!")  # Debug print
    db_path = Path("data/games.db")
    logging.info(f"Looking for DB at: {db_path.absolute()}")  # Debug print

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 
                    correct_location_country as country,
                    team_score,
                    opponent_score,
                    (team_score - opponent_score) as score_difference
                FROM team_duel_rounds_enriched
                WHERE correct_location_country IS NOT NULL
            """
            )

            rows = cursor.fetchall()
            result = [
                {
                    "country": row[0],
                    "team_score": row[1],
                    "opponent_score": row[2],
                    "score_difference": row[3],
                }
                for row in rows
            ]
            print(f"Found {len(result)} rows")  # Debug print
            return jsonify(result)
    except Exception as e:
        print(f"Error: {e}")  # Debug print
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("Starting server...")  # Server startup print
    # Force HTTP, disable redirects
    app.config["PREFERRED_URL_SCHEME"] = "http"
    app.run(debug=True, port=5000, ssl_context=None)
    print("Server started!")  # Server started print
