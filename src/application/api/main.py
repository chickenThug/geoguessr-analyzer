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
    db_path = Path("data/games.db")

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM team_duel_rounds_enriched
                WHERE correct_location_country IS NOT NULL
                ORDER BY id DESC
            """
            )

            # Get column names
            columns = [description[0] for description in cursor.description]

            rows = cursor.fetchall()
            result = [dict(zip(columns, row)) for row in rows]

            return jsonify(result)
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("Starting server...")  # Server startup print
    # Force HTTP, disable redirects
    app.config["PREFERRED_URL_SCHEME"] = "http"
    app.run(debug=True, port=5000, ssl_context=None)
    print("Server started!")  # Server started print
