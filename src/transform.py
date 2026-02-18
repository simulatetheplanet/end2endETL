import json
import pandas as pd
import os

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def get_latest_raw_file():
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".json")]
    if not files:
        raise FileNotFoundError("No raw JSON files found.")
    latest_file = max(files)
    return os.path.join(RAW_DIR, latest_file)

def transform_games():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    latest_file = get_latest_raw_file()

    with open(latest_file, "r") as f:
        data = json.load(f)

    games = data.get("data", [])

    records = []
    for game in games:
        records.append({
            "game_id": game["id"],
            "date": game["date"],
            "home_team_id": game["home_team"]["id"],
            "home_team": game["home_team"]["full_name"],
            "home_score": game["home_team_score"],
            "away_team_id": game["visitor_team"]["id"],
            "away_team": game["visitor_team"]["full_name"],
            "away_score": game["visitor_team_score"],
            "season": game["season"],
            "status": game["status"]
        })

    df = pd.DataFrame(records)

    output_path = os.path.join(PROCESSED_DIR, "games.parquet")
    df.to_parquet(output_path, index=False)

    print(f"Processed data saved to {output_path}")

if __name__ == "__main__":
    transform_games()
