import os
import json
import glob
import pandas as pd

RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "games.parquet")


def ensure_dirs():
    os.makedirs(PROCESSED_DIR, exist_ok=True)


def flatten_game(game: dict) -> dict:
    return {
        "game_id": game.get("id"),
        "date": game.get("date"),
        "season": game.get("season"),
        "status": game.get("status"),
        "postseason": game.get("postseason"),

        "home_team_id": game.get("home_team", {}).get("id"),
        "home_team": game.get("home_team", {}).get("full_name"),
        "visitor_team_id": game.get("visitor_team", {}).get("id"),
        "visitor_team": game.get("visitor_team", {}).get("full_name"),

        "home_score": game.get("home_score"),
        "visitor_score": game.get("visitor_score"),

        "home_q1": (game.get("home_team_score_by_period") or [None]*4)[0],
        "home_q2": (game.get("home_team_score_by_period") or [None]*4)[1],
        "home_q3": (game.get("home_team_score_by_period") or [None]*4)[2],
        "home_q4": (game.get("home_team_score_by_period") or [None]*4)[3],

        "visitor_q1": (game.get("visitor_team_score_by_period") or [None]*4)[0],
        "visitor_q2": (game.get("visitor_team_score_by_period") or [None]*4)[1],
        "visitor_q3": (game.get("visitor_team_score_by_period") or [None]*4)[2],
        "visitor_q4": (game.get("visitor_team_score_by_period") or [None]*4)[3],
    }


def transform():
    ensure_dirs()

    json_files = glob.glob(os.path.join(RAW_DIR, "games_*.json"))

    if not json_files:
        print("No raw files found.")
        return

    rows = []

    for file in json_files:
        print(f"Reading {file}")
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        games = data.get("data", [])
        for game in games:
            rows.append(flatten_game(game))

    df = pd.DataFrame(rows)

    print(f"Total rows: {len(df)}")

    df.to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    transform()
