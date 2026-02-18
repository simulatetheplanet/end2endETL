import os
import json
import time
from datetime import datetime
import requests

try:
    import pandas as pd
except ImportError:
    pd = None

BASE_URL = "https://api.balldontlie.io/v1/games"
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

# Free tier is ~5 requests/min => sleep ~12s/request
REQUEST_DELAY_SECONDS = 12


def get_api_key() -> str:
    api_key = os.getenv("BALLDONTLIE_API_KEY")
    if not api_key:
        raise ValueError("BALLDONTLIE_API_KEY not set. (PowerShell: $env:BALLDONTLIE_API_KEY='...')")
    return api_key


def extract_quarter_scores(game: dict) -> dict:
    row = {}

    # Q1-Q4
    for q in range(1, 5):
        row[f"home_q{q}"] = game.get(f"home_q{q}")
        row[f"away_q{q}"] = game.get(f"visitor_q{q}")

    # OTs (home_ot1, visitor_ot1, etc.)
    ot = 1
    while True:
        hk = f"home_ot{ot}"
        ak = f"visitor_ot{ot}"
        if hk in game or ak in game:
            row[f"home_ot{ot}"] = game.get(hk)
            row[f"away_ot{ot}"] = game.get(ak)
            ot += 1
        else:
            break

    return row


def summarize_game(game: dict) -> dict:
    raw_date = game.get("date") or ""
    game_date = raw_date[:10] if isinstance(raw_date, str) else None

    home_team = (game.get("home_team") or {}).get("full_name")
    away_team = (game.get("visitor_team") or {}).get("full_name")

    row = {
        # IDs / keys
        "game_id": game.get("id"),

        # date + context
        "date": game_date,
        "season": game.get("season"),
        "status": game.get("status"),
        "datetime": game.get("datetime"),
        "period": game.get("period"),

        # flags
        "postseason": game.get("postseason"),
        "postponed": game.get("postponed"),

        # teams
        "home_team": home_team,
        "away_team": away_team,

        # final score
        "home_score": game.get("home_team_score"),
        "away_score": game.get("visitor_team_score"),
    }

    row.update(extract_quarter_scores(game))
    return row


def safe_get(url: str, *, params: dict, headers: dict) -> dict:
    """GET with simple retry for rate limit and transient errors."""
    while True:
        resp = requests.get(url, params=params, headers=headers, timeout=30)

        if resp.status_code == 429:
            # Too many requests
            print("Rate limit hit (429). Sleeping...")
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        # Occasionally APIs throw 5xx - retry politely
        if 500 <= resp.status_code < 600:
            print(f"Server error {resp.status_code}. Sleeping then retrying...")
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        resp.raise_for_status()
        return resp.json()


def fetch_games_for_season(season_start_year: int) -> tuple[list[dict], list[dict]]:
    """
    Fetch ALL games (regular + postseason) for a season using cursor pagination.
    season_start_year example:
      2023 => 2023-2024 season
      2024 => 2024-2025 season
      2025 => 2025-2026 season
    """
    api_key = get_api_key()
    headers = {"Authorization": f"Bearer {api_key}"}

    # Use the season filter only; do NOT filter postseason so we get both.
    # Using start/end date can sometimes lead to confusion for older seasons.
    cursor = 0
    raw_games: list[dict] = []
    summary_rows: list[dict] = []

    print(f"\n=== Ingesting season {season_start_year} (regular + postseason) ===")

    while True:
        params = {
            "seasons[]": season_start_year,
            "per_page": 100,
            "cursor": cursor,
        }

        payload = safe_get(BASE_URL, params=params, headers=headers)

        games = payload.get("data", [])
        meta = payload.get("meta", {}) or {}
        next_cursor = meta.get("next_cursor")

        if not games:
            print(f"Season {season_start_year}: no more games returned. Stopping.")
            break

        raw_games.extend(games)
        summary_rows.extend([summarize_game(g) for g in games])

        print(f"Season {season_start_year}: fetched {len(games)} games (cursor={cursor} -> next_cursor={next_cursor})")

        if next_cursor is None:
            break

        cursor = next_cursor
        time.sleep(REQUEST_DELAY_SECONDS)

    print(f"Season {season_start_year}: total games saved = {len(raw_games)}")
    return raw_games, summary_rows


def write_outputs_for_season(season_start_year: int, raw_games: list[dict], summary_rows: list[dict]) -> None:
    import csv

    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # RAW snapshot
    raw_path = os.path.join(RAW_DIR, f"games_{season_start_year}_{ts}.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump({"data": raw_games}, f, indent=2)
    print(f"Saved RAW -> {raw_path}")

    # Processed CSV (quoted properly)
    csv_path = os.path.join(PROCESSED_DIR, f"games_summary_{season_start_year}_{ts}.csv")
    keys = list(summary_rows[0].keys()) if summary_rows else []

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(summary_rows)
    print(f"Saved SUMMARY CSV -> {csv_path}")

    # Processed Parquet (preferred for data engineering)
    if pd is not None and summary_rows:
        df = pd.DataFrame(summary_rows)
        parquet_path = os.path.join(PROCESSED_DIR, f"games_summary_{season_start_year}_{ts}.parquet")
        try:
            df.to_parquet(parquet_path, index=False)
            print(f"Saved SUMMARY Parquet -> {parquet_path}")
        except Exception as e:
            print(f"(Parquet skipped) Install pyarrow to enable parquet: {e}")


def main():
    seasons = [2023, 2024, 2025]

    for season in seasons:
        raw_games, summary_rows = fetch_games_for_season(season)
        write_outputs_for_season(season, raw_games, summary_rows)

    print("\nAll seasons completed.")


if __name__ == "__main__":
    main()
