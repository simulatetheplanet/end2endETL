import requests
import json
from datetime import datetime
import os

RAW_DIR = "data/raw"

def fetch_games():
    url = "https://www.balldontlie.io/api/v1/games"
    params = {
        "per_page": 10
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(RAW_DIR, f"games_{timestamp}.json")

    
    os.makedirs(RAW_DIR, exist_ok=True)

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw data to {filename}")

if __name__ == "__main__":
    fetch_games()
