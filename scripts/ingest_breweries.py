import requests
import json
import os
from typing import List, Dict

def fetch_breweries():
    base_url = "https://api.openbrewerydb.org/v1/breweries"
    all_data = []
    page = 1
    items_per_page = 200

    while True:
        print(f"Buscando página {page}...")
        response = requests.get(base_url, params={"page": page, "per_page": items_per_page})
    
        if response.status_code != 200:
            print(f"Erro na página {page}: {response.status_code}.")
            break
        
        data = response.json()

        if not data:
            break
        
        all_data.extend(data)
        page += 1

        if page > 1000:
            break

    os.makedirs("/app/data/bronze", exist_ok=True)
    with open("/app/data/bronze/breweries_raw.json", "w") as f:
        json.dump(all_data, f, indent=4)

    print(f"Ingestão concluída com {len(all_data)} registros.")

if __name__ == "__main__":
    fetch_breweries()
