import requests
import json
import os
from typing import List, Dict

def fetch_breweries():
    base_url = "https://api.openbrewerydb.org/v1/breweries"
    
    print("Buscando dados da API...")
    response = requests.get(base_url)

    if response.status_code == 200:
        data = response.json()
        print(f"Sucesso! {len(data)} cervejarias encontradas.")
        return data
    else:
        print(f"Erro na API: {response.status_code}")
        return None

def save_to_bronze(data):
    os.makedirs('data/bronze', exist_ok=True)   

    file_path = 'data/bronze/breweries_raw.json'
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Dados salvos em {file_path}")

if __name__ == "__main__":
    breweries_data = fetch_breweries()
    if breweries_data:
        save_to_bronze(breweries_data)