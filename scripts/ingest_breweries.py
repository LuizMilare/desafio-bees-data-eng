import requests
import json
import os
import logging
import time
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
ITEMS_PER_PAGE = 200
MAX_PAGES = 1000
MAX_RETRIES = 3
BACKOFF_FACTOR = 2
TIMEOUT_SECONDS = 10
OUTPUT_PATH = "/app/data/bronze"

def fetch_page(page):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                BASE_URL,
                params={"page": page, "per_page": ITEMS_PER_PAGE},
                timeout=TIMEOUT_SECONDS
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout occurred on attempt {attempt}. Retrying...")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error on page {page}: {e} (attempt {attempt}/{MAX_RETRIES}).")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on page {page}: {e} (attempt {attempt}/{MAX_RETRIES}).")

        if attempt < MAX_RETRIES:
            wait = BACKOFF_FACTOR ** attempt
            logger.info(f"Retrying page {page} in {wait}s...")
            time.sleep(wait)
    
    logger.error(f"Page {page} failed after {MAX_RETRIES} attempts. Skipping.")
    return None

def fetch_breweries(ingestion_date: str = None) -> None:
    all_data = []
    failed_pages = []
    page = 1
    path = f"{OUTPUT_PATH}/ingestion_date={ingestion_date}"

    logger.info("Starting brewery ingestion...")

    while page <= MAX_PAGES:
        logger.info(f"Fetching page {page}...")
        data = fetch_page(page)
        
        if data is None:
            failed_pages.append(page)
            page += 1
            continue
        
        if not data:
            logger.info(f"No more data at page {page}. Ingestion complete.")
            break

        all_data.extend(data)
        logger.info(f"Page {page} fetched — {len(data)} records. Total so far: {len(all_data)}.")
        page += 1
    
    if  failed_pages:
        logger.warning(f"Failed to fetch pages: {failed_pages}")

    if not all_data:
        raise ValueError("No data fetched. Ingestion failed.")


    os.makedirs(os.path.dirname(f"{path}/breweries_raw.json"), exist_ok=True)
    with open(f"{path}/breweries_raw.json", "w") as f:
        json.dump(all_data, f, indent=4)

    logger.info(f"Ingestion successful with {len(all_data)} records.")

    if failed_pages:
        raise RuntimeError(f"Ingestion completed with failures on pages: {failed_pages}")

if __name__ == "__main__":
    fetch_breweries()
