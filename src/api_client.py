import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BSALE_BASE_URL", "https://api.bsale.io/v1")
TOKEN = os.getenv("BSALE_TOKEN")

if not TOKEN:
    raise RuntimeError("No se encontró BSALE_TOKEN en .env")

HEADERS = {"access_token": TOKEN, "Content-Type": "application/json"}
session = requests.Session()
session.headers.update(HEADERS)

def fetch_bsale_data(endpoint, params=None, page_param="page", start_page=1, sleep_between_pages=0.2):
    all_items = []
    page = start_page

    while True:
        p = dict(params or {})
        p[page_param] = page
        url = f"{BASE_URL}/{endpoint}.json"
        resp = session.get(url, params=p, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Error {resp.status_code} al consultar {url}: {resp.text}")

        payload = resp.json()

        # intentar encontrar la lista correcta
        items = None
        if isinstance(payload, dict):
            if "items" in payload:
                items = payload["items"]
            else:
                for v in payload.values():
                    if isinstance(v, list):
                        items = v
                        break
        elif isinstance(payload, list):
            items = payload

        if not items:
            break

        all_items.extend(items)

        if isinstance(payload, dict) and (not payload.get("next")):
            break

        if len(items) == 0:
            break

        page += 1
        time.sleep(sleep_between_pages)

    return all_items
