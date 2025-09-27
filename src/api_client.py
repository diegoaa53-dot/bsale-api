# src/api_client.py
import os
import time
import requests
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BSALE_BASE_URL", "https://api.bsale.io/v1").rstrip("/")
TOKEN = os.getenv("BSALE_TOKEN")

if not TOKEN:
    raise RuntimeError("No se encontró BSALE_TOKEN en .env")

HEADERS = {"access_token": TOKEN, "Accept": "application/json", "Content-Type": "application/json"}
session = requests.Session()
session.headers.update(HEADERS)

def fetch_bsale_data(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    page_param: str = "page",           # se acepta por compatibilidad
    start_page: int = 1,                # se usa para calcular offset
    sleep_between_pages: float = 0.2,
) -> List[Dict[str, Any]]:
    """
    Descarga todos los registros de un endpoint usando paginación Bsale (limit/offset).
    - endpoint: 'documents', 'products', 'stocks', 'offices', etc. (sin .json)
    - params: puedes pasar 'expand', 'fields', filtros, etc.
    """
    all_items: List[Dict[str, Any]] = []
    base_params = dict(params or {})

    # Bsale pagina con limit/offset. Si te pasan start_page, lo convertimos a offset.
    limit = int(base_params.get("limit", 50))
    offset = int(base_params.get("offset", (max(start_page, 1) - 1) * limit))

    while True:
        q = dict(base_params)
        q["limit"] = limit
        q["offset"] = offset

        url = f"{BASE_URL}/{endpoint}.json"
        resp = session.get(url, params=q, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Error {resp.status_code} al consultar {url}: {resp.text}")

        payload = resp.json()
        items = payload.get("items") if isinstance(payload, dict) else (payload if isinstance(payload, list) else [])

        if not items:
            break

        all_items.extend(items)

        # Si vino menos que el límite, no hay más páginas
        if len(items) < limit:
            break

        offset += limit
        time.sleep(sleep_between_pages)

    return all_items
