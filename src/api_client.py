"""Client utilities to interact with the Bsale API."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv


load_dotenv()


BASE_URL = os.getenv("BSALE_BASE_URL", "https://api.bsale.io/v1").rstrip("/")
TOKEN = os.getenv("BSALE_TOKEN")
BSALE_DEBUG = os.getenv("BSALE_DEBUG", "0") == "1"

if not TOKEN:
    raise RuntimeError("No se encontró BSALE_TOKEN en .env")


HEADERS = {
    "access_token": TOKEN,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

_SESSION = requests.Session()
_SESSION.headers.update(HEADERS)


class BsaleAPIError(RuntimeError):
    """Error raised when the Bsale API returns a non-success response."""


def _log_debug(message: str) -> None:
    if BSALE_DEBUG:
        print(message)


def fetch_bsale_data(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    start_page: int = 1,
    sleep_between_pages: float = 0.2,
) -> List[Dict[str, Any]]:
    """Fetch all items from a Bsale endpoint using limit/offset pagination."""

    all_items: List[Dict[str, Any]] = []
    base_params = dict(params or {})

    limit = int(base_params.get("limit", 50))
    offset = int(base_params.get("offset", (max(start_page, 1) - 1) * limit))

    while True:
        query = dict(base_params)
        query["limit"] = limit
        query["offset"] = offset

        url = f"{BASE_URL}/{endpoint}.json"
        _log_debug(f"GET {url} params={query}")

        try:
            response = _SESSION.get(url, params=query, timeout=30)
        except requests.RequestException as exc:
            raise BsaleAPIError(f"Error de red consultando {url}: {exc}") from exc

        if response.status_code != 200:
            raise BsaleAPIError(
                f"Error {response.status_code} al consultar {url}: {response.text}"
            )

        payload = response.json()
        items: List[Dict[str, Any]]
        if isinstance(payload, dict):
            items = payload.get("items") or []
        elif isinstance(payload, list):
            items = payload
        else:
            items = []

        _log_debug(f"→ página offset={offset} len(items)={len(items)}")

        if not items:
            break

        all_items.extend(items)

        if len(items) < limit:
            break

        offset += limit
        time.sleep(max(sleep_between_pages, 0.0))

    return all_items
