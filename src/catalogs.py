"""Catalog utilities and dimensional data loaders."""

from __future__ import annotations

import json
import os
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from .api_client import BsaleAPIError, fetch_bsale_data


CACHE_DIR = os.path.join("data", "cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(name: str) -> str:
    return os.path.join(CACHE_DIR, f"{name}.json")


def _load_cache(name: str):
    path = _cache_path(name)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return None


def _save_cache(name: str, data) -> None:
    with open(_cache_path(name), "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False)


def _fetch_all(
    endpoint: str,
    *,
    fields: str | None = None,
    extra_params: Dict[str, str] | None = None,
) -> List[Dict]:
    params: Dict[str, str] = {"limit": "50"}
    if fields:
        params["fields"] = fields
    if extra_params:
        params.update(extra_params)
    return fetch_bsale_data(endpoint, params=params)

# ---------------- Catalog maps: nombres legibles ----------------

def get_document_types_map(refresh: bool = False) -> Dict[int, str]:
    if not refresh:
        cached = _load_cache("document_types")
        if cached:
            try:
                return {int(k): str(v) if v is not None else "" for k, v in cached.items()}
            except (ValueError, AttributeError):
                pass

    rows = _fetch_all("document_types", fields="[id,name]")
    mapping = {
        int(row["id"]): str(row.get("name", ""))
        for row in rows
        if row.get("id") is not None
    }
    _save_cache("document_types", {str(k): v for k, v in mapping.items()})
    return mapping

def get_price_lists_map(refresh: bool = False) -> Dict[int, str]:
    if not refresh:
        cached = _load_cache("price_lists")
        if cached:
            try:
                return {int(k): str(v) if v is not None else "" for k, v in cached.items()}
            except (ValueError, AttributeError):
                pass

    rows = _fetch_all("price_lists", fields="[id,name]")
    mapping = {
        int(row["id"]): str(row.get("name", ""))
        for row in rows
        if row.get("id") is not None
    }
    _save_cache("price_lists", {str(k): v for k, v in mapping.items()})
    return mapping

def get_users_map(refresh: bool = False) -> Dict[int, str]:
    """
    Algunas cuentas no exponen 'name' y traen 'firstName'/'lastName'.
    Construimos full_name = firstName + lastName.
    """
    cache_name = "users"
    if not refresh:
        cached = _load_cache(cache_name)
        if cached:
            try:
                return {int(k): str(v) if v is not None else "" for k, v in cached.items()}
            except (ValueError, AttributeError):
                pass

    rows = _fetch_all("users")
    m: Dict[int, str] = {}
    for r in rows:
        uid = r.get("id")
        if uid is None:
            continue
        full = r.get("name")
        if not full:
            fn = r.get("firstName") or ""
            ln = r.get("lastName") or ""
            full = f"{fn} {ln}".strip()
        m[int(uid)] = full or ""
    _save_cache(cache_name, {str(k): v for k, v in m.items()})
    return m

def get_offices_map(refresh: bool = False) -> Dict[int, str]:
    if not refresh:
        cached = _load_cache("offices")
        if cached:
            try:
                return {int(k): str(v) if v is not None else "" for k, v in cached.items()}
            except (ValueError, AttributeError):
                pass

    rows = _fetch_all("offices", fields="[id,name]")
    mapping = {
        int(row["id"]): str(row.get("name", ""))
        for row in rows
        if row.get("id") is not None
    }
    _save_cache("offices", {str(k): v for k, v in mapping.items()})
    return mapping

# ---------------- dim_variant con costos ----------------

def get_variants_dim(refresh: bool = False) -> pd.DataFrame:
    """
    Devuelve un DataFrame con variantes (SKU) y costo unitario neto si está disponible.
    Intentamos varios nombres de campo de costo (dependen de la cuenta):
    - 'cost', 'costPrice', 'netCost', 'lastPurchasePrice', 'averageCost'
    Si no hay costo disponible, deja 0.0
    """
    cache_name = "variants_dim"
    cache_path = _cache_path(cache_name)
    if not refresh and os.path.exists(cache_path):
        try:
            return pd.read_json(cache_path, orient="records", dtype=False)
        except ValueError:
            pass

    try:
        rows = _fetch_all("variants")
    except BsaleAPIError:
        # Endpoint no disponible: devolvemos dataframe vacío con costos 0.
        df_fallback = pd.DataFrame(
            {"variant_id": pd.Series(dtype="float"), "sku": pd.Series(dtype="object"),
             "variant_description": pd.Series(dtype="object"), "cost_net_unit": pd.Series(dtype="float")}
        )
        df_fallback.to_json(cache_path, orient="records", force_ascii=False)
        return df_fallback

    df = pd.json_normalize(rows, sep=".")
    if df.empty:
        df["id"] = pd.Series(dtype="float")
        df["code"] = pd.Series(dtype="object")
        df["description"] = pd.Series(dtype="object")

    for required in ("id", "code", "description"):
        if required not in df.columns:
            df[required] = pd.NA

    possible_cost_cols: Iterable[str] = (
        "cost",
        "costPrice",
        "netCost",
        "lastPurchasePrice",
        "averageCost",
        "prices.cost",
        "unitCost",
        "purchasePrice",
    )
    cost_col = next((col for col in possible_cost_cols if col in df.columns and df[col].notna().any()), None)

    if cost_col is None:
        df["cost_net_unit"] = 0.0
    else:
        df["cost_net_unit"] = pd.to_numeric(df[cost_col], errors="coerce").fillna(0.0)

    result = pd.DataFrame(
        {
            "variant_id": pd.to_numeric(df["id"], errors="coerce"),
            "sku": df["code"].astype(str).where(df["code"].notna(), ""),
            "variant_description": df["description"].astype(str).where(df["description"].notna(), ""),
            "cost_net_unit": pd.to_numeric(df["cost_net_unit"], errors="coerce").fillna(0.0),
        }
    )

    result.to_json(cache_path, orient="records", force_ascii=False)
    return result

def get_all_maps(refresh: bool = False) -> Tuple[Dict[int,str], Dict[int,str], Dict[int,str], Dict[int,str], pd.DataFrame]:
    return (
        get_document_types_map(refresh),
        get_users_map(refresh),
        get_price_lists_map(refresh),
        get_offices_map(refresh),
        get_variants_dim(refresh),
    )
