# src/catalogs.py
import os, json
import pandas as pd
from typing import Dict, Tuple

try:
    from .api_client import fetch_bsale_data
except Exception:
    from api_client import fetch_bsale_data

CACHE_DIR = "data/cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _cache_path(name: str) -> str:
    return os.path.join(CACHE_DIR, f"{name}.json")

def _load_cache(name: str):
    p = _cache_path(name)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def _save_cache(name: str, data):
    with open(_cache_path(name), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def _fetch_all(endpoint: str, fields: str | None = None, extra_params: Dict | None = None) -> list[dict]:
    params = {"limit": 50}
    if fields:
        params["fields"] = fields
    if extra_params:
        params.update(extra_params)
    return fetch_bsale_data(endpoint, params=params)

# ---------------- Catalog maps: nombres legibles ----------------

def get_document_types_map(refresh: bool = False) -> Dict[int, str]:
    name = "document_types"
    if not refresh:
        c = _load_cache(name)
        if c: return {int(k): v for k, v in c.items()}
    rows = _fetch_all("document_types", fields="[id,name]")
    m = {int(r["id"]): r.get("name","") for r in rows if r.get("id") is not None}
    _save_cache(name, {str(k): v for k, v in m.items()})
    return m

def get_price_lists_map(refresh: bool = False) -> Dict[int, str]:
    name = "price_lists"
    if not refresh:
        c = _load_cache(name)
        if c: return {int(k): v for k, v in c.items()}
    rows = _fetch_all("price_lists", fields="[id,name]")
    m = {int(r["id"]): r.get("name","") for r in rows if r.get("id") is not None}
    _save_cache(name, {str(k): v for k, v in m.items()})
    return m

def get_users_map(refresh: bool = False) -> Dict[int, str]:
    """
    Algunas cuentas no exponen 'name' y traen 'firstName'/'lastName'.
    Construimos full_name = firstName + lastName.
    """
    name = "users"
    if not refresh:
        c = _load_cache(name)
        if c: return {int(k): v for k, v in c.items()}
    rows = _fetch_all("users")  # campos varían por cuenta
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
    _save_cache(name, {str(k): v for k, v in m.items()})
    return m

def get_offices_map(refresh: bool = False) -> Dict[int, str]:
    name = "offices"
    if not refresh:
        c = _load_cache(name)
        if c: return {int(k): v for k, v in c.items()}
    rows = _fetch_all("offices", fields="[id,name]")
    m = {int(r["id"]): r.get("name","") for r in rows if r.get("id") is not None}
    _save_cache(name, {str(k): v for k, v in m.items()})
    return m

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
            df = pd.read_json(cache_path, orient="records", dtype=False)
            return df
        except Exception:
            pass

    # Traemos variants. En muchas cuentas están bajo /variants. En otras, via /products expand=variants.
    # Partimos por /variants directamente.
    rows = _fetch_all("variants")  # si tu API requiere fields, se puede afinar

    # Normalizamos a dataframe
    df = pd.json_normalize(rows, sep=".")
    # Aseguramos columnas clave
    if "id" not in df.columns:
        df["id"] = pd.NA
    if "code" not in df.columns:
        df["code"] = pd.NA
    if "description" not in df.columns:
        df["description"] = pd.NA

    # Detectamos el mejor campo para costo
    possible_cost_cols = [
        "cost", "costPrice", "netCost", "lastPurchasePrice", "averageCost",
        "prices.cost", "unitCost", "purchasePrice"
    ]
    cost_col = None
    for c in possible_cost_cols:
        if c in df.columns and df[c].notna().any():
            cost_col = c
            break

    if cost_col is None:
        # No hay costo, devolvemos 0.0
        df["cost_net_unit"] = 0.0
    else:
        df["cost_net_unit"] = pd.to_numeric(df[cost_col], errors="coerce").fillna(0.0)

    out = pd.DataFrame({
        "variant_id": pd.to_numeric(df["id"], errors="coerce"),
        "sku": df["code"].astype(str),
        "variant_description": df["description"].astype(str),
        "cost_net_unit": pd.to_numeric(df["cost_net_unit"], errors="coerce").fillna(0.0)
    })

    # guardamos cache como json
    out.to_json(cache_path, orient="records", force_ascii=False)
    return out

def get_all_maps(refresh: bool = False) -> Tuple[Dict[int,str], Dict[int,str], Dict[int,str], Dict[int,str], pd.DataFrame]:
    return (
        get_document_types_map(refresh),
        get_users_map(refresh),
        get_price_lists_map(refresh),
        get_offices_map(refresh),
        get_variants_dim(refresh),
    )
