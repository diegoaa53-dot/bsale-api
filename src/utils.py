import os
import pandas as pd
from pandas import json_normalize

def ensure_data_dir(path="data"):
    os.makedirs(path, exist_ok=True)

def save_jsonlist_to_csv(json_list, filename):
    ensure_data_dir(os.path.dirname(filename) or "data")
    if not json_list:
        print(f"⚠️  No hay datos para {filename}")
        return
    df = json_normalize(json_list)
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"✅ Guardado {filename} — {len(df)} filas")
