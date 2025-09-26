from api_client import fetch_bsale_data
from utils import save_jsonlist_to_csv, ensure_data_dir
import os

DATA_DIR = "data"
ensure_data_dir(DATA_DIR)

RESOURCES = {
    "documents": os.path.join(DATA_DIR, "ventas.csv"),
    "products": os.path.join(DATA_DIR, "productos.csv"),
    "stocks": os.path.join(DATA_DIR, "stock.csv"),
    "offices": os.path.join(DATA_DIR, "sucursales.csv"),
}

def main():
    for endpoint, out_path in RESOURCES.items():
        print(f"\n🔎 Descargando {endpoint} ...")
        try:
            items = fetch_bsale_data(endpoint)
            save_jsonlist_to_csv(items, out_path)
        except Exception as e:
            print(f"❌ Error al descargar {endpoint}: {e}")

if __name__ == "__main__":
    main()
