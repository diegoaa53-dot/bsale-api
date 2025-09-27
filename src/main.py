# src/main.py
import os
import argparse
from datetime import datetime
from .api_client import fetch_bsale_data
from .utils import save_jsonlist_to_csv, ensure_data_dir

DATA_DIR = "data"
ensure_data_dir(DATA_DIR)

RESOURCES = {
    "documents": os.path.join(DATA_DIR, "ventas.csv"),
    "products": os.path.join(DATA_DIR, "productos.csv"),
    "stocks": os.path.join(DATA_DIR, "stock.csv"),
    "offices": os.path.join(DATA_DIR, "sucursales.csv"),
}

def parse_args():
    p = argparse.ArgumentParser(description="Extractor Bsale")
    p.add_argument("--only", choices=list(RESOURCES.keys()),
                   help="Solo un endpoint (documents/products/stocks/offices)")
    p.add_argument("--limit", type=int, default=None,
                   help="Tamaño de página (override). Ej: --limit 50 (máx 50 en Bsale)")
    p.add_argument("--page", type=int, default=1,
                   help="Página inicial (se traduce a offset). Ej: --page 1")
    p.add_argument("--debug", action="store_true",
                   help="Muestra conteo y ejemplo de campos")
    # Fechas para DOCUMENTS
    p.add_argument("--since", type=str, help="Fecha inicio (YYYY-MM-DD)")
    p.add_argument("--until", type=str, help="Fecha fin (YYYY-MM-DD). Si no se da, se usa hoy.")
    return p.parse_args()

def _to_unix_day_bounds(since_str: str | None, until_str: str | None):
    """Convierte YYYY-MM-DD a timestamps Unix (00:00:00 y 23:59:59)."""
    start_unix = None
    if since_str:
        d0 = datetime.fromisoformat(since_str)
        start_unix = int(datetime(d0.year, d0.month, d0.day, 0, 0, 0).timestamp())

    if until_str:
        d1 = datetime.fromisoformat(until_str)
        end_unix = int(datetime(d1.year, d1.month, d1.day, 23, 59, 59).timestamp())
    else:
        today = datetime.today()
        end_unix = int(datetime(today.year, today.month, today.day, 23, 59, 59).timestamp())

    if start_unix is None:
        start_unix = 0  # “desde siempre” si no se especifica since

    return start_unix, end_unix

def _build_params(endpoint: str, args) -> dict:
    """Construye los params de consulta según endpoint y flags."""
    params = {}
    if args.limit:
        params["limit"] = args.limit

    if endpoint == "documents":
        # Si se pasan fechas, aplicamos rango; si no, sin filtro (traería todo).
        if args.since or args.until:
            start_unix, end_unix = _to_unix_day_bounds(args.since, args.until)
            params["emissiondaterange"] = f"[{start_unix},{end_unix}]"

        # Sugerencia: traer lo justo para revisar mejor
        params.setdefault("expand", "details,client,office")
        params.setdefault("fields", "[emissionDate,totalAmount,number,client,office,documentTypeId]")

    return params

def main():
    args = parse_args()
    targets = {args.only: RESOURCES[args.only]} if args.only else RESOURCES

    for endpoint, out_path in targets.items():
        print(f"\n🔎 Descargando {endpoint} ...")
        try:
            params = _build_params(endpoint, args)
            if endpoint == "documents" and "emissiondaterange" not in params:
                print("⚠️  Sin filtro de fechas en documents: puede tardar bastante (trae todo). "
                      "Sugerencia: usa --since y --until (YYYY-MM-DD).")

            items = fetch_bsale_data(endpoint, params=params, start_page=args.page)

            if args.debug:
                print(f"ℹ️ {endpoint}: {len(items)} registros")
                if endpoint == "documents" and "emissiondaterange" in params:
                    print(f"   Filtro usado: emissiondaterange={params['emissiondaterange']}")
                if items:
                    # muestra campos de ejemplo
                    sample = {k: items[0][k] for k in list(items[0].keys())[:12]}
                    print(f"   Ejemplo campos: {sample}")

            save_jsonlist_to_csv(items, out_path)
        except Exception as e:
            print(f"❌ Error al descargar {endpoint}: {e}")

if __name__ == "__main__":
    main()
