# src/main.py
import os
import argparse
from datetime import datetime
from .api_client import fetch_bsale_data
from .utils import ensure_data_dir, build_reporte_ventas

DATA_DIR = "data"
ensure_data_dir(DATA_DIR)

def parse_args():
    p = argparse.ArgumentParser(description="Reporte de Ventas estilo Bsale (detalle por ítem)")
    p.add_argument("--since", type=str, help="YYYY-MM-DD")
    p.add_argument("--until", type=str, help="YYYY-MM-DD")
    p.add_argument("--limit", type=int, default=50, help="tamaño de página (máx 50)")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()

def _to_unix_day_bounds(since_str: str | None, until_str: str | None):
    start_unix = 0
    if since_str:
        d0 = datetime.fromisoformat(since_str)
        start_unix = int(datetime(d0.year, d0.month, d0.day, 0, 0, 0).timestamp())
    if until_str:
        d1 = datetime.fromisoformat(until_str)
        end_unix = int(datetime(d1.year, d1.month, d1.day, 23, 59, 59).timestamp())
    else:
        today = datetime.today()
        end_unix = int(datetime(today.year, today.month, today.day, 23, 59, 59).timestamp())
    return start_unix, end_unix

def main():
    args = parse_args()
    params = {
        "limit": args.limit,
        # relaciones necesarias para el reporte
        "expand": "details,client,office,user,coin,priceList",
        # campos útiles
        "fields": (
            "[id,number,serialNumber,emissionDate,documentTypeId,trackingNumber,token,"
            "netAmount,taxAmount,totalAmount,discountAmount,"
            "client,office,user,coin,priceList,details]"
        ),
    }
    if args.since or args.until:
        s, e = _to_unix_day_bounds(args.since, args.until)
        params["emissiondaterange"] = f"[{s},{e}]"

    print("🔎 Descargando documents ...")
    docs = fetch_bsale_data("documents", params=params, start_page=1)

    if args.debug:
        print(f"ℹ️ documents: {len(docs)} registros")
        if docs:
            sample = {k: docs[0][k] for k in list(docs[0].keys())[:12]}
            print("   ejemplo cabecera:", sample)

    out_path = os.path.join(DATA_DIR, "reporte_ventas.csv")
    build_reporte_ventas(docs, out_path)
    print(f"✅ Reporte generado: {out_path}")

if __name__ == "__main__":
    main()
