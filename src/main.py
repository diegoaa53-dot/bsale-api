"""CLI entrypoint to generate the Bsale sales report."""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Dict, List, Tuple

from .api_client import BsaleAPIError, fetch_bsale_data
from .utils import build_reporte_ventas, ensure_data_dir


VERSION = "main v5"
DATA_DIR = "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reporte de ventas desde Bsale")
    parser.add_argument("--since", type=str, help="Fecha inicio YYYY-MM-DD", default=None)
    parser.add_argument("--until", type=str, help="Fecha fin YYYY-MM-DD", default=None)
    parser.add_argument("--limit", type=int, default=50, help="Tamaño de página (máx 50 en Bsale)")
    parser.add_argument("--out", type=str, help="Ruta del CSV de salida", default=None)
    parser.add_argument("--debug", action="store_true", help="Imprime información adicional")
    return parser.parse_args()


def _to_unix_day_bounds(since: str | None, until: str | None) -> Tuple[int, int]:
    today = datetime.today()
    start_source = since or until or today.strftime("%Y-%m-%d")
    end_source = until or since or today.strftime("%Y-%m-%d")

    start_date = datetime.fromisoformat(start_source)
    end_date = datetime.fromisoformat(end_source)

    start = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
    end = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)
    return int(start.timestamp()), int(end.timestamp())


def _build_params(args: argparse.Namespace, include_document_type: bool = True) -> Dict[str, str]:
    expand_parts = ["details", "client", "office", "user", "coin", "priceList"]
    if include_document_type:
        expand_parts.append("document_type")

    params: Dict[str, str] = {
        "limit": str(args.limit),
        "expand": ",".join(expand_parts),
        "fields": (
            "[id,number,emissionDate,documentTypeId,trackingNumber,token," \
            "netAmount,taxAmount,totalAmount,totalDiscount," \
            "client,office,user,coin,priceList,details]"
        ),
    }

    if args.since or args.until:
        start_unix, end_unix = _to_unix_day_bounds(args.since, args.until)
        params["emissiondaterange"] = f"[{start_unix},{end_unix}]"

    return params


def inspect_one(documents: List[Dict]) -> None:
    if not documents:
        print("ℹ️ Sin documentos para inspeccionar")
        return

    first = documents[0]
    header_keys = list(first.keys())
    print(f"   keys cabecera: {header_keys}")

    relations = [key for key, value in first.items() if isinstance(value, dict)]
    print(f"   relaciones disponibles: {relations}")

    details = first.get("details", {})
    items = details.get("items") if isinstance(details, dict) else None
    if isinstance(items, list) and items:
        print(f"   primer ítem: {items[0]}")
        variant = items[0].get("variant", {})
        if isinstance(variant, dict):
            print(f"   claves variant: {list(variant.keys())}")


def main() -> None:
    args = parse_args()

    ensure_data_dir(DATA_DIR)

    if args.debug:
        print("▶ Ejecutando", VERSION)

    params = _build_params(args, include_document_type=True)

    if args.debug and "emissiondaterange" in params:
        print(f"   emissiondaterange={params['emissiondaterange']}")

    try:
        documents = fetch_bsale_data("documents", params=params)
    except BsaleAPIError as exc:
        if "document_type" in str(exc) and "expand" in str(exc):
            if args.debug:
                print("⚠️ expand=document_type no soportado, reintentando sin esa relación")
            params = _build_params(args, include_document_type=False)
            documents = fetch_bsale_data("documents", params=params)
        else:
            raise

    if args.debug:
        print(f"ℹ️ documents: {len(documents)}")
        inspect_one(documents)

    out_path = args.out
    if not out_path:
        since = args.since or "full"
        until = args.until or "full"
        filename = f"reporte_ventas_{since}_{until}.csv"
        out_path = os.path.join(DATA_DIR, filename)

    build_reporte_ventas(documents, out_path)
    print(f"✅ Reporte generado: {out_path}")


if __name__ == "__main__":
    main()
