import os
import pandas as pd
from pandas import json_normalize
from datetime import datetime


def ensure_data_dir(path: str = "data"):
    os.makedirs(path, exist_ok=True)


def _fmt_date(unix_series):
    """Convierte unix (segundos) a dd/mm/YYYY; tolera None/objetos."""
    try:
        dt = pd.to_datetime(unix_series, unit="s", errors="coerce")
        return dt.dt.strftime("%d/%m/%Y")
    except Exception:
        return pd.Series([None] * len(unix_series)) if hasattr(unix_series, "__len__") else None


def _fmt_datetime(unix_series):
    """Convierte unix (segundos) a dd/mm/YYYY HH:MM:SS; tolera None/objetos."""
    try:
        dt = pd.to_datetime(unix_series, unit="s", errors="coerce")
        return dt.dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return pd.Series([None] * len(unix_series)) if hasattr(unix_series, "__len__") else None


def build_reporte_ventas(documents: list[dict], out_csv: str):
    """
    Genera un CSV con los encabezados del reporte de ventas (detalle por ítem)
    que nos pasaste. REQUIERE extraer documents con:
      expand=details,client,office,user,coin,priceList
    """
    ensure_data_dir(os.path.dirname(out_csv) or "data")

    if not documents:
        pd.DataFrame().to_csv(out_csv, index=False, encoding="utf-8-sig")
        print("⚠️ No llegaron documentos.")
        return

    # ---------- Aplanar items ----------
    df_items = json_normalize(
        documents,
        record_path=["details", "items"],
        meta=[
            "id", "number", "serialNumber", "emissionDate", "documentTypeId", "trackingNumber", "token",
            ["client", "firstName"], ["client", "lastName"], ["client", "company"], ["client", "code"], ["client", "email"],
            ["client", "address"], ["client", "municipality"], ["client", "city"],
            ["office", "name"], ["user", "name"], ["coin", "code"], ["priceList", "name"],
        ],
        sep=".",
        errors="ignore",
        meta_prefix="doc."
    )

    # ---------- Construir dataframe final con tus columnas (y orden) ----------
    cols = {
        "Tipo Movimiento": "venta",
        "Tipo de Documento": None,         # luego podemos hacer lookup al nombre por documentTypeId
        "Numero Documento": "doc.number",
        "Fecha de Emisión": None,          # derivado de doc.emissionDate
        "Tracking number": None,           # trackingNumber o token
        "Fecha y Hora Venta": None,        # derivado de doc.emissionDate
        "Sucursal": "doc.office.name",
        "Vendedor": "doc.user.name",
        "Nombre Cliente": None,            # company o nombre+apellido
        "Cliente RUT": "doc.client.code",
        "Email Cliente": "doc.client.email",
        "Cliente Dirección": "doc.client.address",
        "Cliente Comuna": "doc.client.municipality",
        "Cliente Ciudad": "doc.client.city",
        "Lista de Precio": "doc.priceList.name",
        "Tipo de entrega": "",             # no disponible en documents
        "Moneda": "doc.coin.code",
        "Tipo de Producto / Servicio": 0,  # en tu muestra es 0
        "SKU": "variant.code",
        "Producto / Servicio": "variant.description",
        "Variante": "",
        "Otros Atributos": "",
        "Marca": "",
        "Detalle de Productos/Servicios Pack/Promo": "",
        "Precio de Lista": None,           # si no viene, fallback = totalUnitValue
        "Precio Neto Unitario": "netUnitValue",
        "Precio Bruto Unitario": "totalUnitValue",
        "Cantidad": "quantity",
        "Venta Total Neta": "netAmount",
        "Total Impuestos": "taxAmount",
        "Venta Total Bruta": "totalAmount",
        "Nombre de dcto": "",
        "Descuento Neto": "netDiscount",
        "Descuento Bruto": "totalDiscount",
        "% Descuento": None,               # derivado
        "Costo neto unitario": "",
        "Costo Total Neto": "",
        "Margen": "",
        "% Margen": "",
    }

    out = pd.DataFrame(index=df_items.index)

    # Cargar columnas directas / constantes
    for col, src in cols.items():
        if src is None:
            out[col] = ""   # las completamos después
        elif src == "":
            out[col] = ""
        elif isinstance(src, (int, float)):
            out[col] = src
        else:
            out[col] = df_items.get(src, "")

    # ---------- Derivados / limpiezas ----------
    # Tipo de Documento: por ahora dejamos el ID
    out["Tipo de Documento"] = df_items.get("doc.documentTypeId", "")

    # Fechas
    em = df_items.get("doc.emissionDate")
    out["Fecha de Emisión"] = _fmt_date(em)
    out["Fecha y Hora Venta"] = _fmt_datetime(em)

    # Tracking: combinar trackingNumber con token (sin usar `or` sobre Series)
    track = df_items.get("doc.trackingNumber")
    tok = df_items.get("doc.token")

    if track is None:
        track = pd.Series([pd.NA] * len(out), index=out.index)
    else:
        track = track.reindex(out.index)

    if tok is None:
        tok = pd.Series([pd.NA] * len(out), index=out.index)
    else:
        tok = tok.reindex(out.index)

    out["Tracking number"] = track.fillna(tok).fillna("")

    # Nombre Cliente: company o nombre+apellido
    comp = df_items.get("doc.client.company")
    first = df_items.get("doc.client.firstName")
    last = df_items.get("doc.client.lastName")
    full = ((first.fillna("") + " " + last.fillna("")).str.strip()) if (first is not None and last is not None) else None
    out["Nombre Cliente"] = (comp.fillna(full) if (comp is not None and full is not None) else (comp or full or ""))

    # Moneda: default CLP si viene vacía
    out["Moneda"] = out["Moneda"].replace("", "CLP")

    # Precio de Lista: convertir "" -> NaN y luego rellenar con Bruto Unitario
    bruto_u = pd.to_numeric(
        df_items.get("totalUnitValue", pd.Series([pd.NA] * len(out), index=out.index)),
        errors="coerce"
    ).reindex(out.index)
    pl = out["Precio de Lista"]
    if pl.dtype == "O":
        pl = pl.replace("", pd.NA)
    pl = pd.to_numeric(pl, errors="coerce")
    out["Precio de Lista"] = pl.fillna(bruto_u)

    # % Descuento (evita div/0)
    net_disc = pd.to_numeric(df_items.get("netDiscount", 0), errors="coerce").fillna(0)
    tot_disc = pd.to_numeric(df_items.get("totalDiscount", 0), errors="coerce").fillna(0)
    tot_linea = pd.to_numeric(df_items.get("totalAmount", 0), errors="coerce").fillna(0)
    base = (tot_linea + tot_disc).replace(0, pd.NA)
    pct = ((tot_disc / base) * 100).round(0).fillna(0).astype(int).astype(str) + "%"
    out["% Descuento"] = pct

    # Orden final de columnas
    out = out[list(cols.keys())]

    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Reporte generado en {out_csv} — {len(out)} filas")


def save_documents_to_csvs(documents, base_dir="data"):
    """
    Utilidad alternativa: guarda cabecera y detalle "tal cual" sin esquema de reporte.
    - data/ventas.csv
    - data/ventas_detalle.csv
    """
    os.makedirs(base_dir, exist_ok=True)

    if not documents:
        print("⚠️  No llegaron documentos para guardar")
        pd.DataFrame().to_csv(os.path.join(base_dir, "ventas.csv"), index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(os.path.join(base_dir, "ventas_detalle.csv"), index=False, encoding="utf-8-sig")
        return

    # Cabecera
    df_head = json_normalize(documents, sep=".")
    if "emissionDate" in df_head.columns:
        try:
            df_head["emissionDate_dt"] = pd.to_datetime(df_head["emissionDate"], unit="s", errors="ignore")
        except Exception:
            pass

    head_path = os.path.join(base_dir, "ventas.csv")
    df_head.to_csv(head_path, index=False, encoding="utf-8-sig")
    print(f"✅ Guardado {head_path} — {len(df_head)} filas")

    # Detalle
    try:
        df_det = json_normalize(
            documents,
            record_path=["details", "items"],
            meta=[["id"], ["number"], "emissionDate",
                  ["client", "firstName"], ["client", "lastName"], ["client", "company"],
                  ["office", "name"]],
            sep=".",
            errors="ignore",
            meta_prefix="document."
        )
        rename_map = {
            "document.id": "documentId",
            "document.number": "documentNumber",
            "document.client.firstName": "client.firstName",
            "document.client.lastName": "client.lastName",
            "document.client.company": "client.company",
            "document.office.name": "office.name",
            "document.emissionDate": "emissionDate"
        }
        df_det.rename(columns=rename_map, inplace=True, errors="ignore")
        if "emissionDate" in df_det.columns:
            try:
                df_det["emissionDate_dt"] = pd.to_datetime(df_det["emissionDate"], unit="s", errors="ignore")
            except Exception:
                pass

        detail_path = os.path.join(base_dir, "ventas_detalle.csv")
        df_det.to_csv(detail_path, index=False, encoding="utf-8-sig")
        print(f"✅ Guardado {detail_path} — {len(df_det)} filas")
    except Exception as e:
        detail_path = os.path.join(base_dir, "ventas_detalle.csv")
        pd.DataFrame().to_csv(detail_path, index=False, encoding="utf-8-sig")
        print(f"⚠️  No se pudo aplanar details.items → {e}. Guardé un archivo vacío en ventas_detalle.csv")


def save_jsonlist_to_csv(json_list, filename):
    ensure_data_dir(os.path.dirname(filename) or "data")
    if not json_list:
        pd.DataFrame().to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"⚠️  No hay datos para {filename}")
        return
    df = json_normalize(json_list, sep=".")
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"✅ Guardado {filename} — {len(df)} filas")
