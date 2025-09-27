import os
import pandas as pd
from pandas import json_normalize

# Cargar catálogos y dim_variant (con costos)
try:
    from .catalogs import get_all_maps     # cuando corres: python -m src.main
except ImportError:
    from catalogs import get_all_maps      # fallback si ejecutaras archivos sueltos

# --------------------- Helpers ---------------------

def ensure_data_dir(path: str = "data"):
    os.makedirs(path, exist_ok=True)

def _as_series(df: pd.DataFrame, col: str, index) -> pd.Series:
    s = df.get(col)
    if s is None:
        return pd.Series([pd.NA] * len(index), index=index)
    return s.reindex(index)

def _as_num(df_or_series, col_or_series, index) -> pd.Series:
    if isinstance(col_or_series, pd.Series):
        s = col_or_series
    else:
        s = _as_series(df_or_series, col_or_series, index)
    return pd.to_numeric(s, errors="coerce").astype(float).fillna(0.0)

def _fmt_date(unix_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(unix_series, unit="s", errors="coerce")
    return dt.dt.strftime("%d/%m/%Y")

def _fmt_datetime(unix_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(unix_series, unit="s", errors="coerce")
    return dt.dt.strftime("%d/%m/%Y %H:%M:%S")

# --------------------- Reporte de Ventas ---------------------

def build_reporte_ventas(documents: list[dict], out_csv: str):
    """
    Genera un CSV con los encabezados del reporte de ventas (detalle por ítem).
    Requiere extraer documents con:
      expand=details,client,office,user,coin,priceList
    """
    ensure_data_dir(os.path.dirname(out_csv) or "data")

    if not documents:
        pd.DataFrame().to_csv(out_csv, index=False, encoding="utf-8-sig")
        print("⚠️ No llegaron documentos.")
        return

    # Aplanar items (cada fila = un detalle)
    df_items = json_normalize(
        documents,
        record_path=["details", "items"],
        meta=[
            "id", "number", "serialNumber", "emissionDate", "documentTypeId",
            "trackingNumber", "token",
            ["client", "firstName"], ["client", "lastName"], ["client", "company"],
            ["client", "code"], ["client", "email"],
            ["client", "address"], ["client", "municipality"], ["client", "city"],
            ["office", "name"],
            ["user", "id"], ["user", "name"],
            ["coin", "code"],
            ["priceList", "id"], ["priceList", "name"],
            ["variant", "id"], ["variant", "code"], ["variant", "description"],
        ],
        sep=".",
        errors="ignore",
        meta_prefix="doc."
    )

    # ------- Catálogos y dim_variant -------
    doc_type_map, user_map, price_list_map, office_map, dim_variant = get_all_maps(refresh=False)
    idx = df_items.index

    # Lookups nombres
    doc_type_name = _as_series(df_items, "doc.documentTypeId", idx).map(
        lambda x: doc_type_map.get(int(x), "") if pd.notna(x) else ""
    )
    user_name_from_map = _as_series(df_items, "doc.user.id", idx).map(
        lambda x: user_map.get(int(x), "") if pd.notna(x) else ""
    )
    pricelist_name_from_map = _as_series(df_items, "doc.priceList.id", idx).map(
        lambda x: price_list_map.get(int(x), "") if pd.notna(x) else ""
    )

    # dim_variant por join (por id o por sku como fallback)
    # Primero intentamos por variant.id
    var_id_series = _as_series(df_items, "doc.variant.id", idx)
    joined = pd.DataFrame({"__row__": idx})
    joined["variant_id"] = pd.to_numeric(var_id_series, errors="coerce")

    dim = dim_variant.copy()
    # Merge por variant_id
    joined = joined.merge(dim, how="left", on="variant_id")

    # Fallback: si cost_net_unit quedó NaN, intentar por SKU
    missing_cost_mask = joined["cost_net_unit"].isna() | (joined["cost_net_unit"] == 0)
    if missing_cost_mask.any():
        sku_series = _as_series(df_items, "doc.variant.code", idx).astype(str)
        dim_by_sku = dim_variant[["sku", "cost_net_unit"]].dropna()
        joined.loc[missing_cost_mask, "sku"] = sku_series[missing_cost_mask]
        joined = joined.merge(dim_by_sku.rename(columns={"cost_net_unit": "cost_by_sku"}),
                              how="left", on="sku")
        # si cost_net_unit es 0/NaN y cost_by_sku existe, úsala
        joined["cost_net_unit"] = joined["cost_net_unit"].fillna(0.0)
        joined["cost_net_unit"] = joined["cost_net_unit"].where(joined["cost_net_unit"] > 0,
                                                                joined["cost_by_sku"].fillna(0.0))
    # Serie costo final alineada
    cost_unit_variant = joined.set_index("__row__")["cost_net_unit"].reindex(idx).fillna(0.0)

    # Columnas y orden final (tus encabezados)
    cols = [
        "Tipo Movimiento",
        "Tipo de Documento",
        "Numero Documento",
        "Fecha de Emisión",
        "Tracking number",
        "Fecha y Hora Venta",
        "Sucursal",
        "Vendedor",
        "Nombre Cliente",
        "Cliente RUT",
        "Email Cliente",
        "Cliente Dirección",
        "Cliente Comuna",
        "Cliente Ciudad",
        "Lista de Precio",
        "Tipo de entrega",
        "Moneda",
        "Tipo de Producto / Servicio",
        "SKU",
        "Producto / Servicio",
        "Variante",
        "Otros Atributos",
        "Marca",
        "Detalle de Productos/Servicios Pack/Promo",
        "Precio de Lista",
        "Precio Neto Unitario",
        "Precio Bruto Unitario",
        "Cantidad",
        "Venta Total Neta",
        "Total Impuestos",
        "Venta Total Bruta",
        "Nombre de dcto",
        "Descuento Neto",
        "Descuento Bruto",
        "% Descuento",
        "Costo neto unitario",
        "Costo Total Neto",
        "Margen",
        "% Margen",
    ]

    out = pd.DataFrame(index=idx, columns=cols)

    # Constantes
    out["Tipo Movimiento"] = "venta"
    out["Tipo de entrega"] = ""
    out["Variante"] = ""
    out["Otros Atributos"] = ""
    out["Marca"] = ""
    out["Detalle de Productos/Servicios Pack/Promo"] = ""
    out["Nombre de dcto"] = ""
    out["Tipo de Producto / Servicio"] = 0

    # Directos / enriquecidos
    out["Numero Documento"]  = _as_series(df_items, "doc.number", idx)

    suc = _as_series(df_items, "doc.office.name", idx)
    out["Sucursal"] = suc  # si quieres fallback por office_id + office_map, se puede agregar

    # Vendedor: expand.name -> users map
    vend_expand = _as_series(df_items, "doc.user.name", idx)
    vend = vend_expand.where(~vend_expand.isna() & (vend_expand != ""), user_name_from_map)
    out["Vendedor"] = vend.fillna("")

    # Cliente
    out["Cliente RUT"]       = _as_series(df_items, "doc.client.code", idx)
    out["Email Cliente"]     = _as_series(df_items, "doc.client.email", idx)
    out["Cliente Dirección"] = _as_series(df_items, "doc.client.address", idx)
    out["Cliente Comuna"]    = _as_series(df_items, "doc.client.municipality", idx)
    out["Cliente Ciudad"]    = _as_series(df_items, "doc.client.city", idx)
    comp  = _as_series(df_items, "doc.client.company", idx)
    first = _as_series(df_items, "doc.client.firstName", idx).fillna("")
    last  = _as_series(df_items, "doc.client.lastName", idx).fillna("")
    full  = (first + " " + last).str.strip()
    out["Nombre Cliente"] = comp.where(~comp.isna() & (comp != ""), full)

    # Lista de precio: expand.name -> price_lists map
    lp_expand = _as_series(df_items, "doc.priceList.name", idx)
    lp = lp_expand.where(~lp_expand.isna() & (lp_expand != ""), pricelist_name_from_map)
    out["Lista de Precio"] = lp.fillna("")

    # Moneda
    out["Moneda"] = _as_series(df_items, "doc.coin.code", idx).replace("", "CLP")

    # Tipo de Documento: nombre por map; si falta, ID como string
    tipo_doc_name = doc_type_name
    falta = tipo_doc_name.isna() | (tipo_doc_name == "")
    tipo_doc_name = tipo_doc_name.where(~falta, _as_series(df_items, "doc.documentTypeId", idx).astype("Int64").astype(str))
    out["Tipo de Documento"] = tipo_doc_name.fillna("")

    # Fechas
    em = _as_series(df_items, "doc.emissionDate", idx)
    out["Fecha de Emisión"]   = _fmt_date(em)
    out["Fecha y Hora Venta"] = _fmt_datetime(em)

    # Tracking: trackingNumber -> token -> ""
    track = _as_series(df_items, "doc.trackingNumber", idx)
    tok   = _as_series(df_items, "doc.token", idx)
    out["Tracking number"] = track.fillna(tok).fillna("")

    # Detalle (ítem)
    out["SKU"]                 = _as_series(df_items, "doc.variant.code", idx)
    out["Producto / Servicio"] = _as_series(df_items, "doc.variant.description", idx)

    # Númericos (línea)
    out["Precio Neto Unitario"]  = _as_num(df_items, "netUnitValue", idx)
    out["Precio Bruto Unitario"] = _as_num(df_items, "totalUnitValue", idx)
    out["Cantidad"]              = _as_num(df_items, "quantity", idx)
    out["Venta Total Neta"]      = _as_num(df_items, "netAmount", idx)
    out["Total Impuestos"]       = _as_num(df_items, "taxAmount", idx)
    out["Venta Total Bruta"]     = _as_num(df_items, "totalAmount", idx)
    out["Descuento Neto"]        = _as_num(df_items, "netDiscount", idx)
    out["Descuento Bruto"]       = _as_num(df_items, "totalDiscount", idx)

    # Precio de Lista (si no hay, fallback = bruto unitario)
    pl_raw = _as_series(df_items, "listPrice", idx)  # si no existe, NA
    pl = pd.to_numeric(pl_raw.replace("", pd.NA), errors="coerce")
    out["Precio de Lista"] = pl.fillna(out["Precio Bruto Unitario"])

    # % Descuento robusto
    base = out["Venta Total Bruta"] + out["Descuento Bruto"]
    pct = pd.Series(0.0, index=idx, dtype=float)
    mask = base > 0
    pct.loc[mask] = (out["Descuento Bruto"][mask] / base[mask]) * 100.0
    out["% Descuento"] = pct.round(0).astype("Int64").astype(str) + "%"

    # -------- Costos y margen --------
    out["Costo neto unitario"] = pd.to_numeric(cost_unit_variant, errors="coerce").fillna(0.0)
    out["Costo Total Neto"]    = (out["Costo neto unitario"] * out["Cantidad"]).round(0)

    # Margen = Venta Total Neta - Costo Total Neto (sin IVA)
    out["Margen"] = (out["Venta Total Neta"] - out["Costo Total Neto"]).round(0)

    # % Margen = Margen / Venta Total Neta
    m_pct = pd.Series(0.0, index=idx, dtype=float)
    mask2 = out["Venta Total Neta"] > 0
    m_pct.loc[mask2] = (out["Margen"][mask2] / out["Venta Total Neta"][mask2]) * 100.0
    out["% Margen"] = m_pct.round(0).astype("Int64").astype(str) + "%"

    # Validación simple de montos (opcional)
    calc_bruta = (out["Cantidad"] * out["Precio Bruto Unitario"])
    diff = (out["Venta Total Bruta"] - calc_bruta).abs()
    tol = (calc_bruta.abs() * 0.01) + 1.0  # 1% + 1 CLP
    warn = diff > tol
    if warn.any():
        out["_warn_monto"] = warn.map(lambda x: "REVISA" if x else "")

    # Orden final y export
    cols = [c for c in out.columns if c != "_warn_monto"]
    if "_warn_monto" in out.columns:
        cols = cols + ["_warn_monto"]

    out = out[cols]
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Reporte generado en {out_csv} — {len(out)} filas")
