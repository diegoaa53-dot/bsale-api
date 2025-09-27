from __future__ import annotations

import os
from typing import Iterable, List

import pandas as pd
from pandas import json_normalize

from .catalogs import get_all_maps


EXPECTED_COLUMNS: List[str] = [
    "Tipo de Documento",
    "Numero Documento",
    "Fecha de Emisión",
    "Tracking number",
    "Sucursal",
    "Vendedor",
    "Nombre Cliente",
    "Cliente RUT",
    "Lista de Precio",
    "Moneda",
    "SKU",
    "Producto / Servicio",
    "Precio de Lista",
    "Precio Neto Unitario",
    "Precio Bruto Unitario",
    "Cantidad",
    "Venta Total Neta",
    "Total Impuestos",
    "Venta Total Bruta",
    "Descuento Bruto",
    "% Descuento",
    "Costo neto unitario",
    "Costo Total Neto",
    "Margen",
    "% Margen",
]


def ensure_data_dir(path: str = "data") -> None:
    os.makedirs(path, exist_ok=True)


def _as_series(df: pd.DataFrame, columns: Iterable[str], index) -> pd.Series:
    for col in columns:
        if col in df.columns:
            return df[col].reindex(index)
    return pd.Series([pd.NA] * len(index), index=index)


def _as_num(df: pd.DataFrame, columns: Iterable[str], index) -> pd.Series:
    series = _as_series(df, columns, index)
    return pd.to_numeric(series, errors="coerce").astype(float).fillna(0.0)


def _fmt_date(unix_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(unix_series, unit="s", errors="coerce")
    return dt.dt.strftime("%d/%m/%Y")


def _fmt_datetime(unix_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(unix_series, unit="s", errors="coerce")
    return dt.dt.strftime("%d/%m/%Y %H:%M:%S")


def _first_non_empty(*series: pd.Series) -> pd.Series:
    if not series:
        raise ValueError("Se requiere al menos una serie para _first_non_empty")
    result = series[0].copy()
    for extra in series[1:]:
        mask = result.isna() | (result.astype(str).str.strip() == "")
        result = result.where(~mask, extra)
    return result


def _as_clean_str(series: pd.Series) -> pd.Series:
    if series.dtype == "O" or isinstance(series.dtype, pd.StringDtype):
        result = series.astype("string")
    else:
        result = series.astype("string")
    return result.fillna("")


def _warn_monto(out: pd.DataFrame) -> None:
    calc_bruta = out["Cantidad"] * out["Precio Bruto Unitario"]
    diff = (out["Venta Total Bruta"] - calc_bruta).abs()
    tolerance = calc_bruta.abs() * 0.01 + 1.0
    warn_mask = diff > tolerance
    if warn_mask.any():
        out.loc[warn_mask, "_warn_monto"] = "REVISA"


def build_reporte_ventas(documents: List[dict], out_csv: str) -> None:
    ensure_data_dir(os.path.dirname(out_csv) or "data")

    if not documents:
        pd.DataFrame(columns=EXPECTED_COLUMNS).to_csv(out_csv, index=False, encoding="utf-8-sig")
        print("⚠️ No llegaron documentos.")
        return

    df_items = json_normalize(
        documents,
        record_path=["details", "items"],
        meta=[
            "id",
            "number",
            "emissionDate",
            "documentTypeId",
            "trackingNumber",
            "token",
            ["document_type", "name"],
            ["documentType", "name"],
            ["office", "id"],
            ["office", "name"],
            ["user", "id"],
            ["user", "name"],
            ["client", "firstName"],
            ["client", "lastName"],
            ["client", "company"],
            ["client", "code"],
            ["priceList", "id"],
            ["priceList", "name"],
            ["coin", "code"],
            ["variant", "id"],
            ["variant", "code"],
            ["variant", "description"],
        ],
        sep=".",
        errors="ignore",
        meta_prefix="doc.",
    )

    (
        doc_type_map,
        user_map,
        price_list_map,
        office_map,
        dim_variant,
    ) = get_all_maps(refresh=False)

    index = df_items.index

    doc_type_from_expand = _first_non_empty(
        _as_series(df_items, ["doc.document_type.name"], index),
        _as_series(df_items, ["doc.documentType.name"], index),
    )
    doc_type_id_raw = _as_series(df_items, ["doc.documentTypeId"], index)
    doc_type_from_map = doc_type_id_raw.map(
        lambda value: doc_type_map.get(int(value), "") if pd.notna(value) else ""
    )
    doc_type_id_str = doc_type_id_raw.map(lambda value: str(int(value)) if pd.notna(value) else "")
    tipo_documento = _first_non_empty(doc_type_from_expand, doc_type_from_map, doc_type_id_str)

    vendedor_expand = _as_series(df_items, ["doc.user.name"], index)
    vendedor_map = _as_series(df_items, ["doc.user.id"], index).map(
        lambda value: user_map.get(int(value), "") if pd.notna(value) else ""
    )
    vendedor = _first_non_empty(vendedor_expand, vendedor_map)

    oficina_expand = _as_series(df_items, ["doc.office.name"], index)
    oficina_map = _as_series(df_items, ["doc.office.id"], index).map(
        lambda value: office_map.get(int(value), "") if pd.notna(value) else ""
    )
    sucursal = _first_non_empty(oficina_expand, oficina_map)

    cliente_nombre = _first_non_empty(
        _as_series(df_items, ["doc.client.company"], index),
        (
            _as_series(df_items, ["doc.client.firstName"], index).fillna("")
            + " "
            + _as_series(df_items, ["doc.client.lastName"], index).fillna("")
        ).str.strip(),
    )

    lista_expand = _as_series(df_items, ["doc.priceList.name"], index)
    lista_map = _as_series(df_items, ["doc.priceList.id"], index).map(
        lambda value: price_list_map.get(int(value), "") if pd.notna(value) else ""
    )
    lista_precio = _first_non_empty(lista_expand, lista_map)

    moneda = _first_non_empty(
        _as_series(df_items, ["doc.coin.code"], index),
        pd.Series(["CLP"] * len(index), index=index),
    )

    variant_id = pd.to_numeric(_as_series(df_items, ["doc.variant.id"], index), errors="coerce")
    joined = pd.DataFrame({"__row__": index, "variant_id": variant_id})
    joined = joined.merge(dim_variant, how="left", on="variant_id")
    missing_cost = joined["cost_net_unit"].isna() | (joined["cost_net_unit"] == 0)
    if missing_cost.any():
        sku_series = _as_series(df_items, ["doc.variant.code"], index).astype(str)
        dim_by_sku = dim_variant[["sku", "cost_net_unit"]].dropna()
        joined.loc[missing_cost, "sku"] = sku_series[missing_cost]
        joined = joined.merge(
            dim_by_sku.rename(columns={"cost_net_unit": "cost_from_sku"}),
            how="left",
            on="sku",
        )
        joined["cost_net_unit"] = joined["cost_net_unit"].fillna(0.0)
        joined.loc[missing_cost, "cost_net_unit"] = (
            joined.loc[missing_cost, "cost_net_unit"].where(
                joined.loc[missing_cost, "cost_net_unit"] > 0, joined.loc[missing_cost, "cost_from_sku"].fillna(0.0)
            )
        )

    cost_unit = joined.set_index("__row__")["cost_net_unit"].reindex(index).fillna(0.0)

    precio_lista_raw = _as_series(df_items, ["listPrice"], index)
    precio_lista = pd.to_numeric(precio_lista_raw, errors="coerce")

    precio_bruto_unitario = _as_num(df_items, ["totalUnitValue"], index)
    precio_neto_unitario = _as_num(df_items, ["netUnitValue"], index)
    cantidad = _as_num(df_items, ["quantity"], index)
    venta_total_neta = _as_num(df_items, ["netAmount"], index)
    venta_total_bruta = _as_num(df_items, ["totalAmount"], index)
    total_impuestos = _as_num(df_items, ["taxAmount"], index)
    descuento_bruto = _as_num(df_items, ["totalDiscount"], index)

    base_descuento = venta_total_bruta + descuento_bruto
    porcentaje_descuento = pd.Series(0.0, index=index)
    mask_descuento = base_descuento > 0
    porcentaje_descuento.loc[mask_descuento] = descuento_bruto[mask_descuento] / base_descuento[mask_descuento]

    costo_total_neto = cost_unit * cantidad
    margen = venta_total_neta - costo_total_neto
    porcentaje_margen = pd.Series(0.0, index=index)
    mask_margen = venta_total_neta > 0
    porcentaje_margen.loc[mask_margen] = margen[mask_margen] / venta_total_neta[mask_margen]

    out = pd.DataFrame(index=index)
    out["Tipo de Documento"] = _as_clean_str(tipo_documento)
    out["Numero Documento"] = _as_clean_str(_as_series(df_items, ["doc.number"], index))
    out["Fecha de Emisión"] = _fmt_date(_as_series(df_items, ["doc.emissionDate"], index))
    out["Tracking number"] = _first_non_empty(
        _as_clean_str(_as_series(df_items, ["doc.trackingNumber"], index)),
        _as_clean_str(_as_series(df_items, ["doc.token"], index)),
        _as_clean_str(_as_series(df_items, ["doc.id"], index)),
    ).fillna("")
    out["Sucursal"] = _as_clean_str(sucursal)
    out["Vendedor"] = _as_clean_str(vendedor)
    out["Nombre Cliente"] = _as_clean_str(cliente_nombre)
    out["Cliente RUT"] = _as_clean_str(_as_series(df_items, ["doc.client.code"], index))
    out["Lista de Precio"] = _as_clean_str(lista_precio)
    out["Moneda"] = _as_clean_str(moneda)
    out["SKU"] = _as_clean_str(_as_series(df_items, ["doc.variant.code"], index))
    out["Producto / Servicio"] = _as_clean_str(_as_series(df_items, ["doc.variant.description"], index))
    out["Precio Neto Unitario"] = precio_neto_unitario
    out["Precio Bruto Unitario"] = precio_bruto_unitario
    out["Cantidad"] = cantidad
    out["Venta Total Neta"] = venta_total_neta
    out["Total Impuestos"] = total_impuestos
    out["Venta Total Bruta"] = venta_total_bruta
    out["Descuento Bruto"] = descuento_bruto
    out["% Descuento"] = porcentaje_descuento.fillna(0.0)
    out["Costo neto unitario"] = cost_unit
    out["Costo Total Neto"] = costo_total_neto
    out["Margen"] = margen
    out["% Margen"] = porcentaje_margen.fillna(0.0)

    fallback_precio_lista = precio_lista.fillna(precio_bruto_unitario)
    out.insert(
        out.columns.get_loc("Precio Neto Unitario"),
        "Precio de Lista",
        fallback_precio_lista,
    )

    _warn_monto(out)

    out = out[EXPECTED_COLUMNS + (["_warn_monto"] if "_warn_monto" in out.columns else [])]
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Reporte generado en {out_csv} — {len(out)} filas")
