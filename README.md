# Bsale API Reporting

Pipeline en Python para extraer documentos de venta desde la API de Bsale, enriquecerlos con catálogos y costos de variantes, y publicar un reporte tabular listo para análisis.

## Requisitos

* Python 3.10+
* Variables de entorno en `.env`:
  * `BSALE_BASE_URL` (por ejemplo `https://api.bsale.io/v1`)
  * `BSALE_TOKEN`
* Dependencias instaladas con `pip install -r requirements.txt`

## Ejecución

En PowerShell (Windows) con un entorno virtual activado (`.\.venv\Scripts\Activate.ps1`):

```powershell
python -m src.main --since 2025-09-23 --until 2025-09-23 --limit 50 --debug --out data\reporte_ventas_test_2025-09-23.csv
```

Parámetros relevantes:

* `--since` / `--until`: fechas delimitan el rango (inclusive) y se transforman a `emissiondaterange=[unixStart,unixEnd]` cubriendo el día completo.
* `--limit`: tamaño de página para la paginación `limit/offset` de Bsale (máximo 50).
* `--out`: ruta opcional del CSV generado. Si se omite se crea `data/reporte_ventas_<since>_<until>.csv`.
* `--debug`: activa trazas (versión, rango, cantidad de documentos, ejemplo de cabecera e inspect_one).

Puedes habilitar trazas adicionales desde la librería de cliente con `setx BSALE_DEBUG 1` antes de ejecutar, lo que imprime cada `GET ... params=...` y el tamaño de cada página.

## Expand y fields utilizados

* `expand=details,client,office,user,coin,priceList,document_type` (se omite `document_type` automáticamente si la cuenta no lo soporta).
* `fields=[id,number,emissionDate,documentTypeId,trackingNumber,token,netAmount,taxAmount,totalAmount,totalDiscount,client,office,user,coin,priceList,details]` para evitar payloads innecesarios y asegurar los datos mínimos del reporte.

## Costos de variantes

Se consulta `/variants` y se detecta dinámicamente la primera columna disponible entre:
`cost`, `costPrice`, `netCost`, `lastPurchasePrice`, `averageCost`, `prices.cost`, `unitCost`, `purchasePrice`.
Si ninguna columna trae valores, el costo unitario neto se fija en `0.0`.

## Limitaciones conocidas

* Es necesario contar con token válido y permisos de lectura para `/documents`, `/document_types`, `/users`, `/price_lists`, `/offices` y (opcionalmente) `/variants`.
* Si `/variants` no está habilitado en la cuenta, el reporte sigue funcionando con costos `0.0`.
* Los catálogos se cachean en `data/cache/*.json`; eliminar los archivos fuerza una recarga completa en la siguiente ejecución.
* El script no ejecuta pruebas automáticas porque depende de la API real de Bsale.

## Archivos generados de ejemplo

* `data/reporte_ventas_2024-01-01_2024-01-01.csv`: muestra la estructura y columnas esperadas del reporte.
* `data/cache/`: contiene ejemplos de catálogos serializados tras una ejecución.

## Depuración rápida

Ejemplo de invocación con depuración (salida resumida):

```
▶ Ejecutando main v5
GET https://api.bsale.io/v1/documents.json params={'limit': '50', 'offset': 0, ...}
→ página offset=0 len(items)=N
ℹ️ documents: N
   keys cabecera: [...]
   relaciones disponibles: [...]
   primer ítem: {...}
✅ Reporte generado: data\reporte_ventas_test_2025-09-23.csv
```

Sustituye `N` por el número de documentos recibidos y completa con tus datos reales.
