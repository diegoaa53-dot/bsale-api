import requests
import pandas as pd

# ⚡ Pega aquí tu token de Bsale
TOKEN = "TU_TOKEN_DE_BSALE"
BASE_URL = "https://api.bsale.cl/v1"

headers = {
    "access_token": TOKEN
}

# Ejemplo: obtener productos
url = f"{BASE_URL}/products.json"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data['items'])
    df.to_csv("productos.csv", index=False, encoding="utf-8-sig")
    print("✅ Productos exportados a productos.csv")
else:
    print("❌ Error:", response.status_code, response.text)
