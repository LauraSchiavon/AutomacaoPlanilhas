import requests
import json
import sys

# User's provided list of Ads
ads = ["LT1017.7", "LT1010.2", "LT899.22", "LT1033.2", "LT1011.4", "LT899.32", "LT899.33", "LT1034.4", "LT1069", "LT1070"]
ads_str = ",".join(ads)

api_key = "wB7qY69R0KVU9tl4TBaQ"
url = "https://api.redtrack.io/report"

params = {
    'api_key': api_key,
    'date_from': '2024-01-01',  # Pull from a wide range to ensure we capture all data
    'date_to': '2026-03-05',
    'group': 'rt_ad',
    'rt_ad': ads_str,
    'limit': 5000
}

try:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    # Check if empty
    if not data:
        print("Nenhum dado retornado para esses IDs no período avaliado.")
        sys.exit(0)
        
    print(f"Encontrados dados para {len(data)} anúncios:")
    print("-" * 50)
    for row in data:
        ad_id = row.get("rt_ad", "Desconhecido")
        purchases = row.get("convtype2", 0)  # The confirmed 'Vendas' mapping
        revenue = row.get("total_revenue", 0.0)
        spend = row.get("cost", 0.0)
        
        print(f"Anúncio: {ad_id:<15} | Vendas (convtype2): {purchases:<5} | Receita: R$ {revenue:<10.2f} | Custo RedTrack: R$ {spend:.2f}")

except Exception as e:
    print(f"Erro na requisição: {e}")
