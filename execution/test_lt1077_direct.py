#!/usr/bin/env python3
"""
Test script to find the specific ad LT1077 in Diana Vargas CA06 account.
Queries /campaigns directly instead of /insights, to find campaigns even with 0 spend.
"""

import sys, os, requests
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from export_creative_tests import extract_ad_name_from_campaign, build_col_a_label

TOKEN = "EAAWDHozjODgBQ4gAIJDmZCVTNc6ngXmc8zBH78CqowDy8f9dEYYbeqNeXTwqUYlMsuDRzhc1GxnGZAjPspiWfo8am5D8p7MipDbNW1ZBUxaZCPnN00azppueER018h3kpQsCdM3RyevhDIqX1NcekjH11HzeRjSrXgI7eDpOibMWS0G9ZAqVouLTiEWEhC9NAAOZCRwFzTBvVZAmh7wdzRn08taFtob6ZAUnErQ67b9oZChQXW9aLdzF5nAvAniP2QyBR17QhJL9NDQCUigpGq6V6JBcoLvzi2ECP9WAoEQZDZD"
ACCOUNT_ID = "act_542987171356461"  # Diana Vargas - CA06
target_ad = "lt1077"

print(f"Buscando todas as campanhas em {ACCOUNT_ID}...")

# Fetch raw campaigns to ensure we get them even if spend = 0
url = f"https://graph.facebook.com/v19.0/{ACCOUNT_ID}/campaigns"
params = {
    "access_token": TOKEN,
    "fields": "name,effective_status",
    "limit": 500,
}

r = requests.get(url, params=params)
if r.status_code != 200:
    print(f"Erro na API: {r.text}")
    sys.exit(1)

data = r.json().get("data", [])
print(f"Total de campanhas brutas na conta: {len(data)}\n")

found_match = False

for camp in data:
    campaign_name = camp.get("name", "")
    ad_extracted = extract_ad_name_from_campaign(campaign_name)
    
    if ad_extracted and target_ad in ad_extracted.lower():
        found_match = True
        col_a_value = build_col_a_label(campaign_name)
        
        print("✅ MATCH ENCONTRADO!")
        print(f"Nome da Campanha: '{campaign_name}'")
        print(f"Status da Campanha: {camp.get('effective_status')}")
        print(f"Anúncio Extraído: '{ad_extracted}'")
        print(f"Valor que seria escrito na Coluna A: '{col_a_value}'")
        print("-" * 50)

if not found_match:
    print(f"❌ O anúncio '{target_ad}' não existe no final do nome de NENHUMA campanha atual desta conta.")

    # Just print a sample of 20 active campaigns to see the naming structure
    print("\nExemplos de campanhas ativas na conta atualmente:")
    active = [c for c in data if c.get('effective_status') == 'ACTIVE']
    for c in active[:20]:
        print(c.get("name"))
