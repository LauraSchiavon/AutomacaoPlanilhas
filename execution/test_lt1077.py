#!/usr/bin/env python3
"""
Test script to find the specific ad LT1077 in Diana Vargas CA06 account.
Prints what would be written to Column A.
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from facebook_redtrack_importer_v2 import FacebookAdsAPI
from export_creative_tests import extract_ad_name_from_campaign, build_col_a_label

TOKEN = "EAAWDHozjODgBQ4gAIJDmZCVTNc6ngXmc8zBH78CqowDy8f9dEYYbeqNeXTwqUYlMsuDRzhc1GxnGZAjPspiWfo8am5D8p7MipDbNW1ZBUxaZCPnN00azppueER018h3kpQsCdM3RyevhDIqX1NcekjH11HzeRjSrXgI7eDpOibMWS0G9ZAqVouLTiEWEhC9NAAOZCRwFzTBvVZAmh7wdzRn08taFtob6ZAUnErQ67b9oZChQXW9aLdzF5nAvAniP2QyBR17QhJL9NDQCUigpGq6V6JBcoLvzi2ECP9WAoEQZDZD"
ACCOUNT_ID = "act_542987171356461"  # Diana Vargas - CA06

target_ad = "lt1077"

fb_api = FacebookAdsAPI(TOKEN)

# Fetch all campaigns from a wide date range to guarantee finding it
print(f"Buscando campanhas em {ACCOUNT_ID}...")
raw_data = fb_api.get_ad_insights(
    ACCOUNT_ID,
    "2025-01-01",
    "2026-03-03",
    level='campaign'
)

print(f"Total de campanhas recuperadas: {len(raw_data)}\n")

found_match = False

for record in raw_data:
    campaign_name = record.get("campaign_name", "")
    ad_extracted = extract_ad_name_from_campaign(campaign_name)
    
    # Check if lt1077 is in the extracted ad name (or if it matches exactly)
    if ad_extracted and target_ad in ad_extracted.lower():
        found_match = True
        col_a_value = build_col_a_label(campaign_name)
        
        print("MATCH ENCONTRADO!")
        print(f"Nome da Campanha na API: '{campaign_name}'")
        print(f"Anúncio Extraído: '{ad_extracted}'")
        print(f"Valor que seria escrito na Coluna A: '{col_a_value}'")
        print("-" * 50)

if not found_match:
    print(f"❌ O anúncio base '{target_ad}' não foi encontrado em nenhuma campanha ATIVA neste período.")
