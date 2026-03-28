#!/usr/bin/env python3
"""
debug_fill.py - Tests the ad-level API call directly and prints raw results.
Run: python execution/debug_fill.py
"""
import sys, os, requests
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from facebook_redtrack_importer_v2 import FacebookAdsAPI

TOKEN = "EAAWDHozjODgBQZCaUO1BmIRhlor7pT3l5NnLt9OE3sD7DlY0XCyELNOLw5Fm73EYJplWcFGwN52V40RPyK1RhptKtkoZCD7QJ7m982DxGR1GxTCAcXTb0KzXsy73eQJ7JApVjmkyyf2S9d6hgZB10dX8wyUV50arh1WuM0h1NMzlZCTrQCRKlyMZAGlzMpBt0cpzyHZBpcslNUozDmUZAuEbBYtldb5eQQe5lGVvYD8Dlqw0FuSQRhKjZB3sR0skPHklUg4Rnf5VSAgJkhriQ8xdjh6YkmKp8lzkCK28mAZDZD"
BASE_URL = "https://graph.facebook.com/v19.0"

# ── 1. Get BMs
print("Step 1: Fetching Business Managers...")
r = requests.get(f"{BASE_URL}/me/businesses", params={"access_token": TOKEN, "fields": "id,name"})
bms = r.json().get("data", [])
for i, bm in enumerate(bms):
    print(f"  [{i}] {bm['name']} - {bm['id']}")

if not bms:
    print("No BMs found. Token may be invalid.")
    sys.exit(1)

bm = bms[0]
print(f"\nUsing BM: {bm['name']}")

# ── 2. Get Ad Accounts
print("\nStep 2: Fetching Ad Accounts...")
r = requests.get(f"{BASE_URL}/{bm['id']}/owned_ad_accounts",
    params={"access_token": TOKEN, "fields": "id,name", "limit": 10})
accounts = r.json().get("data", [])
for i, acc in enumerate(accounts):
    print(f"  [{i}] {acc['name']} - {acc['id']}")

if not accounts:
    print("No accounts found.")
    sys.exit(1)

acc = accounts[0]
print(f"\nUsing account: {acc['name']} ({acc['id']})")

# ── 3. Test insights at level=ad
print("\nStep 3: Testing ad-level insights for last 30 days...")
account_raw = acc["id"].replace("act_", "")

r = requests.get(
    f"{BASE_URL}/act_{account_raw}/insights",
    params={
        "access_token": TOKEN,
        "fields": "campaign_name,ad_name,impressions,spend",
        "level": "ad",
        "date_start": "2026-02-01",
        "date_end": "2026-03-03",
        "limit": 5,
    },
    timeout=30
)
data = r.json()
print("\nRaw API response:")
import json
print(json.dumps(data, indent=2)[:2000])  # First 2000 chars
