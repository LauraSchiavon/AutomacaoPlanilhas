"""Quick verify: time_range JSON returns the correct $166.53 for LT1010.2"""
import requests

TOKEN = "EAAWDHozjODgBQ6y16hDYtG5psMsSYZBv3S6O8w3lxeihOtCryNgLFwQXGVxwbJuSks9BJQZBsWY2Iw5ZAuV9sCuTmBusecWWrkxb5glHaKLE9DSvhe6edmfT0b96sVWGZAsBVwZCMAF9cKsPJQ4qZBEGJWFoOZA9iDAPZBFT5GNUMRIIJEjj0hik5d1nTZAvWcjNMwMurcmcWgOfXOWtaZBaM3nYZB2EbdqllBH3mAjCxSGkAgocfYvJZBZCkZCdfmqUcY5LekdzG3w6doGIB0IytWhbY9JZBTEepCY5DgS3ZB8OXQZDZD"
ACC = "1277255524217242"

# Method 1: time_range JSON (CORRECT - what we just fixed to)
r1 = requests.get(f"https://graph.facebook.com/v19.0/act_{ACC}/insights", params={
    "access_token": TOKEN,
    "fields": "campaign_name,spend",
    "level": "campaign",
    "time_range": '{"since":"2026-02-01","until":"2026-03-06"}',
    "limit": 500
}, timeout=30)
for c in r1.json().get('data', []):
    if 'lt1010.2' in c['campaign_name'].lower():
        print(f"[time_range JSON] LT1010.2 spend: ${float(c['spend']):.2f}")

# Method 2: date_start/date_end params (OLD - what was broken)
r2 = requests.get(f"https://graph.facebook.com/v19.0/act_{ACC}/insights", params={
    "access_token": TOKEN,
    "fields": "campaign_name,spend",
    "level": "campaign",
    "date_start": "2026-02-01",
    "date_end": "2026-03-06",
    "limit": 500
}, timeout=30)
for c in r2.json().get('data', []):
    if 'lt1010.2' in c['campaign_name'].lower():
        print(f"[date_start/end ] LT1010.2 spend: ${float(c['spend']):.2f}")
