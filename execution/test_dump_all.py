#!/usr/bin/env python3
import requests
import json

TOKEN = "EAAWDHozjODgBQZBEiBDPWYCCc5AmZA9FTkAh6ICQ3tMju4hCZAWCphJZCY9xsKZBIFq8PCefiiJnZA1MWVitYDNlIU86LOZAYkH91qPVx5vKuYNGb9xZCWGxcyAgp8qetdQgZB9ly3QyMz6wSyzck65iZCkWc8XAAlsAmG1nZB6z5quMXeDNfb5WMgE55N3yyGsUpyVsMEekMIYIl5nW0xH7jg2WUzT3ysBGEnZCrXVdQCTVc5iD6kqfuC5fATZAHqHXyfNiRENJRDHjulEgLszhvKegV7uKYfEBWBlrJeXZBtnGgZD"
ACCOUNT_ID = "act_542987171356461"

url = f"https://graph.facebook.com/v19.0/{ACCOUNT_ID}/insights"
params = {
    'access_token': TOKEN,
    'fields': 'campaign_name,impressions,video_play_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p100_watched_actions,video_15_sec_watched_actions,video_30_sec_watched_actions,video_avg_time_watched_actions,actions',
    'level': 'campaign',
    'filtering': '[{"field":"campaign.name","operator":"CONTAIN","value":"LT581.3 - P21"}]',
    'date_preset': 'maximum'
}

r = requests.get(url, params=params)
print(json.dumps(r.json(), indent=2))
