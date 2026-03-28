# Facebook Ads & RedTrack Data Importer - Setup Guide

## Overview

This system automates the import of advertising data from Facebook Ads Manager and conversion data from RedTrack into your Excel spreadsheet. The data is organized in a "Dados Brutos" (Raw Data) sheet, which can then be referenced in your main analysis sheet.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Your Excel File                       │
├─────────────────────────────────────────────────────────┤
│  Sheet 1: 032026 (Main Analysis)                       │
│  ├─ References data from "Dados Brutos"               │
│  └─ Uses formulas to pull specific metrics            │
├─────────────────────────────────────────────────────────┤
│  Sheet 2: Dados Brutos (Raw Data)                      │
│  ├─ Facebook Ads data (columns A-J)                   │
│  │  └─ Hook View 3", Body View 75%, CPM, CTR, CPC, GASTO
│  └─ RedTrack data (columns K+)                        │
│     └─ Vendas, CPA                                     │
└─────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **Python 3.7+** installed
2. **Required Python packages:**
   ```bash
   pip install requests pandas openpyxl
   ```

3. **Credentials:**
   - Facebook Access Token (already provided)
   - RedTrack API Key (already provided)

## Files Included

| File | Purpose |
|------|---------|
| `facebook_redtrack_importer_v2.py` | Main import script with interactive account selection |
| `SETUP_GUIDE.md` | This documentation |
| `AUTOMATION_SETUP.md` | Instructions for daily automated updates |

## How to Use

### Method 1: Interactive Mode (Recommended)

Run the script and select your account and Business Manager interactively:

```bash
python3 facebook_redtrack_importer_v2.py
```

The script will:
1. List all your Business Managers
2. Ask you to select one
3. List all Ad Accounts in that BM
4. Ask you to select an account
5. Fetch data from the last 30 days
6. Create/update the "Dados Brutos" sheet
7. Save the Excel file

### Method 2: Automated (Non-Interactive)

For scheduling daily updates, modify the script to hardcode the account ID:

```python
# In facebook_redtrack_importer_v2.py, replace the interactive selection with:
account_id = "act_YOUR_ACCOUNT_ID"  # e.g., "act_123456789"
```

## Data Mapping

### Facebook Ads Data (Columns A-J in "Dados Brutos")

| Column | Field | Description |
|--------|-------|-------------|
| A | campaign_id | Campaign ID |
| B | campaign_name | Campaign Name |
| C | adset_id | Ad Set ID |
| D | adset_name | Ad Set Name |
| E | ad_id | Ad ID |
| F | ad_name | Ad Name |
| G | impressions | Total Impressions |
| H | clicks | Total Clicks |
| I | spend | Total Spend (GASTO) |
| J | cpm | Cost Per Mille (CPM) |
| K | ctr | Click-Through Rate (CTR) |
| L | cpc | Cost Per Click (CPC) |

### RedTrack Data (Columns M+ in "Dados Brutos")

| Column | Field | Description |
|--------|-------|-------------|
| M | conversion_id | Conversion ID |
| N | campaign_id | Campaign ID |
| O | revenue | Total Revenue (Vendas) |
| P | cpa | Cost Per Acquisition (CPA) |

## Linking Data to Main Sheet (032026)

In your main analysis sheet, use VLOOKUP or INDEX/MATCH formulas to pull data from "Dados Brutos":

### Example 1: Get CPM for a campaign

```excel
=VLOOKUP(B3, 'Dados Brutos'!A:L, 10, FALSE)
```

Where:
- `B3` is your campaign name
- `'Dados Brutos'!A:L` is the data range
- `10` is the column number (CPM)
- `FALSE` means exact match

### Example 2: Get Sales (Vendas) from RedTrack

```excel
=SUMIF('Dados Brutos'!N:N, B3, 'Dados Brutos'!O:O)
```

This sums all revenues where the campaign ID matches.

### Example 3: Calculate CPA

```excel
=IFERROR(VLOOKUP(B3, 'Dados Brutos'!M:P, 4, FALSE), 0)
```

## Troubleshooting

### Issue: "No business managers found"
**Solution:** Verify your Facebook Access Token is valid and has the necessary permissions.

### Issue: "No ad accounts found"
**Solution:** Ensure the selected Business Manager actually owns ad accounts.

### Issue: "Error fetching conversions"
**Solution:** Check your RedTrack API Key is correct and has access to view conversions.

### Issue: Excel file is locked
**Solution:** Close the Excel file before running the import script.

## Daily Automation Setup

See `AUTOMATION_SETUP.md` for instructions on scheduling daily updates using:
- **Windows:** Task Scheduler
- **macOS/Linux:** Cron jobs

## API Limits

- **Facebook Ads API:** 200 requests per hour (per token)
- **RedTrack API:** Check your plan limits

The script respects these limits and will handle pagination automatically.

## Security Notes

⚠️ **Important:** 
- Store credentials in environment variables, not in code
- Never commit credentials to version control
- Rotate tokens regularly
- Use a dedicated service account if possible

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review API documentation:
   - Facebook: https://developers.facebook.com/docs/marketing-api
   - RedTrack: https://api.redtrack.io/docs/index.html
3. Verify your credentials are correct

## Next Steps

1. Run the script for the first time
2. Verify data appears in "Dados Brutos" sheet
3. Update your main sheet formulas to reference the new data
4. Set up daily automation (see AUTOMATION_SETUP.md)
