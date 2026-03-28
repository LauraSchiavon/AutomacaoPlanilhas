# Automation Setup - Daily Data Updates

## Overview

This guide explains how to schedule the data import script to run automatically every day.

## Windows - Task Scheduler

### Step 1: Create a Batch File

Create a file named `run_import.bat`:

```batch
@echo off
cd /d "C:\path\to\your\folder"
python3 facebook_redtrack_importer_v2.py >> import_log.txt 2>&1
```

Replace `C:\path\to\your\folder` with your actual folder path.

### Step 2: Open Task Scheduler

1. Press `Win + R`
2. Type `taskschd.msc` and press Enter

### Step 3: Create New Task

1. Click "Create Basic Task" on the right
2. Name: "Facebook RedTrack Daily Import"
3. Description: "Automatically import data from Facebook Ads and RedTrack"
4. Click Next

### Step 4: Set Trigger

1. Select "Daily"
2. Set time (e.g., 2:00 AM)
3. Click Next

### Step 5: Set Action

1. Select "Start a program"
2. Program: `C:\path\to\run_import.bat`
3. Click Next
4. Click Finish

## macOS/Linux - Cron Job

### Step 1: Create a Shell Script

Create a file named `run_import.sh`:

```bash
#!/bin/bash
cd /path/to/your/folder
python3 facebook_redtrack_importer_v2.py >> import_log.txt 2>&1
```

Make it executable:
```bash
chmod +x run_import.sh
```

### Step 2: Edit Crontab

```bash
crontab -e
```

### Step 3: Add Cron Job

Add this line to run daily at 2:00 AM:

```
0 2 * * * /path/to/your/folder/run_import.sh
```

To run every 6 hours:
```
0 */6 * * * /path/to/your/folder/run_import.sh
```

### Step 4: Verify

```bash
crontab -l
```

## Monitoring

### Check Logs

After running, check the log file:

```bash
tail -f import_log.txt
```

### Email Notifications (Linux/macOS)

Add to crontab to send email on errors:

```
0 2 * * * /path/to/your/folder/run_import.sh || mail -s "Import Failed" your@email.com < import_log.txt
```

## Troubleshooting

### Script doesn't run

1. Verify Python path: `which python3`
2. Verify file permissions: `ls -la run_import.sh`
3. Check logs for errors

### Data not updating

1. Check if Excel file is open (close it)
2. Verify credentials are still valid
3. Check API rate limits
4. Review log file for error messages

## Best Practices

1. **Schedule during off-hours** (e.g., 2-3 AM)
2. **Keep logs** for debugging
3. **Test manually first** before automating
4. **Monitor for 1-2 weeks** to ensure reliability
5. **Set up alerts** for failures
6. **Rotate credentials** regularly

## Advanced: Error Handling

Modify the script to send email on failure:

```python
import smtplib
from email.mime.text import MIMEText

def send_error_email(error_message):
    sender = "your@email.com"
    recipient = "admin@email.com"
    
    msg = MIMEText(f"Import failed:\n\n{error_message}")
    msg['Subject'] = "Facebook RedTrack Import Failed"
    msg['From'] = sender
    msg['To'] = recipient
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender, "your_app_password")
        server.send_message(msg)
```

## Support

For issues with scheduling:
- **Windows:** Search "Task Scheduler" in Help
- **macOS:** `man crontab`
- **Linux:** `man cron`
