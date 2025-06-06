import os
import re
import json
from datetime import datetime, timedelta, timezone
import functions_framework
from google.cloud import storage, bigquery

# Environment
PROCESSED_BUCKET = os.environ['PROCESSED_BUCKET']  # e.g. cityprogressmobilityl2c-incidents
PREFIX           = os.environ.get('FOLDER_PREFIX', 'reports')
BQ_TABLE         = os.environ['BQ_TABLE']           # e.g. cityprogressmobilityl2c.real_time.incidents

# Clients
storage_client = storage.Client()
bq_client      = bigquery.Client()

# Severity mapping for news (“1” to “5”), tweets get 0
def map_severity(val: str) -> int:
    try:
        return int(val)
    except:
        return 0

def parse_txt_blob(blob) -> list[dict]:
    """
    Download and parse one report_XXXX.txt blob into one or more rows.
    For news: entire blob is one news record.
    For tweet: blob has 2 lines: tweet: ..., time: ...
    """
    text = blob.download_as_text().strip()
    lines = text.splitlines()
    rows = []
    # Determine type by first line
    if lines[0].lower().startswith("title"):
        # NEWS: parse Title, Incident, Severity, Time
        data = {}
        for line in lines:
            if ':' not in line:
                continue
            k, v = line.split(':',1)
            key = k.strip().lower()
            val = v.strip()
            if key == 'title':
                data['potential_address'] = val.split(' at ')[-1]
            elif key == 'incident':
                data['description'] = val
            elif key == 'severity':
                data['severity'] = map_severity(val)
            elif key == 'time':
                data['event_time'] = val
        rows.append(data)
    else:
       # TWEET: expect lines “tweet: …” and “time: …”
        tweet_line = next((l for l in lines if l.lower().startswith('tweet:')), None)
        time_line  = next((l for l in lines if l.lower().startswith('time:')), None)
        if tweet_line and time_line:
            # Extract description (everything after “tweet: ”)
            desc = tweet_line.split(':', 1)[1].strip()
            # Look for “ at <stop name>” to pull potential_address
            address = None
            m = re.search(r' at (.+)$', desc, flags=re.IGNORECASE)
            if m:
                address = m.group(1).strip()
            # Extract timestamp
            evt = time_line.split(':', 1)[1].strip()
            rows.append({
                'potential_address': address,
                'description':        desc,
                'severity':           1,
                'event_time':         evt
            })
    # Attach source for traceability
    for r in rows:
        r['source_blob'] = blob.name
    return rows

@functions_framework.http
def handler(request):
    now   = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=1, minutes=9)
    date_str = now.strftime("%Y%m%d")
    folder   = f"{PREFIX}_{date_str}/"
    bucket   = storage_client.bucket(PROCESSED_BUCKET)

    # Collect rows
    all_rows = []
    for blob in bucket.list_blobs(prefix=folder):
        if not blob.name.endswith('.txt'):
            continue
        # Use blob.create_time to filter by last 69 minutes
        if blob.time_created < window_start:
            continue
        all_rows.extend(parse_txt_blob(blob))

    # Insert into BigQuery
    if all_rows:
        errors = bq_client.insert_rows_json(BQ_TABLE, all_rows)
        if errors:
            return (f"BQ insert errors: {errors}", 500)
    return (f"Processed {len(all_rows)} incidents", 200)
