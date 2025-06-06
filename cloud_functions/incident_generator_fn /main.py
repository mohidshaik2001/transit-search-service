# cloud_functions/incident_generator/main.py

import functions_framework
import os, io, random
from datetime import datetime, timedelta, timezone
import pandas as pd
from google.cloud import storage

# Config via env
GTFS_BUCKET        = os.environ['GTFS_BUCKET']            # e.g. cityprogressmobilityl2c-gtfs
RAW_BUCKET         = os.environ['RAW_BUCKET']             # e.g. cityprogressmobilityl2c-incidents-raw
STOPS_PATH         = os.environ.get('STOPS_PATH', 'stops.txt')
MAX_SEVERITY       = int(os.environ.get('MAX_SEVERITY', 5))
MIN_TWEETS, MAX_TWEETS = 5, 10

storage_client = storage.Client()
gtfs_bucket = storage_client.bucket(GTFS_BUCKET)
raw_bucket  = storage_client.bucket(RAW_BUCKET)

def load_stops() -> list[str]:
    blob = gtfs_bucket.blob(STOPS_PATH)
    data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(data), usecols=['stop_name'])
    return df['stop_name'].dropna().unique().tolist()

def jittered_time() -> str:
    now = datetime.now(timezone.utc)
    delta = timedelta(seconds=random.randint(0, 14*60))
    return (now - delta).isoformat()

def generate_news(stops: list[str]) -> str:
    stop = random.choice(stops)
    severity = random.randint(1, MAX_SEVERITY)
    ts = datetime.now(timezone.utc).isoformat()
    return (
        f"Title: Incident at {stop}\n"
        f"Incident: A random event occurred near {stop}.\n"
        f"Severity: {severity}\n"
        f"Time: {ts}\n"
    )

def generate_tweets(stops: list[str]) -> str:
    tweets = []
    for _ in range(random.randint(MIN_TWEETS, MAX_TWEETS)):
        stop = random.choice(stops)
        msg = f"tweet: Traffic buildup at {stop}"
        ts  = f"time: {jittered_time()}"
        tweets.append(f"{msg}\n{ts}")
    return "\n\n".join(tweets) + "\n"

def write_blob(name: str, content: str):
    b = raw_bucket.blob(name)
    b.upload_from_string(content, content_type="text/plain")

def generate():
    stops = load_stops()
    # Overwrite the two files:
    write_blob("news.txt",   generate_news(stops))
    write_blob("tweets.txt", generate_tweets(stops))



@functions_framework.http
def handler(request):
    try:
        generate()
        return ("Incidents generated", 200)
    except Exception as e:
        return (f"Error: {e}", 500)
