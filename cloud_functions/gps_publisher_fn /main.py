import os
import functions_framework
import io
import json, time
import random
from datetime import datetime, timezone
import pandas as pd
from google.cloud import pubsub_v1, storage


# Environment variables
PROJECT_ID = os.environ['L2C_PROJECT_ID']
GTFS_BUCKET = os.environ['L2C_GTFS_BUCKET']  # e.g., "cityprogressmobilityl2c-gtfs"
CONFIG_PATH = os.environ.get('ROUTE_BOUNDS_PATH', 'Processed/route_bounds.csv')
# How long to loop internally (5 minutes)
MAX_RUNTIME = int(os.environ.get("MAX_RUNTIME_SEC", 5*60))
# How long between batches
INTERVAL = int(os.environ.get("PUBLISH_INTERVAL_SEC", 30))
# Pub/Sub setup
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, 'vehicle-locations')

# Storage client for loading config from GCS
storage_client = storage.Client()


def load_route_bounds(bucket_name: str, blob_path: str) -> pd.DataFrame:
    """
    Load route bounds CSV from GCS into a pandas DataFrame.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_text()
    return pd.read_csv(io.StringIO(data))


# Load ROUTES at import time
routes_df = load_route_bounds(GTFS_BUCKET, CONFIG_PATH)
ROUTES = {
    row['route_id']: {
        'lat_min': row['lat_min'], 'lat_max': row['lat_max'],
        'lon_min': row['lon_min'], 'lon_max': row['lon_max']
    }
    for _, row in routes_df.iterrows()
}


def run_publisher() -> int:
    """
    Publish one batch of pings (one message per route).
    Returns the last message_id published.
    """
    message_id = None
    for route, bounds in ROUTES.items():
        lat = random.uniform(bounds['lat_min'], bounds['lat_max'])
        lon = random.uniform(bounds['lon_min'], bounds['lon_max'])
        ping = {
            'vehicle_id': f"{route}_{random.randint(100,999)}",
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'lat': lat,
            'lon': lon
        }
        data = json.dumps(ping).encode('utf-8')
        future = publisher.publish(topic_path, data)
        message_id = future.result(timeout=60)
    print(f"[{datetime.now()}] Published {len(ROUTES)} pings, last message ID: {message_id}")
    return message_id 







@functions_framework.http
def handler(request):
    start = time.time()
    while time.time() - start < MAX_RUNTIME:
        run_publisher()
        time.sleep(INTERVAL)
    return ("GPS publishing cycle complete", 200)
