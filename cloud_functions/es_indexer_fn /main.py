import os
import json
from datetime import datetime
from google.cloud import bigquery, secretmanager
from elasticsearch import Elasticsearch, helpers

# ------------------------------------------------------------------------------
# 1) Helper to fetch a Secret Manager secret (version “latest”)
# ------------------------------------------------------------------------------

def access_secret(secret_name: str) -> str:
    """
    Reads the latest version of the secret from Secret Manager.
    Expects the environment variable GCP_PROJECT to be set.
    """
    project_id = os.getenv("GCP_PROJECT")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


# ------------------------------------------------------------------------------
# 2) On cold start, fetch ES_ENDPOINT and ES_API_KEY from Secret Manager
# ------------------------------------------------------------------------------

ES_ENDPOINT = access_secret("elastic-endpoint")
ES_API_KEY   = access_secret("elastic-api-key")

# Initialize Elasticsearch client once (reused across invocations)
es = Elasticsearch(
    [ES_ENDPOINT],
    api_key=ES_API_KEY    # directly pass the single Base64-encoded string
)

# Initialize BigQuery client once
bq_client = bigquery.Client()


# ------------------------------------------------------------------------------
# 3) Cloud Function entrypoint
# ------------------------------------------------------------------------------

def handler(request):
    """
    Cloud Function that:
    1) Queries BigQuery for the last 6 hours of rows in real_time.integrated,
    2) Transforms each row into an ES document,
    3) Bulk-inserts into the transit-integrated index (upsert by _id).
    """

    # 3.A. Define the timestamp cutoff = “now − 6 hours”
    now = datetime.utcnow()
    six_hours_ago = now.replace(microsecond=0)  # drop micros for readability
    six_hours_ago_ts = six_hours_ago.isoformat() + "Z"  # e.g. "2025-06-02T12:34:00Z"

    # 3.B. Build & run the BigQuery query
    query = f"""
    SELECT
      vehicle_id,
      ping_ts,
      stop_id,
      schedu_ts,
      delay_sec,
      lat,
      lon,
      incident_count
    FROM
      `cityprogressmobilityl2c.real_time.integrated`
    WHERE
      ping_ts >= TIMESTAMP("{six_hours_ago_ts}")
    """

    # Use a default timeout of 60 seconds (6-hour window is small enough)
    try:
        query_job = bq_client.query(query)
        rows = list(query_job.result())  # fetch all matching rows
    except Exception as e:
        print(f"[ERROR] BigQuery query failed: {e}")
        return (f"BigQuery query error: {str(e)}", 500)

    if not rows:
        # No new rows in the last 6 hours → nothing to index
        print("No rows from last 6 hours; exiting.")
        return ("No documents to index", 200)

    # 3.C. Build ES bulk-index payload
    actions = []
    for row in rows:
        # Unique document ID: "<vehicle_id>_<milliseconds_of_ping_ts>"
        millis = int(row.ping_ts.timestamp() * 1000)
        doc_id = f"{row.vehicle_id}_{millis}"

        # Construct the ES document body
        doc_body = {
            "vehicle_id":     row.vehicle_id,
            "ping_ts":        row.ping_ts.isoformat(),
            "stop_id":        row.stop_id,
            "schedu_ts":      row.schedu_ts.isoformat(),
            "delay_sec":      row.delay_sec,
            "location":       {"lat": row.lat, "lon": row.lon},
            "incident_count": row.incident_count
        }

        actions.append({
            "_index": "transit-integrated",
            "_id":    doc_id,
            "_source": doc_body
        })

    # 3.D. Bulk-insert into Elasticsearch
    try:
        success, _ = helpers.bulk(es, actions)
        # success = number of documents indexed
    except Exception as e:
        print(f"[ERROR] Elasticsearch bulk insert failed: {e}")
        return (f"Elasticsearch error: {str(e)}", 500)

    print(f"Indexed {success} documents into transit-integrated.")
    return (f"Indexed {success} documents.", 200)
