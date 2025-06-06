# cloud_functions/gtfs_processor_fn/main.py

import os
import io
import pandas as pd
from google.cloud import storage
import functions_framework  # <-- the Functions Framework

# Your GTFS logic, copied from gtfs_processor.py (inlined here to simplify)
BUCKET = os.environ['BUCKET']  # set via --set-env-vars

def load_df(client, blob_name):
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(f"{blob_name}")
    data = blob.download_as_text()
    return pd.read_csv(io.StringIO(data))

def upload_df(client, df, target_path):
    bucket = client.bucket(BUCKET)
    out_blob = bucket.blob(target_path)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    out_blob.upload_from_string(csv_bytes, content_type="text/csv")

def process_gtfs():
    client = storage.Client()
    # load tables
    agency_df = load_df(client, "agency.txt")
    cal_df    = load_df(client, "calendar.txt")
    routes_df = load_df(client, "routes.txt")
    trips_df  = load_df(client, "trips.txt")
    stop_times_df = load_df(client, "stop_times.txt")
    stops_df      = load_df(client, "stops.txt")
    # compute bounds
    merged = (
        routes_df[['route_id','route_short_name']]
        .merge(trips_df[['route_id','trip_id']], on='route_id')
        .merge(stop_times_df[['trip_id','stop_id','arrival_time']], on='trip_id')
        .merge(stops_df[['stop_id','stop_name','stop_lat','stop_lon']], on='stop_id')
    )
    bounds = (
        merged.groupby('route_id')
        .agg(lat_max=('stop_lat','max'),
             lat_min=('stop_lat','min'),
             lon_max=('stop_lon','max'),
             lon_min=('stop_lon','min'))
        .reset_index()
    )
    # compute summary
    gtfs_summary = stop_times_df.merge(trips_df, on='trip_id')
    gtfs_summary = gtfs_summary.rename(columns={'arrival_time':'scheduled_time'})
    gtfs_summary = gtfs_summary.drop(
        columns=['departure_time','stop_sequence','trip_id','service_id','shape_id'],
        errors='ignore'
    )
    # upload outputs
    upload_df(client, bounds,       "Processed/route_bounds.csv")
    upload_df(client, gtfs_summary, "Processed/gtfs_summary.csv")

@functions_framework.http
def handler(request):
    """
    HTTP Cloud Function (Gen 2) entry point.
    Invoked when the / endpoint receives a request.
    """
    try:
        process_gtfs()
        return ("GTFS processing complete", 200)
    except Exception as e:
        return (f"Error during GTFS processing: {e}", 500)
