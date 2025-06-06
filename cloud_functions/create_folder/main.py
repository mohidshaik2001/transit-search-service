# cloud_functions/create_folder/main.py
import os
from datetime import datetime, timezone
import functions_framework
from google.cloud import storage

BUCKET = os.environ['INCIDENTS_BUCKET']
PREFIX = os.environ.get('FOLDER_PREFIX', 'reports')
storage_client = storage.Client()

@functions_framework.http
def handler(request):
    # timezone-aware “today”
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    folder = f"{PREFIX}_{date_str}/"
    blob = storage_client.bucket(BUCKET).blob(folder + ".placeholder")
    blob.upload_from_string("", content_type="text/plain")
    return (f"Folder created: {folder}", 200)
