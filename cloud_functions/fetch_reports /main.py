import os
import io
from datetime import datetime, timezone
import functions_framework
from google.cloud import storage

# Environment
RAW_BUCKET      = os.environ['RAW_BUCKET']       # e.g. cityprogressmobilityl2c-incidents-raw
PROCESSED_BUCKET= os.environ['PROCESSED_BUCKET'] # e.g. cityprogressmobilityl2c-incidents
PREFIX          = os.environ.get('FOLDER_PREFIX','reports')

storage_client = storage.Client()
raw_bucket      = storage_client.bucket(RAW_BUCKET)
processed_bucket= storage_client.bucket(PROCESSED_BUCKET)

def get_today_folder() -> str:
    dt = datetime.now(timezone.utc)
    return f"{PREFIX}_{dt.strftime('%Y%m%d')}/"

def fetch_and_write():
    folder = get_today_folder()

    # Ensure placeholder exists
    placeholder = processed_bucket.blob(folder + ".keep")
    if not placeholder.exists():
        placeholder.upload_from_string("", content_type="text/plain")

    # Determine next index
    existing = list(processed_bucket.list_blobs(prefix=folder))
    next_idx = len([b for b in existing if b.name.endswith('.txt')])

    # 1) Handle news.txt
    news_blob = raw_bucket.blob("news.txt")
    if news_blob.exists():
        content = news_blob.download_as_text()
        dest = processed_bucket.blob(f"{folder}report_{next_idx:04d}.txt")
        dest.upload_from_string(content, content_type="text/plain")
        next_idx += 1

    # 2) Handle tweets.txt
    tweets_blob = raw_bucket.blob("tweets.txt")
    if tweets_blob.exists():
        tweets = tweets_blob.download_as_text().strip().split("\n\n")
        for tweet in tweets:
            # each tweet is two lines: tweet: ...\ntime: ...
            dest = processed_bucket.blob(f"{folder}report_{next_idx:04d}.txt")
            dest.upload_from_string(tweet, content_type="text/plain")
            next_idx += 1

@functions_framework.http
def handler(request):
    try:
        fetch_and_write()
        return ("Raw reports fetched into processed folder", 200)
    except Exception as e:
        return (f"Error in fetchRawReports: {e}", 500)
