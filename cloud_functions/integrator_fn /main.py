import os
import functions_framework
from google.cloud import bigquery

BQ_PROJECT = os.environ['BQ_PROJECT']      # cityprogressmobilityl2c
INTEGRATED_TABLE = os.environ['INTEGRATED_TABLE']  
# e.g. "cityprogressmobilityl2c.real_time.integrated"

@functions_framework.http
def handler(request):
    client = bigquery.Client(project=BQ_PROJECT)
    sql = open("integration_query.sql").read()
    job = client.query(sql)  # runs the CREATE OR REPLACE TABLE
    job.result()  # wait for completion
    return (f"Integrated table updated, {job.num_dml_affected_rows} rows upserted", 200)
