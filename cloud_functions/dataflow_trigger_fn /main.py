# cloud_functions/dataflow_trigger_fn/main.py

import os
import threading
import functions_framework
import json, base64, apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


# Environment variables
PROJECT_ID         = os.environ['PROJECT_ID']
INPUT_SUBSCRIPTION = os.environ['INPUT_SUBSCRIPTION']
OUTPUT_TABLE       = os.environ['OUTPUT_TABLE']


# cloud_functions/dataflow_trigger_fn/dataflow_pipeline.py


class ParsePubSubMessage(beam.DoFn):
    def process(self, data_bytes):
        record = json.loads(data_bytes.decode('utf-8'))
        yield {
            'vehicle_id': record['vehicle_id'],
            'timestamp': record['timestamp'],
            'lat': float(record['lat']),
            'lon': float(record['lon']),
        }

def run_pipeline(project, input_subscription, output_table):
    """
    Launches a streaming Beam pipeline that reads from Pub/Sub and writes to BigQuery.
    Blocks until the pipeline is running (does not terminate).
    """
    options = PipelineOptions(
        [
            f"--project={project}",
            f"--streaming"
        ],
        save_main_session=True
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | 'ParseJSON'      >> beam.ParDo(ParsePubSubMessage())
            | 'WriteToBQ'      >> beam.io.WriteToBigQuery(
                                   output_table,
                                   write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                   create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
                                )
        )
    return


@functions_framework.http
def handler(request):
    """
    HTTP entrypoint that starts the Dataflow pipeline in a background thread
    and immediately returns HTTP 200.
    """
    try:
        # Fire-and-forget so HTTP doesn’t block indefinitely
        t = threading.Thread(
            target=run_pipeline,
            args=(PROJECT_ID, INPUT_SUBSCRIPTION, OUTPUT_TABLE),
            daemon=True
        )
        t.start()
        return ("Dataflow pipeline launching", 200)
    except Exception as e:
        return (f"Error launching pipeline: {e}", 500)
