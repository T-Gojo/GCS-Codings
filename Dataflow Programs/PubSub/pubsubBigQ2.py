# Attached with the pubsubRandomValueGenerator.py code
# After running the above file run this file ["pubsubBigQ2.py"] by Dataflow Runner to create Job
# for inserting data into BigQuery
# The Dataflow Runner command is in -----------------> PubsubRunnerCode-DATAFLOWRUNNER.txt


# --------------------------------------------------------------
# python pubSubStreaming2.py \
#   --project=practice-id1 \
#   --region=asia-east2 \
#   --input_topic=projects/practice-id1/topics/in-2-gcstobucket \
#   --output_path=gs://pubsubtest-1/output/slidingwindows/output \
#   --runner=DataflowRunner \
#   --window_size=2 \
#   --num_shards=5 \
#   --temp_location=gs://pubsubtest-1/temp
# --------------------------------------------------------------

# Standard Python Imports
import argparse
import itertools
import logging
import datetime
import time
import base64
import json

# 3rd Party Imports
import apache_beam as beam
import six as six
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions


def parse_json(line):
    record = json.loads(line)
    return record


def decode_message(line):
    return base64.urlsafe_b64decode(line)


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_mode',
                        default='stream',
                        help='Streaming input or file based batch input')

    parser.add_argument('--input_topic',
                        default='projects/practice-id1/topics/pubsubtobigq',
                        required=True,
                        help='Topic to pull data from.')

    parser.add_argument('--output_table',
                        required=True,
                        help=
                        ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
                         'or DATASET.TABLE.'))

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    if known_args.input_mode == 'stream':
        pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        price = (p
                 | "ReadInput" >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(
                    six.binary_type)
                 | "Decode" >> beam.Map(decode_message)
                 | "Parse" >> beam.Map(parse_json)
                 | "Write to Table" >> beam.io.WriteToBigQuery(
                    known_args.output_table,
                    schema=' timestamp:TIMESTAMP, stock_price:FLOAT',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
