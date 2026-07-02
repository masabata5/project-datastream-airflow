import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

# Define a helper function to convert CSV rows into dictionaries
def parse_csv(element):
    # Splits the CSV row by commas. Adjust if your file uses tabs or semicolons.
    columns = element.split(',')
    
    # Mapping the CSV columns to BigQuery fields. 
    # Modify these keys/indices based on your actual us-countries.csv headers!
    return {
        'date': columns[0].strip(),
        'country': columns[1].strip(),
        'cases': int(columns[2].strip()) if columns[2].strip().isdigit() else 0,
        'deaths': int(columns[3].strip()) if columns[3].strip().isdigit() else 0
    }

def run():
    # 1. Define Pipeline Options
    options = PipelineOptions()
    
    # Configure GCP specific parameters
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'your-gcp-project-id'
    gcp_options.region = 'us-central1'  # Choose your bucket/BQ region
    gcp_options.job_name = 'gcs-to-bigquery-csv'
    
    # Define variables for input and output
    input_file = 'gs://your-bucket-name/us-countries.csv'
    output_table = 'your-gcp-project-id:your_dataset.us_countries'
    
    # Define the BigQuery Schema
    table_schema = {
        'fields': [
            {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cases', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'deaths', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ]
    }

    # 2. Build and run the pipeline
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            # Read from GCS (skip_header_lines=1 ignores the header row)
            | 'Read From GCS' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            
            # Transform CSV strings into dictionaries
            | 'Parse CSV Rows' >> beam.Map(parse_csv)
            
            # Write into BigQuery
            | 'Write To BigQuery' >> beam.io.WriteToBigQuery(
                output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, # Overwrites table. Use WRITE_APPEND to add new data.
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()