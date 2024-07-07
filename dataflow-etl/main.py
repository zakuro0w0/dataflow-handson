import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import csv

# パイプラインオプションを設定
pipeline_options = PipelineOptions()
google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'dataflow-etl-handson'
google_cloud_options.region = 'asia-northeast2'
google_cloud_options.job_name = 'dataflow-etl-example'
google_cloud_options.staging_location = 'gs://dataflow-etl-handson/staging'
google_cloud_options.temp_location = 'gs://dataflow-etl-handson/temp'
pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

class ParseCSV(beam.DoFn):
    def process(self, element):
        import csv
        for row in csv.DictReader([element]):
            yield row

def run():
    with beam.Pipeline(options=pipeline_options) as p:
        rows = (
            p
            | 'Read CSV' >> beam.io.ReadFromText('gs://dataflow-etl-handson/etl-data.csv', skip_header_lines=1)
            | 'Parse CSV' >> beam.ParDo(ParseCSV())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                'dataflow-etl-handson:dataflow_etl.personal_data',
                schema='id:INTEGER, name:STRING, age:INTEGER, city:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
