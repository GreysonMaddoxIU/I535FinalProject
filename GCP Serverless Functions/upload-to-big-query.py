# Purpose of Function: When a CSV is added to a GCP storage bucket, add that CSV as rows into the song-records fact table

import os
from google.cloud import bigquery
def csv_loader(event, context):
  client = bigquery.Client()
  dataset_id = 'spotify_songs'
  dataset_ref = client.dataset(dataset_id)
  job_config = bigquery.LoadJobConfig()
  job_config.schema = [
    bigquery.SchemaField('TrackUri', 'STRING'),
    bigquery.SchemaField('ArtistUri', 'STRING'),
    bigquery.SchemaField('AlbumUri', 'STRING')]
  job_config.skip_leading_rows = 0
  job_config.source_format = bigquery.SourceFormat.CSV
  uri = 'gs://greyson-spotify-bucket/' + event['name']
  load_job = client.load_table_from_uri(
    uri,
    dataset_ref.table('song-records'),
    job_config=job_config)
  print('Starting job {}'.format(load_job.job_id))
  print('File: {}'.format(event['name']))
  load_job.result()
  print('Job finished.')
  destination_table = client.get_table(dataset_ref.table('song-records'))