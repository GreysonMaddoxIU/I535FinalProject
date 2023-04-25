# Purpose of Function: Upload the 1000 JSON files consisting of 1 million playlists to BigQuery as CSVs to allow it to be more easily uploaded to BigQuery tables
	
# pipeline.conf is a file consisting of information to be used in this function, specificaly the GCP storage bucket and path to my credentials. The file path is the downloaded path of the Zipped file


import requests
import json
import os
import configparser
import csv
from google.cloud import storage
import zipfile
from unidecode import unidecode
from time import perf_counter

file_path = "C:/Users/greys/i535final/Scripts/spotify_million_playlist_dataset.zip"
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = parser.get("google_cloud_storage_credentials", "credential_path")
bucket_name = parser.get("google_cloud_storage_credentials", "bucket_name")
storage_client = storage.Client()
blobs = storage_client.list_blobs(bucket_name)
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob_list = [blob.name for blob in blobs]
with zipfile.ZipFile(file_path, 'r') as zip:
    file_names = zip.namelist()
    file_count = 0
    for name in file_names:
        if '.json' in name and name.split('.')[2] + '.json' not in blob_list:
            tracks = []
            print('Looking at file ' + str(file_count))
            file_count += 1
            file = zip.open(name, 'r').read()
            json_file = json.loads(file)
            start = perf_counter()
            for playlist in json_file["playlists"]:
                for track in playlist["tracks"]:
                    song = []
                    artist_uri = track["artist_uri"]
                    track_uri = track["track_uri"]
                    album_uri = track["album_uri"]
                    song = [track_uri, artist_uri, album_uri]
                    tracks.append(song)
            end = perf_counter()
            print(f"Iterating through playlists took {end - start:0.4f} seconds")
            local_filename = name.split('.')[2] + '.json'
            start = perf_counter()
            with open(local_filename, 'w', encoding="utf-8", newline = '') as fp:
              csv_w = csv.writer(fp, delimiter=',') 
              csv_w.writerows(tracks)
              fp.close()
            end = perf_counter()
            print(f"Writing the CSV took {end - start:0.4f} seconds")
            start = perf_counter()
            blob = bucket.blob(local_filename)
            blob.upload_from_filename(local_filename, timeout = 300)
            end = perf_counter()
            print(f"Uploading to GCP took {end - start:0.4f} seconds")
            print("Uploaded " + str(local_filename) + " to " + str(bucket_name))
            os.remove(local_filename)