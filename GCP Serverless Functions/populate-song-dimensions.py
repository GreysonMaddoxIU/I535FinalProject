# Purpose of Function: Access the Spotify API for song information and add to the song dimension table
	
# bigquerycredentials.json is a private file consisting of my credentials and the ClientId and ClientSecret used for spotify are hidden

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import bigquery
from google.oauth2 import service_account
from time import sleep
import pandas

def process_spotify_songs(event, context):
	credentials = service_account.Credentials.from_service_account_file("bigquerycredentials.json")
	client = bigquery.Client(credentials=credentials, project="greyson-project")

	query_job = client.query("""
  	SELECT TrackUri
  	FROM `greyson-project.spotify_songs.pending-songs` LIMIT 6000
  	""")
    
	results = query_job.to_dataframe()
	if len(results) == 0:
		return True
	client_id = '***'
	client_secret = '***'
	cpp = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
	access_token = cpp.get_access_token(as_dict=False)
	token_info = cpp.get_access_token(as_dict=True)
	sp = spotipy.client.Spotify(auth=access_token, retries=0, status_retries=0)
  
	count = 0
	rows_insert = []
	while count < len(results):
		if cpp.is_token_expired(token_info):
			cpp = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
			access_token = cpp.get_access_token(as_dict=False)
			token_info = cpp.get_access_token(as_dict=True)
			sp = spotipy.client.Spotify(auth = access_token) 
			print('Token Expired, Getting New One')
		songs = []
		for j in range(count, count+50):
			try:
				songs.append(results.iloc[j]["TrackUri"])
			except:
				print('Over Max Index')
		song_results = sp.tracks(songs)
		for track in song_results['tracks']:
			try:
				TrackUri = results.iloc[count]["TrackUri"]
				TrackName = track['name']
				DurationMS = track['duration_ms']
				Explicit = track['explicit']
				Popularity = track['popularity']
				rows_insert.append((str(TrackUri), str(TrackName), int(DurationMS), bool(Explicit), int(Popularity)))
			except:
				print('Error looking at track ' + str(count))
				rows_insert.append((str(TrackUri), '', 0, False, 0))
			count += 1
		print('Looked at songs ' + str(count - 49) + ' through ' + str(count))
		sleep(3)
	dataset_id = 'spotify_songs'
	schematable_id = 'song-dimensions'
	table_ref = client.dataset(dataset_id).table(schematable_id)
	table = client.get_table(table_ref)
	client.insert_rows(table, rows_insert)