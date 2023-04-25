# Purpose of Function: Access the Spotify API for song feature information and add to the song features dimension table
	
# bigquerycredentials.json is a private file consisting of my credentials and the ClientId and ClientSecret used for spotify are hidden

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import bigquery
from google.oauth2 import service_account
from time import sleep
import pandas

def process_spotify_song_features(event, context):
	credentials = service_account.Credentials.from_service_account_file("bigquerycredentials.json")
	client = bigquery.Client(credentials=credentials, project="greyson-project")

	query_job = client.query("""
  	SELECT TrackUri
  	FROM `greyson-project.spotify_songs.pending-song-audio-features` LIMIT 12000
  	""")
    
	results = query_job.to_dataframe()
	if len(results) == 0:
		return True
	client_id = '***'
	client_secret = '***'
	cpp = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
	access_token = cpp.get_access_token(as_dict=False)
	token_info = cpp.get_access_token(as_dict=True)
	print(access_token)
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
		for j in range(count, count+100):
			try:
				songs.append(results.iloc[j]["TrackUri"])
			except:
				print('Over Max Index')
		song_results = sp.audio_features(songs)
		print(song_results)
		for track in song_results:
			try:
				TrackUri = results.iloc[j]["TrackUri"]
				Acousticness = track['acousticness']
				Danceability = track['danceability']
				Energy = track['energy']
				Instrumentalness = track['instrumentalness']
				Liveness = track['liveness']
				Loudness = track['loudness']
				Speechiness = track['speechiness']
				Tempo = track['tempo']
				TimeSignature = track['time_signature']
				Valence = track['valence']
				rows_insert.append((str(TrackUri), float(Acousticness), float(Danceability), float(Energy), float(Instrumentalness), 
									float(Liveness), float(Loudness), float(Speechiness), float(Tempo), int(TimeSignature), float(Valence)))
			except:
				print('Error looking at track ' + str(count))
				rows_insert.append((str(results.iloc[count]["TrackUri"]), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0))
			count += 1
		print('Looked at songs ' + str(count - 99) + ' through ' + str(count))
		sleep(3)
	dataset_id = 'spotify_songs'
	schematable_id = 'song-audio-features'
	table_ref = client.dataset(dataset_id).table(schematable_id)
	table = client.get_table(table_ref)
	client.insert_rows(table, rows_insert)