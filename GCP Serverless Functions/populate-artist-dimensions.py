# Purpose of Function: Access the Spotify API for artist information and add to the artist dimension table as well as artist-genres
	
# bigquerycredentials.json is a private file consisting of my credentials and the ClientId and ClientSecret used for spotify are hidden

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import bigquery
from google.oauth2 import service_account
from time import sleep
import pandas

def process_spotify_artists(event, context):
	credentials = service_account.Credentials.from_service_account_file("bigquerycredentials.json")
	client = bigquery.Client(credentials=credentials, project="greyson-project")
	query_job = client.query("""
  	SELECT ArtistUri
  	FROM `greyson-project.spotify_songs.pending-artists` LIMIT 6000
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
	artist_insert = []
	artist_genre_insert = []
	while count < len(results):
		if cpp.is_token_expired(token_info):
			cpp = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
			access_token = cpp.get_access_token(as_dict=False)
			token_info = cpp.get_access_token(as_dict=True)
			sp = spotipy.client.Spotify(auth = access_token) 
			print('Token Expired, Getting New One')
		artists = []
		for j in range(count, count+50):
			try:
				artists.append(results.iloc[j]["ArtistUri"])
			except:
				print('Over Max Index')
		artist_results = sp.artists(artists)
		for artist in artist_results['artists']:
			try:
				if artist['uri'] is not None:
					ArtistUri = results.iloc[count]["ArtistUri"]
				else:
					ArtistUri = ''
				if artist['name'] is not None:
					ArtistName = artist['name']
				else:
					ArtistName = ''
				if artist['popularity'] is not None:
					Popularity = artist['popularity']
				else:
					Popularity = 0
				if artist['followers'] is not None:
					if artist['followers']['total'] is not None:
						Followers = artist['followers']['total']
					else:
						Followers = 0
				else:
					Followers = 0
				if artist['genres'] is not None:
					for genre in artist['genres']:
						GenreName = genre
						artist_genre_insert.append((str(ArtistUri), str(GenreName)))				
				artist_insert.append((str(ArtistUri), str(ArtistName), int(Popularity), int(Followers)))
			except:
				print('Error looking at artist ' + str(count))
			count += 1
		print('Looked at artists ' + str(count - 49) + ' through ' + str(count))
		sleep(3)
	dataset_id = 'spotify_songs'
	schematable_id = 'artist-dimensions'
	table_ref = client.dataset(dataset_id).table(schematable_id)
	table = client.get_table(table_ref)
	client.insert_rows(table, artist_insert)
	if len(artist_genre_insert) > 0:
		schematable_id = 'artist-genres'
		table_ref = client.dataset(dataset_id).table(schematable_id)
		table = client.get_table(table_ref)
		client.insert_rows(table, artist_genre_insert)