# Purpose of Function: Access the Spotify API for album information and add to the album dimension table as well as album-artists and album-genres
	
# bigquerycredentials.json is a private file consisting of my credentials and the ClientId and ClientSecret used for spotify are hidden

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import bigquery
from google.oauth2 import service_account
from time import sleep
import pandas

def process_spotify_albums(event, context):
	credentials = service_account.Credentials.from_service_account_file("bigquerycredentials.json")
	client = bigquery.Client(credentials=credentials, project="greyson-project")
	query_job = client.query("""
  	SELECT AlbumUri
  	FROM `greyson-project.spotify_songs.pending-albums` LIMIT 3000
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
	album_insert = []
	album_genre_insert = []
	album_artist_insert = []
	while count < len(results):
		if cpp.is_token_expired(token_info):
			cpp = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
			access_token = cpp.get_access_token(as_dict=False)
			token_info = cpp.get_access_token(as_dict=True)
			sp = spotipy.client.Spotify(auth = access_token) 
			print('Token Expired, Getting New One')
		albums = []
		for j in range(count, count+20):
			try:
				albums.append(results.iloc[j]["AlbumUri"])
			except:
				print('Max Index Exceeded')
		album_results = sp.albums(albums)
		for album in album_results['albums']:
			try:
				AlbumUri = results.iloc[count]["AlbumUri"]
				AlbumName = album['name']
				ReleaseDate = album['release_date']
				ReleaseDatePrecision = album['release_date_precision']
				Popularity = album['popularity']
				TrackCount = album['total_tracks']
				if album['popularity'] is not None:
					Popularity = album['popularity']
				if album['artists'] is not None:
					for artist in album['artists']:
						ArtistUri = artist['uri']
						album_artist_insert.append((str(AlbumUri), str(ArtistUri)))
				if album['genres'] is not None:
					for genre in album['genres']:
						GenreName = genre
						album_genre_insert.append((str(AlbumUri), str(GenreName)))					
				album_insert.append((str(AlbumUri), str(AlbumName), str(ReleaseDate), str(ReleaseDatePrecision), int(Popularity), int(TrackCount)))
			except:
				print('Error looking at album ' + str(count))
				album_insert.append((str(AlbumUri), 'Deleted Album', '', '', 0, 0))
			count += 1
		print('Looked at albums ' + str(count - 19) + ' through ' + str(count))
		sleep(3)
	dataset_id = 'spotify_songs'
	schematable_id = 'album-dimensions'
	table_ref = client.dataset(dataset_id).table(schematable_id)
	table = client.get_table(table_ref)
	client.insert_rows(table, album_insert)
	if len(album_genre_insert) > 0:
		schematable_id = 'album-genres'
		table_ref = client.dataset(dataset_id).table(schematable_id)
		table = client.get_table(table_ref)
		client.insert_rows(table, album_genre_insert)
	if len(album_artist_insert) > 0:
		schematable_id = 'album-artists'
		table_ref = client.dataset(dataset_id).table(schematable_id)
		table = client.get_table(table_ref)
		client.insert_rows(table, album_artist_insert)