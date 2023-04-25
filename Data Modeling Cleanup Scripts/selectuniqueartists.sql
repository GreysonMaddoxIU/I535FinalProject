/*
Purpose of Function: Insert only unique artists into the temporary artists dimensions table (which would later become the new artists dimension table)
*/

INSERT INTO `greyson-project.spotify_songs.artist-dimensions-temp`
SELECT * FROM `greyson-project.spotify_songs.artist-dimensions`
GROUP BY ArtistUri, ArtistName, Popularity, Followers
HAVING COUNT(ArtistUri) = 1 AND ArtistUri IN (SELECT DISTINCT ArtistUri FROM `spotify_songs.song-records-aggs`)