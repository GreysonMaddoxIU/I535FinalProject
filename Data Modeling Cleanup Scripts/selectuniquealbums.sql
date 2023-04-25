/*
Purpose of Function: Insert only unique albums into the temporary album dimensions table (which would later become the new album dimension table)
*/

INSERT INTO `greyson-project.spotify_songs.album-dimensions-temp`
SELECT AlbumUri, AlbumName, ReleaseDate, ReleaseDatePrecision, MAX(Popularity), MAX(TrackCount)
FROM `spotify_songs.album-dimensions`
GROUP BY 1, 2, 3, 4