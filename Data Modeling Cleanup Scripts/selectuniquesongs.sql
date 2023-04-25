/*
Purpose of Function: Insert only unique songs into the temporary songs dimensions table (which would later become the new songs dimension table)
*/

INSERT INTO `greyson-project.spotify_songs.song-dimensions_copy`
SELECT TrackUri, TrackName, MAX(DurationMS), Explicit, MAX(Popularity) FROM `greyson-project.spotify_songs.song-dimensions`
WHERE TrackUri IN (SELECT TrackUri FROM `spotify_songs.song-records-aggs`)
GROUP BY 1, 2, 4
