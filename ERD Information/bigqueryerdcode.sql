/*
Purpose of this function: Code to create the ERD seen in Appendix A on dbdiagram.io
*/

Table "song-records" {
  TrackUri string
  ArtistUri string
  AlbumUri string 
}

Table "song-records-aggs" {
  TrackUri string
  ArtistUri string
  AlbumUri string
  Occurrences integer
}

Table "song-dimensions" {
  TrackUri string
  TrackName string
  DurationMS integer
  Explicit boolean
  Popularity integer
}

Table "artist-dimensions" {
  ArtistUri string
  ArtistName string
  Popularity integer
  Followers integer
}

Table "album-dimensions" {
  AlbumUri string
  AlbumName string
  ReleaseDate string
  ReleaseDatePrecision string
  Popularity integer
  TrackCount integer
}

Table "song-audio-features" {
  TrackUri string
  Acousticness float
  Danceability float
  Energy float
  Instrumentalness float
  Liveness float
  Loudness float
  Speechiness float
  Tempo float
  TimeSignature integer
  Valence float
}

Table "album-artists" {
  AlbumUri string
  ArtistUri string
}

Table "album-genres" {
  AlbumUri string
  Genre string
}

Table "artist-genres" {
  ArtistUri string
  Genre string
}

Table "pending-albums (VIEW)" {
  AlbumUri string
}

Table "pending-artists (VIEW)" {
  ArtistUri string
}

Table "pending-songs (VIEW)" {
  TrackUri string
}

Table "pending-song-audio-features (VIEW)" {
  TrackUri string
}

Ref: "song-records".TrackUri > "song-dimensions".TrackUri

Ref: "song-records".TrackUri > "song-audio-features".TrackUri

Ref: "song-records".AlbumUri > "album-dimensions".AlbumUri

Ref: "song-records".ArtistUri > "artist-dimensions".ArtistUri

Ref: "song-records-aggs".AlbumUri - "pending-albums (VIEW)".AlbumUri

Ref: "song-records-aggs".ArtistUri - "pending-artists (VIEW)".ArtistUri

Ref: "song-records-aggs".TrackUri - "pending-songs (VIEW)".TrackUri

Ref: "song-records-aggs".TrackUri - "pending-song-audio-features (VIEW)".TrackUri

Ref: "album-dimensions".AlbumUri - "pending-albums (VIEW)".AlbumUri

Ref: "artist-dimensions".ArtistUri - "pending-artists (VIEW)".ArtistUri

Ref: "song-dimensions".TrackUri - "pending-songs (VIEW)".TrackUri

Ref: "song-audio-features".TrackUri - "pending-song-audio-features (VIEW)".TrackUri

Ref: "album-dimensions".AlbumUri > "album-artists".AlbumUri

Ref: "artist-dimensions".ArtistUri > "album-artists".ArtistUri

Ref: "artist-dimensions".ArtistUri > "artist-genres".ArtistUri

Ref: "album-dimensions".AlbumUri > "album-genres".AlbumUri
