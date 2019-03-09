create table if not exists play (
  id               integer primary key,
  played_at        text,
  track_id         text,
  track_name       text,
  album_id         text,
  album_name       text,
  main_artist_id   text,
  main_artist_name text,
  foreign key (track_id) references track (track_id),
  foreign key (main_artist_id) references artist (artist_id),
  foreign key (album_id) references album (album_id)
);

create table if not exists track (
  track_id         text primary key,
  name             text,
  duration_ms      int,
  popularity       int,
  album_id         text,
  track_number     int,
  disc_number      int,
  valance          float,
  tempo            float,
  danceability     float,
  energy           float,
  instrumentalness float,
  speechiness      float,
  time_signature   int,
  acousticness     float,
  loadness         float,
  liveness         float,
  foreign key (album_id) references album (album_id)
);

create table if not exists album (
  album_id               text primary key,
  name                   text,
  release_date_precision text,
  release_date           text,
  album_type             text,
  artwork_large_url      text,
  artwork_medium_url     text,
  artwork_small_url      text,
  label                  text,
  popularity             int,
  genres                 text
);

create table if not exists artist (
  artist_id  text primary key,
  name       text,
  popularity int,
  followers  int,
  genres     text
);

create table if not exists album_artist (
  album_id  text,
  artist_id text,
  foreign key (album_id) references album (album_id),
  foreign key (artist_id) references artist (artist_id)
);

create table if not exists track_artist (
  track_id  text,
  artist_id text,
  foreign key (track_id) references track (track_id),
  foreign key (artist_id) references artist (artist_id)
);

--IGNORE_ERROR
alter table play add column context text;