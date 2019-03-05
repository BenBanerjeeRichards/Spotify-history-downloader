import sqlite3
import util
from spotify import Credentials, get_albums


class DbStore:

    def __init__(self):
        cfg = util.config()
        self.conn = sqlite3.connect(cfg["db"]["db_sqlite_file"])
        schema = open("db/db_schema.sql").read()
        for statement in schema.split(";"):
            self.conn.execute(statement)
        self.conn.commit()

    # Adds a play event using data from spotify api
    # If needed creates rows in artist, album, ... tables with incomplete information
    # The full information can then later be filled in using the detailed spotify api calls
    def add_play(self, play):
        track = play["track"]
        album = track["album"]

        self.add_simple_album(album["id"], album["name"])

        for artist in track["artists"]:
            self.add_simple_artist(artist["id"], artist["name"])
            self.add_track_artist(track["id"], artist["id"])

        self.add_simple_track(track["id"], track["name"], track["duration_ms"], track["popularity"],
                              track["album"]["id"])

        for artist in album["artists"]:
            self.add_album_artist(album["id"], artist["id"])

        # Finally add play
        self.conn.execute("insert into play values (?, ?, ?, ?, ?, ?, ?, ?)",
                          (None, play["played_at"], play["track"]["id"], track["name"], album["id"], album["name"],
                           track["artists"][0]["id"], track["artists"][0]["name"]))

    # Used when transferring data over from mongo
    # Has different format than spotify API
    def add_play_from_mongo(self, mongo_play):
        play = {"played_at": mongo_play["played_at"].isoformat(), "track": mongo_play["track"]}

        return self.add_play(play)

    def add_simple_track(self, track_id, name, duration_ms, popularity, album_id):
        if self.exists("track", "track_id", track_id):
            return

        self.conn.execute("insert into track values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (track_id, name, duration_ms, popularity, album_id,
                           None, None, None, None, None, None, None, None, None, None, None, None))
        self.conn.commit()

    def update_full_track(self, track_id, track_number, disc_number, valence, tempo, danceability, energy,
                          instrumentalness, speechiness, time_signature, loadness, liveness, acousticness):
        self.conn.execute("""
          update track set
            track_number=?,
            disc_number=?,
            valance=?,
            tempo=?,
            danceability=?,
            energy=?,
            instrumentalness=?,
            speechiness=?,
            time_signature=?,
            loadness=?,
            liveness=?,
            acousticness=?
          where track_id=?
    """, (track_number, disc_number, valence, tempo, danceability, energy,
          instrumentalness, speechiness, time_signature, loadness, liveness, acousticness, track_id))

        self.conn.commit()

    def incomplete_track_ids(self):
        res = self.conn.execute("select track_id from track where track_number is null")
        ids = []
        for result in res.fetchall():
            ids.append(result[0])

        return ids

    def add_simple_artist(self, artist_id, name):
        if self.exists("artist", "artist_id", artist_id):
            return
        self.conn.execute("insert into artist values (?, ?, ?, ?, ?)",
                          (artist_id, name, None, None, None,))
        self.conn.commit()

    def incomplete_artist_ids(self):
        res = self.conn.execute("select artist_id from artist where popularity is null")
        ids = []
        for result in res.fetchall():
            ids.append(result[0])

        return ids

    def update_full_artist(self, artist_id, popularity, followers, genres):
        self.conn.execute("""
          update artist set
            popularity=?,
            followers=?,
            genres=?
          where artist_id = ?
          """, (popularity, followers, ",".join(genres), artist_id,))
        self.conn.commit()

    def incomplete_album_ids(self):
        res = self.conn.execute("select album_id from album where popularity is null")
        ids = []
        for result in res.fetchall():
            ids.append(result[0])

        return ids

    def update_full_album(self, album_id, release_day_precision, release_date, album_type,
                          artwork_large_url, artwork_medium_url, artwork_small_url, label, popularity, genres):
        self.conn.execute("""
          update album set
            release_date_precision=?,
            release_date=?,
            album_type=?,
            artwork_large_url=?,
            artwork_medium_url=?,
            artwork_small_url=?,
            label=?,
            popularity=?,
            genres=?
          where album_id = ?
          """, (release_day_precision, release_date, album_type,
                artwork_large_url, artwork_medium_url, artwork_small_url, label, popularity, ",".join(genres),
                album_id,))
        self.conn.commit()

    def add_simple_album(self, album_id, name):
        if self.exists("album", "album_id", album_id):
            return

        self.conn.execute("insert into album values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (album_id, name, None, None, None, None, None, None, None, None, None,))
        self.conn.commit()

    def add_album_artist(self, album_id, artist_id):
        find = self.conn.execute("select count(*) from album_artist where album_id=? and artist_id=?",
                                 (album_id, artist_id,))
        if find.fetchone()[0] == 0:
            self.conn.execute("insert into album_artist values (?, ?)",
                              (album_id, artist_id,))
            self.conn.commit()

    def add_track_artist(self, track_id, artist_id):
        find = self.conn.execute("select count(*) from track_artist where track_id=? and artist_id=?",
                                 (track_id, artist_id,))
        if find.fetchone()[0] == 0:
            self.conn.execute("insert into album_artist values (?, ?)",
                              (track_id, artist_id,))
            self.conn.commit()

    def exists(self, table, column, value):
        query = "select count(*) from {} where {} = ?".format(table, column)
        res = self.conn.execute(query, (value,))
        return res.fetchone()[0] > 0

    def most_recent_played_at(self):
        res = self.conn.execute("select played_at from play order by played_at desc limit 1")
        return res.fetchone()[0]
