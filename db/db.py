import sqlite3
import util
import logging
import util
import csv
import datetime
import dateutil.parser


class DbStore:

    def __init__(self):
        cfg = util.config()
        self.conn = sqlite3.connect(util.get_path(cfg["db"]["db_sqlite_file"]))
        logging.debug("Got connection to database")

        schema = open(util.get_path("db/db_schema.sql")).read()
        for statement in schema.split(";"):
            try:
                self.conn.execute(statement)
            except sqlite3.OperationalError as e:
                if statement.strip().startswith("--IGNORE_ERROR"):
                    logging.info("Ignoring error thrown by statement {}: {}".format(statement, e))
                else:
                    raise e
        self.conn.commit()
        logging.debug("Done!")

        if self.get_db_version() == "1":
            logging.info("Got db version of 1, migrating to version 2")
            if self.migrate_genre():
                logging.info("It worked")
                self.set_db_version("2")
            else:
                logging.error("Failed to migrate genres")

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

        context = None
        if play["context"] is not None and "uri" in play["context"]:
            context = play["context"]["uri"]

        # Finally add play
        self.conn.execute("insert into play values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (None, play["played_at"], play["track"]["id"], track["name"], album["id"], album["name"],
                           track["artists"][0]["id"], track["artists"][0]["name"], context))

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

        for genre in genres:
            self.add_genre_to_artist(artist_id, genre)

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
            self.conn.execute("insert into track_artist values (?, ?)",
                              (track_id, artist_id,))
            self.conn.commit()

    def exists(self, table, column, value):
        query = "select count(*) from {} where {} = ?".format(table, column)
        res = self.conn.execute(query, (value,))
        return res.fetchone()[0] > 0

    def most_recent_played_at(self):
        res = self.conn.execute("select played_at from play order by played_at desc limit 1").fetchall()

        if len(res) == 0:
            return ""
        return res[0][0]

    def get_basic_tracks(self):
        return self.conn.execute(
            "select played_at, track_name, main_artist_name from play order by played_at asc").fetchall()

    def add_context(self, played_at, context_uri, no_commit=True):
        self.conn.execute("update play set context=? where played_at=?", (context_uri, played_at,))
        if not no_commit:
            self.conn.commit()

    def add_event(self, event):
        repeat = True if self.s(event, "repeat_state") == "on" else False

        data_tuple = (event["action"], event["prev_progress"], event["prev_timestamp"], event.get("prev_track_id"),
                      self.s(event, "timestamp"),
                      self.s(event, "api_timestamp"),
                      self.s(event, "track_id"), self.s(event, "progress_ms"), self.s(event, "duration_ms"),
                      self.s(event, "is_playing"), repeat, self.s(event, "shuffle_state"),
                      self.s_dev(event, "id"), self.s_dev(event, "is_active"), self.s_dev(event, "volume_percent"),
                      self.s_dev(event, "type"), self.s_dev(event, "name"))

        self.conn.execute("insert into event values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data_tuple)

    def latest_event(self):
        res_list = self.conn.execute("select * from event order by timestamp desc limit 1").fetchall()
        if len(res_list) == 0:
            return None
        event = res_list[0]
        return self.map_event(event)

    def events_with_track_id(self):
        res = self.conn.execute("select * from event where track_id is not null").fetchall()
        return list(map(lambda x: self.map_event(x), res))

    def map_event(self, event):
        return {
            "action": event[0],
            "prev_progress": event[1],
            "prev_timestamp": event[2],
            "prev_track_id": event[3],
            "timestamp": event[4],
            "state": {
                "timestamp": event[4],
                "api_timestamp": event[5],
                "track_id": event[6],
                "progress_ms": event[7],
                "duration_ms": event[8],
                "is_playing": event[9],
                "repeat_state": "on" if event[10] else "off",
                "shuffle_state": event[11],
                "device": {
                    "id": event[12],
                    "active": event[13],
                    "volume_percent": event[14],
                    "type": event[15],
                    "name": event[16]
                }
            }
        }

    def set_prev_track_id(self, timestamp, prev_track_id):
        self.conn.execute("update event set prev_track_id = ? where timestamp=?", (prev_track_id, timestamp,))

    def s(self, event, k):
        if event.get("state") is None:
            return None
        return event["state"].get(k)

    def s_dev(self, event, k):
        if event.get("state") is None:
            return None
        if event["state"].get("device") is None:
            return None
        return event["state"]["device"][k]

    def export_plays_as_csv(self, oot):
        with open(oot, "w+") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "Played At", "Spotify Track ID", "Track", "Spotify album ID", "Album",
                             "Spotify artist ID", "Artist", "Context"])
            for track in self.conn.execute("select * from play order by played_at asc").fetchall():
                writer.writerow(track)

    def commit(self):
        self.conn.commit()

    def track_ids(self):
        return list(map(lambda x: x[0], self.conn.execute("select track_id from track").fetchall()))

    def album_ids(self):
        return list(map(lambda x: x[0], self.conn.execute("select album_id from album").fetchall()))

    def get_first_record(self):
        res = self.conn.execute("select played_at from play order by played_at asc limit 1").fetchone()
        return dateutil.parser.parse(res[0])

    def play_from_name_and_artist(self, name, artist):
        return self.query_db("select * from play where track_name=? and main_artist_name=? limit 1",
                             (name, artist), True)

    def as_list(self, result):
        res = []
        for r in result.fetchall():
            res.append(r)

        return res

    def query_db(self, query, args=(), one=False):
        cur = self.conn.cursor()
        cur.execute(query, args)
        r = [dict((cur.description[i][0], value)
                  for i, value in enumerate(row)) for row in cur.fetchall()]
        return (r[0] if r else None) if one else r

    def get_db_version(self):
        res = self.as_list(self.conn.execute("select version from info order by version desc"))
        if len(res) == 0:
            self.set_db_version("1")
            return 1
        return res[0][0]

    def set_db_version(self, version):
        with Cursor(self) as cur:
            cur.execute("insert into info values (?)", (version,))

    # Add genre to db if it doesn't exist. Then return it's id
    def add_genre_if_not_exists(self, name) -> int:
        if not self.exists("genre", "name", name):
            with Cursor(self) as cur:
                cur.execute("insert into genre values (?, ?)", (None, name))

        with Cursor(self) as cur:
            return cur.execute("select id from genre where name = ?", (name,)).fetchone()[0]

    def add_genre_to_artist(self, artist_id, genre_name):
        genre_id = self.add_genre_if_not_exists(genre_name)

        with Cursor(self) as cur:
            # Check that (artist_id, genre_id) doesn't already exist
            duplicate_check = cur.execute("select count(*) from artist_genre where artist_id=? and genre_id=?", (artist_id, genre_id))
            existing_items_count = duplicate_check.fetchone()[0]
            if existing_items_count == 0:
                cur.execute("insert into artist_genre values (?, ?)", (artist_id, genre_id))

    def migrate_genre(self):
        logging.info("(dave attenborough in hushed tones) And so, the mass genre migration begins")
        no_genre = 0
        has_genre = 0
        for artist in self.query_db("select artist_id, genres from artist"):
            if artist["genres"] is None or len(artist["genres"].strip()) == 0:
                # too indie
                no_genre += 1
            else:
                genres = artist["genres"].strip()
                has_genre += 1
                for genre in genres.split(","):
                    genre = genre.strip()
                    assert genre != ""
                    self.add_genre_to_artist(artist["artist_id"], genre)

        logging.info("Migrated genres has_genre = {} no genre 'cause it's too indie' = {}".format(has_genre, no_genre))
        return True


class Cursor:

    def __init__(self, db: DbStore):
        self.db = db

    def __enter__(self):
        self.cursor = self.db.conn.cursor()
        return self.cursor

    def __exit__(self, a, b, c):
        self.cursor.close()
