import pymongo
import datetime
import json
import logging
from db.db import DbStore
from spotify import Credentials, get_credentials, search, get_tracks
import util

CONFIG = util.config()["gdpr"]
EXPORT_PATH = CONFIG["export_path"]


def parse_file(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
        data = json.loads(text)
        for song in data:
            time_parts = song["time"].split(" ")
            iso_date = "{}T{}Z".format(time_parts[0], time_parts[1])
            song["time"] = datetime.datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ")
        return data


def remove_recent(data, date: datetime.datetime):
    plays_to_add = []
    for d in data:
        if d["time"] < date:
            plays_to_add.append(d)
    return plays_to_add


# To be consistant with recently played API only keep songs played > 30 s
def remove_short_plays(data):
    filtered = []
    limit = datetime.timedelta(0, 30)

    for i in range(len(data) - 1):
        dt = data[i + 1]["time"] - data[i]["time"]
        if dt > limit:
            filtered.append(data[i])

    return filtered


def get_track_play(db: DbStore, creds, track_name, artist_name):
    # Check local database first for id
    local_db_play = db.play_from_name_and_artist(track_name, artist_name)
    if local_db_play is not None:
        local_db_play["played_at"] = None
        local_db_play["context"] = None
        print("FROM LOCAL DB {} {}".format(track_name, artist_name))
        return local_db_play

    # No luck, spotify search instead to get id
    query = "track:{} artist:{}".format(track_name, artist_name)
    result = search(query, "track", "tracks", creds)
    for track in result:
        if track["name"] == track_name:
            for artist in track["artists"]:
                if artist["name"] == artist_name:
                    print("FROM SPOTIFY {} {}".format(track_name, artist_name))
                    return {
                        "track": track,
                        "played_at": None,  #  Fill this out later
                        "context": None  #  No context in GDPR
                    }

    return None


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def load(path):
    spotify = util.get_spotify_db()
    ids = []
    data = None

    with open(path) as f:
        data = json.loads(f.read())
        ids = []
        creds = get_credentials()

        for track in data:
            if not "trackId" in track:
                continue

            ids.append(track["trackId"])

        ids = list(set(ids))

    full_tracks = get_tracks(ids, creds)
    track_by_id = {}
    for f_track in full_tracks:
        track_by_id[f_track["id"]] = f_track
    print("GOT {} tracks".format(len(full_tracks)))

    states = []
    for track in data:
        if "trackId" not in track:
            continue
        state = {
            "played_at": track["time"],
            "track": track_by_id[track["trackId"]]
        }

        states.append(state)

    spotify.tracks.insert_many(states)


def add_track_ids(db: DbStore, export_path=EXPORT_PATH):
    data = parse_file(export_path)
    print("Loaded {} plays fro GDPR. Removing short plays (<30s)...".format(len(data)))
    data = remove_short_plays(data)
    print("Now got {} plays. Removing tracks already in database...".format(len(data)))
    # FIXME
    # data = remove_recent(data, db.get_first_record())
    print("Got {} plays to insert into database".format(len(data)))

    creds = get_credentials()
    count = len(data)
    failures = []
    plays = []
    cache = {}
    for i, item in enumerate(data):
        main_artist = item["artistName"].split(",")[0]
        cache_key = "{}:{}".format(item["trackName"], item["artistName"])
        print("[{}%] ".format(int(100 * i / count)), end="")

        if cache_key in cache:
            print("Cache hit! ", end="")
            play = cache[cache_key]
            # shift into format for play
        else:
            play = get_track_play(db, creds, item["trackName"], main_artist)
            play = {
                "track": {
                    "duration_ms": None,
                    "popularity": None,
                    "name": play["track_name"],
                    "id": play["track_id"],
                    "album": {
                        "name": play["album_name"],
                        "id": play["album_id"]
                    },
                    "artists": [
                        {
                            "name": play["main_artist_name"],
                            "id": play["main_artist_id"],
                        }
                    ]
                }
            }

        # strip().replace(" ", "T") + "Z"

        if play:
            play["played_at"] = item["time"].isoformat()

            print("got {} by {} with id {}".format(item["trackName"], item["artistName"], play))
            cache[cache_key] = play
            db.add_play(play)
        else:
            failures.append(item)

    print("FAIL FAIL FAIL")
    print(failures)

    with open("import.json", "w+", encoding="utf-8") as f:
        f.write(json.dumps(plays, default=json_serial))


def main():
    db = DbStore()
    print(add_track_ids(db, "StreamingHistory.json"))


if __name__ == "__main__": main()
