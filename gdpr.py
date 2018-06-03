import pymongo
import datetime
import json 
from spotify import Credentials, get_credentials, search, get_tracks
import sys
import pickle

EXPORT_PATH = "download/StreamingHistory.json"

def parse_file(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
        data =  json.loads(text)
        for song in data:
            time_parts = song["time"].split(" ")
            iso_date = "{}T{}Z".format(time_parts[0], time_parts[1])
            song["time"] = datetime.datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ")
        return data

def remove_recent(data):
    l = []
    #2018-05-11 15:40:38
    dt = datetime.datetime(2018, 5, 11, 15, 40, 38)
    for d in data:
        if d["time"] < dt:
            l.append(d)
    return l

# To be consistant with recently played API only keep songs played > 30 s
def remove_short_plays(data):
    filtered = []
    limit = datetime.timedelta(0, 30)

    for i in range(len(data) - 1):
        dt = data[i + 1]["time"] - data[i]["time"]
        if dt > limit:
            filtered.append(data[i])

    return filtered


def get_track_id(spotify, creds, trackName, artistName):
    track = spotify.full_tracks.find_one({"name": trackName})
    if track:
        # Check artist
        for artist in track["artists"]:
            if artist["name"] == artistName:
                return track["id"]

    # No luck, spotify search instead to get id
    query = "track:{} artist:{}".format(trackName, artistName)
    result = search(query, "track", "tracks", creds)
    for track in result:
        if track["name"] == trackName:
            for artist in track["artists"]:
                if artist["name"] == artistName:
                    return track["id"]

    return None
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def load(path):
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
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

def add_track_ids():
    data = remove_recent(parse_file(EXPORT_PATH))
    data = remove_short_plays(data)

    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
    creds = get_credentials()
    count = len(data)
    failures = []

    cache = {}
    for i, item in enumerate(data):
        main_artist = item["artistName"].split(",")[0]
        cache_key = "{}:{}".format(item["trackName"], item["artistName"])
        print("[{}%] ".format(int(100 * i / count)), end="")

        if cache_key in cache:
            print("Cache hit! ", end="")
            t_id = cache[cache_key]
        else:
            t_id = get_track_id(spotify, creds, item["trackName"], main_artist)

        if t_id:
            print("got {} by {} with id {}".format(item["trackName"], item["artistName"], t_id))
            cache[cache_key] = t_id
            item["trackId"] = t_id
        else: 
            failures.append(item)

    print(failures)

    with open("import.json", "w+", encoding="utf-8") as f:
        f.write(json.dumps(data, default=json_serial))


def main():
    load("import.json")
    
if __name__=="__main__":main()