import pymongo
import datetime
import pytz
from analysis.FreqDict import FreqDict
import util


def get_tracks(date, timezone=None):
    if timezone is None:
        timezone = pytz.timezone("GMT")

    start = datetime.datetime(date.year, date.month, date.day, 0, 0, 0, 0)
    end = datetime.datetime(date.year, date.month, date.day, 23, 59, 59, 0)
    start = timezone.localize(start)
    end = timezone.localize(end)
    client = pymongo.MongoClient("localhost", 27017)
    spotify = util.get_spotify_db()

    query = {
        "played_at": {
            "$gt": start,
            "$lt": end
        }
    }
    return spotify.tracks.find(query)


def simple(track):
    return {
        "name": track["track"]["name"],
        "artist": track["track"]["artists"][0]["name"],
        "played_at": track["played_at"],
        "full": track
    }


def track_frequency(tracks):
    freq = FreqDict()
    for t in tracks:
        key = "{}:{}".format(t["name"], t["artist"])
        freq.add(key)

    return freq.sort()


def limit_top_tracks(tracks, limit=5):
    # Return top 5 (or less)
    top_tracks = []
    for track in tracks:
        if track[1] == 1:
            break

        if len(top_tracks) >= 5:
            break

        parts = track[0].split(":")
        if len(parts) != 2:
            continue

        top_tracks.append({
            "artist": parts[1],
            "name": parts[0],
            "plays": track[1]
        })

    return top_tracks


def time_distribution(tracks):
    freq = FreqDict()
    for track in tracks:
        freq.add(track["played_at"].hour)

    d = freq.d
    arr = []
    for i in range(0, 24):
        if i in d:
            arr.append(d[i])
        else:
            arr.append(0)

    return arr


def get_stats(date, timezone=None):
    tracks = list(map(lambda x: simple(x), get_tracks(date, timezone)))
    top_tracks = limit_top_tracks(track_frequency(tracks))
    return {
        "count": len(tracks),
        "top_tracks": top_tracks,
        "time_dist": time_distribution(tracks)
    }


