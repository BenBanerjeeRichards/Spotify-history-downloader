import datetime
import pymongo
import yaml
import os
import logging

CONFIG = None


# Compare to second accuracy (never need any better in this application)
def datetimes_equal(dt1: datetime.datetime, dt2: datetime.datetime) -> bool:
    return dt1.year == dt2.year \
           and dt1.month == dt2.month \
           and dt1.day == dt2.day \
           and dt1.hour == dt2.hour \
           and dt1.minute == dt2.minute \
           and dt1.second == dt2.second


def get_spotify_db():
    cfg = config()["db"]

    client = pymongo.MongoClient(cfg["host"], cfg["port"])  # Same in prod
    return client.spotify


def config():
    global CONFIG
    if CONFIG:
        return CONFIG

    possible_locations = [
        "" if "SPOTIFY_DOWNLOADER_CONFIG_PATH" not in os.environ else os.environ["SPOTIFY_DOWNLOADER_CONFIG_PATH"],
        "config.yml",
        "spotify_downloader_config.yml",
        "~/spotify_downloader_config.yml",
        "~/Spotify-history-downloader/config.yml",
        "~/Spotify-history-downloader/spotify_downloader_config.yml",
        "~/IdeaProjects/Spotify-history-downloader/config.yml"
    ]

    logging.info("Looking for config file...")

    for location in possible_locations:
        path = os.path.expanduser(location)
        logging.info("Checking " + path)

        if os.path.isfile(path):
            logging.info("Found at " + path)
            CONFIG = yaml.load(open(path))
            break

    if CONFIG is None:
        logging.error("No config file found, checked " + str(possible_locations))

    return CONFIG


def percent(a: int, b: int) -> int:
    return int((a / b) * 100)


def tracks_within_dates(db, start:datetime.datetime, end:datetime.datetime):
    if start and end:
        return db.tracks.find({
            "played_at": {
                "$lt": end,
                "$gt": start
            }
        })

    if start and not end:
        return db.tracks.find({
            "played_at": {
                "$gt": start
            }
        })

    if end and not start:
        return db.tracks.find({
            "played_at": {
                "$lt": end,
            }
        })

    return []


def track_frequency(tracks: [object], reverse=True) -> [(str, int)]:
    freq = {}
    for track in tracks:
        t_id = track["track"]["id"]
        if t_id in freq:
            freq[t_id] += 1
        else:
            freq[t_id] = 1

    return sorted(freq.items(), key=lambda x:x[1], reverse=reverse)


