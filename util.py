import datetime
import pymongo
import yaml
import os
import logging
import sys

CONFIG = None


def get_path(rel_path):
    return sys.path[0] + "/" + rel_path


def in_dev():
    return len(sys.argv) >= 2 and sys.argv[1] == "DEV"


# Compare to second accuracy (never need any better in this application)
def datetimes_equal(dt1: datetime.datetime, dt2: datetime.datetime) -> bool:
    return dt1.year == dt2.year \
           and dt1.month == dt2.month \
           and dt1.day == dt2.day \
           and dt1.hour == dt2.hour \
           and dt1.minute == dt2.minute \
           and dt1.second == dt2.second


def get_spotify_db():
    mongo_un_key = "SPOTIFY_MONGO_USERNAME"
    mongo_pass_key = "SPOTIFY_MONGO_PASSWORD"

    cfg = config()["db"]
    if mongo_un_key in os.environ and mongo_pass_key in os.environ:
        logging.debug("Getting db with username and password")
        return pymongo.MongoClient(
            "mongodb://{}:{}@{}:{}".format(os.environ[mongo_un_key], os.environ[mongo_pass_key], cfg["host"],
                                           cfg["port"])).spotify

    return pymongo.MongoClient(cfg["host"], cfg["port"]).spotify


def config():
    global CONFIG
    if CONFIG:
        return CONFIG

    possible_locations = [
        "" if "SPOTIFY_DOWNLOADER_CONFIG_PATH" not in os.environ else os.environ["SPOTIFY_DOWNLOADER_CONFIG_PATH"],
        "config.yml",
        get_path("config.yml"),
        "~/Spotify-history-downloader/config.yml",
        "~/IdeaProjects/Spotify-history-downloader/config.yml"
    ]

    logging.info("Looking for config file...")

    for location in possible_locations:
        path = os.path.expanduser(location)
        logging.info("Searching for config at " + path)

        if os.path.isfile(path):
            logging.info("Found at " + path)
            CONFIG = yaml.load(open(path))
            logging.info("Returning from config {}".format(CONFIG))

    if os.path.isfile(get_path("config-ben.yml")):
        secondary = yaml.load(open(get_path("config-ben.yml")))
        CONFIG = shite_merge_config(CONFIG, secondary)

    return CONFIG


def shite_merge_config(main, sec):
    for section in main:
        for k in main[section]:
            if section in sec and k in sec[section]:
                # Overwrite
                main[section][k] = sec[section][k]

    return main


def percent(a: int, b: int) -> int:
    return int((a / b) * 100)


def tracks_within_dates(db, start: datetime.datetime, end: datetime.datetime):
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

    return sorted(freq.items(), key=lambda x: x[1], reverse=reverse)


def track_to_string(track) -> str:
    artist = "<UNKNOWN>" if len(track["track"]["artists"]) == 0 else track["track"]["artists"][0]["name"]
    return "[{}] {} - {}".format(track["track"]["id"], artist, track["track"]["name"])
