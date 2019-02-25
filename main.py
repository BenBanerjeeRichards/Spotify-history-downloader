import pymongo
import read
import csv
from spotify import *
import dateutil.parser
import util
import analysis.gen_events
import sys
import scripts.sounds_good
from upload.upload import run_export

class DownloadException(Exception):
    pass


def insert(tracks):
    spotify = util.get_spotify_db()

    # Get last track listened to stored in db
    # This is to ensure we don't duplicate items in database
    latest_track = spotify.tracks.find_one({}, sort=[("played_at", pymongo.DESCENDING)])
    logging.info("Retrieved tracks from spotify, filtering out ones played up to {}".format(latest_track["played_at"]))

    # Properly parse dates and remove stuff we don't care about
    for track in tracks:
        track["played_at"] = dateutil.parser.parse(track["played_at"])
        track["track"] = clean_track(track["track"])

    if latest_track:
        tracks = remove_tracks_before_inc(tracks, latest_track)
        logging.info("Inserting {} tracks".format(len(tracks)))
    else:
        logging.info("Nothing played since last download, doing nothing...")

    if len(tracks) > 0:
        spotify.tracks.insert_many(tracks)


def clean_artist(old_artist):
    return {"name": old_artist["name"], "id": old_artist["id"]}


# Remove items from spotify track object we don't care about
# Saves space, esp. with file export
def clean_track(old_track):
    track = {"id": old_track["id"], "popularity": old_track["popularity"], "duration_ms": old_track["duration_ms"],
             "name": old_track["name"], "external_ids": old_track["external_ids"], "explicit": old_track["explicit"]}

    artists = []
    for old_artist in old_track["artists"]:
        artists.append(clean_artist(old_artist))

    track["artists"] = artists
    album_artists = []
    for old_artist in old_track["album"]["artists"]:
        album_artists.append(clean_artist(old_artist))

    track["album"] = {
        "name": old_track["album"]["name"],
        "id": old_track["album"]["id"],
        "artists": album_artists
    }

    return track


def remove_tracks_before_inc(tracks, stop_at_track):
    new = []
    for track in tracks:
        if util.datetimes_equal(track["played_at"], stop_at_track["played_at"]):
            logging.info("Found repeat track, stopping: {}".format(track["played_at"]))

            break
        new.append(track)

    logging.info("Found all new tracks. Initial = {}, New = {}, Filterd = {}"
                 .format(len(tracks), len(new), len(tracks) - len(new)))

    return new


def pretty_recently_played_json(tracks):
    s = ""
    for item in tracks:
        s += "{} - {}\n".format(item["track"]["artists"][0]["name"], item["track"]["name"])
    return s


# Export album art to csv file (link album id -> largest album art link)
def export_album_art():
    db = util.get_spotify_db()
    mappings_large = []
    mappings_med = []
    mappings_small = []
    for album in db.albums.find({}):
        images = album["images"]
        if images is not None and len(images) == 3:
            mappings_large.append((album["id"], images[0]["url"]))
            mappings_med.append((album["id"], images[1]["url"]))
            mappings_small.append((album["id"], images[2]["url"]))
        else:
            logging.info("No album art found for album {}".format(album["name"]))

    dir = util.config()["upload"]["cwd"]

    with open("{}/art.csv".format(dir), "w+") as f:
        writer = csv.writer(f)
        writer.writerows(mappings_large)
    with open("{}/art-medium.csv".format(dir), "w+") as f:
        writer = csv.writer(f)
        writer.writerows(mappings_med)
    with open("{}/art-small.csv".format(dir), "w+") as f:
        writer = csv.writer(f)
        writer.writerows(mappings_small)


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='spotify-downloader.log')

    # Disable logging we don't need
    # O/W we end up with GBs of logs in just 24 hours
    # (mainly thanks to player state requests, of which there are thousands of)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.DEBUG)

    creds = get_credentials()
    j = get_recently_played(creds)
    insert(j["items"])

    # Update stuff
    read.update()

    # Update events
    analysis.gen_events.refresh_events(util.get_spotify_db())

    # Update sounds goodpy
    # Disabled as I'm not using this right now
    # scripts.sounds_good.run_sounds_good()

    # Backup
    export_album_art()
    run_export()


if __name__ == "__main__": main()
