import csv
from spotify import *
import util
from upload.upload import run_export
from db.db import DbStore


class DownloadException(Exception):
    pass


UPDATE_SLEEP_MS = 50


def insert(tracks):
    db = DbStore()

    # Get last track listened to stored in db
    # This is to ensure we don't duplicate items in database
    latest_track_time = db.most_recent_played_at()
    logging.info("Retrieved tracks from spotify, filtering out ones played up to {}".format(latest_track_time))
    if latest_track_time:
        tracks = remove_tracks_before_inc(tracks, latest_track_time)
        logging.info("Inserting {} tracks".format(len(tracks)))
    else:
        logging.info("Nothing played since last download, doing nothing...")

    for track in tracks:
        db.add_play(track)


def remove_tracks_before_inc(tracks, stop_at_time):
    new = []
    for track in tracks:
        if track["played_at"] == stop_at_time:
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


def update_tracks(db: DbStore, creds: Credentials):
    track_ids = db.incomplete_track_ids()
    full_tracks = get_tracks(track_ids, creds, UPDATE_SLEEP_MS)
    features = get_track_features(track_ids, creds, UPDATE_SLEEP_MS)
    logging.info("Found {} tracks to update".format(len(track_ids)))

    for i, track in enumerate(full_tracks):
        feature = features[i]
        db.update_full_track(track["id"], track["track_number"], track["disc_number"], feature["valence"],
                             feature["tempo"],
                             feature["danceability"], feature["energy"], feature["instrumentalness"],
                             feature["speechiness"]
                             , feature["time_signature"], feature["loudness"], feature["liveness"],
                             feature["acousticness"])


def update_artists(db: DbStore, creds: Credentials):
    artist_ids = db.incomplete_artist_ids()
    full_artists = get_artists(artist_ids, creds, UPDATE_SLEEP_MS)
    for artist in full_artists:
        db.update_full_artist(artist["id"], artist["popularity"], artist["followers"]["total"], artist["genres"])


def update_albums(db: DbStore, creds: Credentials):
    album_ids = db.incomplete_album_ids()
    full_albums = get_albums(album_ids, creds, UPDATE_SLEEP_MS)
    for album in full_albums:
        db.update_full_album(album["id"], album["release_date_precision"], album["release_date"], album["type"],
                             album["images"][0]["url"], album["images"][1]["url"], album["images"][2]["url"],
                             album["label"], album["popularity"], album["genres"])


def import_from_mongo():
    db = DbStore()

    i = 0
    for track in util.get_spotify_db().tracks.find():
        db.add_play_from_mongo(track)
        i += 1

        if i % 100 == 0:
            print("Added {}".format(i))


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

    db = DbStore()

    creds = get_credentials()
    logging.info("updating tracks")
    update_tracks(db, creds)
    update_artists(db, creds)
    update_albums(db, creds)

    # Update events
    # analysis.gen_events.refresh_events(util.get_spotify_db())
    # TODO fix this

    # Update sounds goodpy
    # Disabled as I'm not using this right now
    # scripts.sounds_good.run_sounds_good()

    # Backup
    export_album_art()
    run_export()


if __name__ == "__main__": main()
