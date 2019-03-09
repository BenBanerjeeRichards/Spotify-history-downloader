from spotify import *
import util
from upload.upload import run_export, write_basic_track_file
from db.db import DbStore
from analysis import gen_events


class DownloadException(Exception):
    pass


UPDATE_SLEEP_MS = 50


def insert(tracks, db: DbStore):
    logging.info("Got db instance")

    # Get last track listened to stored in db
    # This is to ensure we don't duplicate items in database
    latest_track_time = db.most_recent_played_at()
    logging.info("Retrieved tracks from spotify, filtering out ones played up to {}".format(latest_track_time))
    if latest_track_time:
        tracks = remove_tracks_before_inc(tracks, latest_track_time)
        logging.info("Inserting {} tracks".format(len(tracks)))
    else:
        logging.info("Nothing played since last download, doing nothing...")

    logging.info("Would insert {} tracks".format(len(tracks)))

    for track in tracks:
        logging.info("Adding track {}".format(track["track"]["name"]))
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


def import_context_from_mongo():
    db = DbStore()
    for i, track in enumerate(util.get_spotify_db().tracks.find()):
        dt = track["played_at"].isoformat()
        context = track.get("context")
        if context is not None and "uri" in context:
            db.add_context(dt, context["uri"])

        if i % 100 == 0:
            print("Added {}".format(i))

    db.conn.commit()


def do_main():
    log_path = util.get_path("spotify-downloader.log")
    print("Log path = {}".format(log_path))

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename=log_path)

    # Disable logging we don't need
    # O/W we end up with GBs of logs in just 24 hours
    # (mainly thanks to player state requests, of which there are thousands of)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    # logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.DEBUG)
    db = DbStore()

    logging.info("Getting recently played tracks")
    creds = get_credentials()
    j = get_recently_played(creds)
    logging.info("Got {} tracks".format(len(j["items"])))
    insert(j["items"], db)

    logging.info("Updating tracks")
    update_tracks(db, creds)

    logging.info("Updating artists")
    update_artists(db, creds)

    logging.info("Updating albums")
    update_albums(db, creds)

    # Update events
    gen_events.refresh_events(util.get_spotify_db())

    # Backup
    run_export()
    logging.info("Done!")


def main():
    try:
        do_main()
    except Exception as e:
        logging.exception("An exception occured:")
        raise e


if __name__ == "__main__": main()
