import pymongo
import read
from spotify import *
import dateutil.parser
import util

class DownloadException(Exception):
    pass


def insert(tracks):
    spotify = util.get_spotify_db()

    # Get last track listened to stored in db
    # This is to ensure we don't duplicate items in database
    latest_track = spotify.tracks.find_one({}, sort=[("played_at", pymongo.DESCENDING)])
    logging.info("Retrieved tracks from spotify, filtering out ones played up to {}".format(latest_track["played_at"]))

    # Properly parse dates
    for track in tracks:
        track["played_at"] = dateutil.parser.parse(track["played_at"])

    if latest_track:
        tracks = remove_tracks_before_inc(tracks, latest_track)
        logging.info("Inserting {} tracks".format(len(tracks)))
    else:
        logging.info("Nothing played since last download, doing nothing...")

    if len(tracks) > 0:
        spotify.tracks.insert_many(tracks)


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


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='output.log')
    logging.getLogger().addHandler(logging.StreamHandler())

    # Disable logging we don't need
    # O/W we end up with GBs of logs in just 24 hours
    # (mainly thanks to player state requests, of which there are thousands of)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    creds = get_credentials()
    j = get_recently_played(creds)
    insert(j["items"])

    # Update stuff
    read.update()


if __name__ == "__main__": main()
