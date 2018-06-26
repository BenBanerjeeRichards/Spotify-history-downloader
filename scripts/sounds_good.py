import datetime
import logging
import spotify
import dateutil.parser
import util

CONFIG = util.config()["sounds_good"]
PLAYLIST_ID = CONFIG["playlist_id"]
MOVE_AFTER = datetime.timedelta(days=CONFIG["move_after_days"])
MOVE_TO_PLAYLIST = CONFIG["move_to_playlist"]
USER_ID = CONFIG["user_id"]


def get_track_ids(items):
    return list(map(lambda x: x["track"]["id"], items))


def split_saved_unsaved(tracks, saved_ids):
    saved = []
    unsaved = []

    for track in tracks:
        if track["track"]["id"] in saved_ids:
            saved.append(track)
        else:
            unsaved.append(track)

    return saved, unsaved


def run_sounds_good():
    creds = spotify.get_credentials()
    sounds_good_tracks = spotify.get_playlist_basic(USER_ID, PLAYLIST_ID, creds)
    saved_songs = spotify.get_saved_tracks(creds)
    logging.info("Got {} tracks from sounds good, {} tracks from saved songs" \
                 .format(len(sounds_good_tracks), len(saved_songs)))

    saved_ids = get_track_ids(saved_songs)

    saved, unsaved = split_saved_unsaved(sounds_good_tracks, saved_ids)
    logging.info("Sounds good: saved = {}, unsaved = {}".format(len(saved), len(unsaved)))

    add_to_archive = []
    remove_from_sounds_good = []

    for track in saved:
        logging.info("Track {}({}) is saved, removing from sounds good" \
                     .format(track["track"]["name"], track["track"]["id"]))
        remove_from_sounds_good.append(track["track"]["id"])

    today = datetime.datetime.now(datetime.timezone.utc)

    for track in unsaved:
        t_id = track["track"]["id"]
        date_added = dateutil.parser.parse(track["added_at"])
        delta = today - date_added

        if delta < MOVE_AFTER:
            logging.info("Track {}({}) is unsaved, stays in playlist due to Delta = {} < {}" \
                         .format(track["track"]["name"], t_id, delta, MOVE_AFTER))
        else:
            logging.info("Track {}({}) is unsaved, removed from playlist due to Delta = {} >= {}" \
                         .format(track["track"]["name"], t_id, delta, MOVE_AFTER))

            add_to_archive.append(t_id)
            remove_from_sounds_good.append(t_id)

    logging.info("Adding to archive playlist: {}".format(",".join(add_to_archive)))
    spotify.add_to_playlist(USER_ID, MOVE_TO_PLAYLIST, add_to_archive, creds)

    logging.info("Removing from sounds good: {}".format(",".join(remove_from_sounds_good)))
    spotify.remove_from_playlist(USER_ID, PLAYLIST_ID, remove_from_sounds_good, creds)

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='output.log')
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    run_sounds_good()


if __name__ == "__main__": main();
