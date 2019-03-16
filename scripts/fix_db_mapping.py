from db.db import DbStore
from spotify import *
import logging

UPDATE_SLEEP_MS = 50


def fix_tracks(creds: Credentials, db: DbStore):
    track_ids = db.track_ids()
    full_tracks = get_tracks(track_ids, creds, UPDATE_SLEEP_MS)

    logging.info("Downloaded all tracks, updating db...")
    for track in full_tracks:
        for artist in track["artists"]:
            db.add_track_artist(track["id"], artist["id"])

        for artist in track["album"]["artists"]:
            db.add_album_artist(track["album"]["id"], artist["id"])

    db.commit()
    logging.info("Fixed all tracks")

