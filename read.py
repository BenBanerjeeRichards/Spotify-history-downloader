import csv

import pymongo
from spotify import *
import sys
import util
import db.player_store

def basic():
    spotify = util.get_spotify_db()

    tracks = spotify.tracks.find({}, sort=[("played_at", pymongo.DESCENDING)])
    for track in tracks:
        print("[{}] {} - {}".format(track["played_at"], track["track"]["artists"][0]["name"], track["track"]["name"]))


# Download full album data if it doesn't already exist in database
def update_albums(creds, spotify):
    ids_to_get = []

    tracks = spotify.tracks.find({})
    album_ids = spotify.albums.distinct("id")
    for track in tracks:
        a_id = track["track"]["album"]["id"]
        if a_id not in album_ids:
            ids_to_get.append(a_id)

    logging.info("Found {} album ids to download".format(len(ids_to_get)))
    album_data = get_albums(ids_to_get, creds)
    if len(album_data) > 0:
        spotify.albums.insert_many(album_data)


# Download full artist data
def update_artists(creds, spotify):
    ids_to_get = []

    tracks = spotify.tracks.find({})
    artist_ids = spotify.artists.distinct("id")

    for track in tracks:
        # Remember we adopt convention of not caring about second, third.. artists
        if len(track["track"]["artists"]) == 0:
            continue
        a_id = track["track"]["artists"][0]["id"]
        if a_id not in artist_ids:
            ids_to_get.append(a_id)

    logging.info("Found {} artist ids to download".format(len(ids_to_get)))
    artist_data = get_artists(ids_to_get, creds)
    if len(artist_data) > 0:
        spotify.artists.insert_many(artist_data)


def update_features(creds, spotify):
    ids_to_get = []

    tracks = spotify.tracks.find({})
    feature_ids = spotify.features.distinct("id")
    for track in tracks:
        track_id = track["track"]["id"]
        if track_id not in feature_ids:
            ids_to_get.append(track_id)

    logging.info("Found {} features ids to download".format(len(ids_to_get)))
    feat_data = get_track_features(ids_to_get, creds)

    def notNone(x):
        return x is not None

    feat_data = list(filter(notNone, feat_data))

    if len(feat_data) > 0:
        spotify.features.insert_many(feat_data)


# Tracks from player and tracks we don't have full track info
# about in full_tracks
def get_unknown_track_ids():
    spotify = util.get_spotify_db()

    # Ids from player database
    # If never played for > 30 secs then doesn't exist in tracks collection
    player_ids = db.player_store.store().player_distinct_track_ids()
    track_ids = spotify.tracks.find().distinct("track.id")

    # Combine and remove duplicates
    all_ids = list(set(player_ids + track_ids))

    # See what we have in db
    full_ids = spotify.full_tracks.find().distinct("id")

    return [item for item in all_ids if item not in full_ids]


def update_full_tracks(creds=None):
    if creds is None:
        creds = get_credentials()

    spotify = util.get_spotify_db()

    ids = get_unknown_track_ids()
    if len(ids) > 0:
        logging.info("[TRACK UPDATE] found {} tracks to update".format(len(ids)))
        tracks = get_tracks(ids, creds)
        spotify.full_tracks.insert_many(tracks)
        logging.info("[TRACK UPDATE] done inserting tracks")
    else:
        logging.info("[TRACK UPDATE] no tracks to update")

    return len(ids)


def track_csv(out_name):
    spotify = util.get_spotify_db()

    tracks = spotify.tracks.find({}, sort=[("played_at", pymongo.DESCENDING)])
    with open(out_name, 'w', newline='', encoding='utf-8') as f:
        header = ["played_at", "track_id", "name", "duration_ms", "danceability", "energy", "key", "loudness", "mode",
                  "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature",
                  "track_popularity", "album_id", "artist_popularity", "artist", "genres"]
        writer = csv.writer(f)
        writer.writerow(header)

        empty_feat = {}
        empty_feat["danceability"] = ""
        empty_feat["energy"] = ""
        empty_feat["key"] = ""
        empty_feat["loudness"] = ""
        empty_feat["mode"] = ""
        empty_feat["speechiness"] = ""
        empty_feat["acousticness"] = ""
        empty_feat["instrumentalness"] = ""
        empty_feat["liveness"] = ""
        empty_feat["valence"] = ""
        empty_feat["tempo"] = ""
        empty_feat["time_signature"] = ""

        for track in tracks:
            # Get artist data
            artist = spotify.artists.find_one({"id": track["track"]["artists"][0]["id"]})
            feat = spotify.features.find_one({"id": track["track"]["id"]})
            if feat is None:
                logging.info("NO FEATURE FOR {}".format(track["track"]["id"]))
                feat = empty_feat

            data = [
                str(track["played_at"]),
                track["track"]["id"],
                track["track"]["name"],
                track["track"]["duration_ms"],
                feat["danceability"],
                feat["energy"],
                feat["key"],
                feat["loudness"],
                feat["mode"],
                feat["speechiness"],
                feat["acousticness"],
                feat["instrumentalness"],
                feat["liveness"],
                feat["valence"],
                feat["tempo"],
                feat["time_signature"],
                track["track"]["popularity"],
                track["track"]["album"]["id"],
                artist["popularity"],
                artist["name"],
                ",".join(artist["genres"])
            ]

            writer.writerow(data)


def print_recent():
    spotify = util.get_spotify_db()

    tracks = spotify.tracks.find({}, sort=[("played_at", pymongo.DESCENDING)])
    for i, track in enumerate(tracks):
        print("[{}]: {} - {}".format(
            track["played_at"], track["track"]["artists"][0]["name"], track["track"]["name"]))
        if i == 9:
            return


def update():
    spotify = util.get_spotify_db()
    creds = get_credentials()

    logging.info("Updating features...")
    update_features(creds, spotify)

    logging.info("Updating atists...")
    update_artists(creds, spotify)

    logging.info("Updating albums...")
    update_albums(creds, spotify)


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='output.log')
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    update()

    if len(sys.argv) > 1:
        if sys.argv[1] == "recent":
            print_recent()
        if sys.argv[1] == "refresh":
            if len(sys.argv) > 2 and sys.argv[2] == "--dry-run":
                print("Would download {} new tracks".format(len(get_unknown_track_ids())))
            else:
                n = update_full_tracks()
                print("downloaded {} tracks".format(n))
    else:
        track_csv("out.csv")


if __name__ == "__main__": main()
