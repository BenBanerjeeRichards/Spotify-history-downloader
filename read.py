import csv

import pymongo
from spotify import *
import sys

def basic():
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    tracks = spotify.tracks.find({}, sort=[("played_at", pymongo.DESCENDING)])
    for track in tracks:
        print("[{}] {} - {}".format(track["played_at"], track["track"]["artists"][0]["name"], track["track"]["name"]))


# Download full album data if it doesn't already exist in database
def update_albums(creds=None):
    if creds is None:
        creds = get_credentials()

    ids_to_get = []
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    tracks = spotify.tracks.find({})
    for track in tracks:
        a_id = track["track"]["album"]["id"]
        album = spotify.albums.find_one({"id": a_id})
        if not album:
            ids_to_get.append(a_id)

    logging.info("Found {} album ids to download".format(len(ids_to_get)))
    album_data = get_albums(ids_to_get, creds)
    if len(album_data) > 0:
        spotify.albums.insert_many(album_data)


# Download full artist data
def update_artists(creds=None):
    if creds is None:
        creds = get_credentials()

    ids_to_get = []
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    tracks = spotify.tracks.find({})
    for track in tracks:
        # Remember we adopt convention of not caring about second, third.. artists
        if len(track["track"]["artists"]) == 0:
            continue
        a_id = track["track"]["artists"][0]["id"]
        artist = spotify.artists.find_one({"id": a_id})
        if not artist:
            ids_to_get.append(a_id)

    logging.info("Found {} artist ids to download".format(len(ids_to_get)))
    artist_data = get_artists(ids_to_get, creds)
    if len(artist_data) > 0:
        spotify.artists.insert_many(artist_data)


def update_features(creds=None):
    if creds is None:
        creds = get_credentials()

    ids_to_get = []
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    tracks = spotify.tracks.find({})
    for track in tracks:
        track_id = track["track"]["id"]
        feature = spotify.features.find_one({"id": track_id})
        if not feature:
            ids_to_get.append(track_id)

    logging.info("Found {} features ids to download".format(len(ids_to_get)))
    feat_data = get_track_features(ids_to_get, creds)
    if len(feat_data) > 0:
        spotify.features.insert_many(feat_data)


def track_csv(out_name):
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    tracks = spotify.tracks.find({}, sort=[("played_at", pymongo.DESCENDING)])
    with open(out_name, 'w', newline='', encoding='utf-8') as f:
        header = ["played_at", "track_id", "name", "duration_ms", "danceability", "energy", "key", "loudness", "mode",
                  "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature",
                  "track_popularity", "album_id", "artist_popularity", "artist", "genres"]
        writer = csv.writer(f)
        writer.writerow(header)

        for track in tracks:
            # Get artist data
            artist = spotify.artists.find_one({"id": track["track"]["artists"][0]["id"]})
            feat = spotify.features.find_one({"id": track["track"]["id"]})
            data = [
                track["played_at"],
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


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='output.log')

    update_features()

    if len(sys.argv) > 1:
        if sys.argv[1] == "recent":
            client = pymongo.MongoClient("localhost", 27017)
            spotify = client.spotify

            tracks = spotify.tracks.find({}, sort=[("played_at", pymongo.DESCENDING)])
            for i, track in enumerate(tracks):
                print("[{}]: {} - {}".format(
                    track["played_at"], track["track"]["artists"][0]["name"], track["track"]["name"]))
                if i == 9:
                    return
    else:
        track_csv("out.csv")


if __name__ == "__main__": main()
