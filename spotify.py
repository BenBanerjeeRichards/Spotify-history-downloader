import logging
import os
import time

import requests

class Credentials:
    def __init__(self, client_id, client_secret, refresh):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh
        self.access_token = None

    def __str__(self):
        return "client_id: {}\nclient_secret:{}\nrefresh_token:{}".format(self.client_id, self.client_secret,
                                                                          self.refresh_token)


def get_credentials():
    # Remember that environment variables MUST BE SPECIFIED IN CRON TAB FILE
    creds = Credentials(os.environ["SPOTIFY_CLIENT_ID"], os.environ["SPOTIFY_CLIENT_SECRET"],
                        os.environ["SPOTIFY_REFRESH_TOKEN"])
    get_access_token(creds)
    return creds


def get_access_token(credentials, attempts=0):
    if attempts > 10:
        logging.error("Failed to get access token after 10 attempts")

    form_data = {
        "grant_type": "refresh_token",
        "refresh_token": credentials.refresh_token,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret
    }

    try:
        response = requests.post("https://accounts.spotify.com/api/token", data=form_data)
        response.raise_for_status()
    except Exception as e:
        logging.exception("Failed to get access token, retrying")
        time.sleep(pow(attempts, 1.5))
        return get_access_token(credentials, attempts + 1)

    logging.info("Got spotify access token")
    token = response.json()["access_token"]
    credentials.access_token = token


def get_recently_played(creds):
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    res = requests.get("https://api.spotify.com/v1/me/player/recently-played", params={"limit": 50}, headers=head)
    return res.json()


def chunks(l, n):
    n = max(1, n)
    return (l[i:i + n] for i in range(0, len(l), n))


def spotify_multiple_req(url, ids, creds, extract_list, max_ids=20):
    data = []
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    ids = list(set(ids))  # don't make duplicate requests

    for id_list in chunks(ids, max_ids):
        res = requests.get(url, params={"ids": ",".join(id_list)}, headers=head)
        new_albums = extract_list(res.json())
        data = data + new_albums
        logging.info("Got {}".format(len(new_albums)))

    return data


def get_albums(ids, creds):
    def extract(j):
        return j["albums"]

    return spotify_multiple_req("https://api.spotify.com/v1/albums", ids, creds, extract)


def get_artists(ids, creds):
    def extract(j):
        return j["artists"]

    return spotify_multiple_req("https://api.spotify.com/v1/artists", ids, creds, extract)


def get_track_features(ids, creds):
    def extract(j):
        return j["audio_features"]

    return spotify_multiple_req("https://api.spotify.com/v1/audio-features", ids, creds, extract, 100)


def get_tracks(ids, creds):
    def extract(j):
        return j["tracks"]

    return spotify_multiple_req("https://api.spotify.com/v1/tracks", ids, creds, extract, 50)

def get_current_playback(creds):
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    res = requests.get("https://api.spotify.com/v1/me/player", headers=head)

    # Sometimes if spotify not open anywhere
    if len(res.text) == 0:
        return None
    return res.json()
