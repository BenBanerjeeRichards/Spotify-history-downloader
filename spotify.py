import logging
import os
import requests
from http import HTTPStatus
import time


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


def spotify_multiple_req(url, ids, creds, extract_list, max_ids=20, sleep_ms=0):
    data = []
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    ids = list(set(ids))  # don't make duplicate requests

    for id_list in chunks(ids, max_ids):
        res = requests.get(url, params={"ids": ",".join(id_list)}, headers=head)
        new_albums = extract_list(res.json())
        data = data + new_albums
        logging.info("Got {}".format(len(new_albums)))

        # Helps with rate limiting
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000)

    return data


def get_albums(ids, creds, sleep=0):
    def extract(j):
        return j["albums"]

    return spotify_multiple_req("https://api.spotify.com/v1/albums", ids, creds, extract, sleep_ms=sleep, max_ids=20)


def get_artists(ids, creds, sleep=0):
    def extract(j):
        return j["artists"]

    return spotify_multiple_req("https://api.spotify.com/v1/artists", ids, creds, extract, sleep)


def search(query, type, type_key, creds):
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    res = requests.get("https://api.spotify.com/v1/search", params={"q": query, "type": type}, headers=head)
    return res.json()[type_key]["items"]


def get_track_features(ids, creds, sleep=0):
    def extract(j):
        return j["audio_features"]

    return spotify_multiple_req("https://api.spotify.com/v1/audio-features", ids, creds, extract, 100, sleep)


def get_tracks(ids, creds, sleep=0):
    def extract(j):
        return j["tracks"]

    return spotify_multiple_req("https://api.spotify.com/v1/tracks", ids, creds, extract, 50, sleep)


def get_current_playback(creds):
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    res = requests.get("https://api.spotify.com/v1/me/player", headers=head)

    # Sometimes if spotify not open anywhere
    if len(res.text) == 0:
        return None
    return res.json()


def get_profile(creds: Credentials):
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    return requests.get("https://api.spotify.com/v1/me", headers=head).json()


def spotify_get_json(url, creds):
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    res = requests.get(url, headers=head)
    res.raise_for_status()
    return res.json()


def get_songs_in_playlist(user_id, playlist_id, creds: Credentials):
    url = "https://api.spotify.com/v1/users/{}/playlists/{}/tracks?fields=items(track(id))%2Cnext&limit={}" \
        .format(user_id, playlist_id, 100)

    items = paging_get_all(url, creds)
    return list(map(lambda x: x["track"]["id"], items))



# Return limited playlist information
# Track id, track name and date added
def get_playlist_basic(user_id, playlist_id, creds):
    url = "https://api.spotify.com/v1/users/{}/playlists/{}/tracks?fields=items(track(name%2Cid)%2Cadded_at)%2Cnext&limit={}" \
        .format(user_id, playlist_id, 100)

    return paging_get_all(url, creds)


def get_saved_tracks(creds):
    return paging_get_all("https://api.spotify.com/v1/me/tracks", creds)


def chunked_function_call(function, constant_params, item_list, chunk_size, list_param_name):
    list_chunks = chunks(item_list, chunk_size)
    ret = []
    for chunk in list_chunks:
        constant_params[list_param_name] = chunk
        ret += function(**constant_params)

    return ret


def remove_from_playlist(user_id, playlist_id, track_ids, creds):
    tracks_json = []
    for t_id in track_ids:
        tracks_json.append({"uri": "spotify:track:{}".format(t_id)})

    def remove_from_pl_inner(t_json):
        head = {"Authorization": "Bearer {}".format(creds.access_token)}
        url = "https://api.spotify.com/v1/users/{}/playlists/{}/tracks".format(user_id, playlist_id)
        res = requests.delete(url, headers=head, json={"tracks": t_json})
        res.raise_for_status()
        return res.json()

    return chunked_function_call(remove_from_pl_inner, {}, tracks_json, 100, "t_json")


def add_to_playlist(user_id, playlist_id, track_ids, creds, replace=False):
    tracks_json = []
    for t_id in track_ids:
        tracks_json.append("spotify:track:{}".format(t_id))

    def remove_from_pl_inner(t_json):
        head = {"Authorization": "Bearer {}".format(creds.access_token)}
        url = "https://api.spotify.com/v1/users/{}/playlists/{}/tracks".format(user_id, playlist_id)
        if replace:
            res = requests.put(url, headers=head, json={"uris": t_json})
        else:
            res = requests.post(url, headers=head, json={"uris": t_json})

        res.raise_for_status()
        return res.json()

    return chunked_function_call(remove_from_pl_inner, {}, tracks_json, 100, "t_json")


def transfer_playlists(user_id, playlist_from, playlist_to, creds, delete_from_orig=False):
    pl_tracks = get_playlist_basic(user_id, playlist_from, creds)
    from_ids = list(map(lambda x: x["track"]["id"], pl_tracks))
    logging.info("Transferring {} tracks from spotify:user:{}:playlist:{} to spotify:user:{}:playlist:{}"
                 .format(len(from_ids), user_id, playlist_from, user_id, playlist_to))

    add_to_playlist(user_id, playlist_to, from_ids, creds)

    if delete_from_orig:
        logging.info("Deleting all tracks from from playlist")
        remove_from_playlist(user_id, playlist_from, from_ids, creds)


def paging_get_all(url, creds):
    items = []
    while url:
        response = spotify_get_json(url, creds)
        items += response["items"]
        url = response["next"] if "next" in response else None

    return items


def create_playlist(user: str, name: str, public: bool, collaborative: bool, description: str, creds):
    body = {
        "name": name,
        "public": public,
        "collaborative": collaborative,
        "description": description
    }

    return authenticated_spotify_post("https://api.spotify.com/v1/users/{}/playlists".format(user, body, creds), body,
                                      creds)


def authenticated_spotify_post(url, body: dict, creds):
    head = {"Authorization": "Bearer {}".format(creds.access_token)}
    res = requests.post(url, headers=head, json=body)
    res.raise_for_status()

    if res.status_code == HTTPStatus.NO_CONTENT:
        return {}

    return res.json()
