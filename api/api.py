from bottle import response, run, hook, route
import sys
from analysis.day import *
import spotify


@route("/spotify/day")
def get_stats():
    return get_stats(datetime.datetime.now())


@route("/spotify/day/<year>/<month>/<day>")
def get_stats_specific(year, month, day):
    return get_stats(datetime.datetime(int(year), int(month), int(day)))


@hook('after_request')
def enable_cors():
    """
    You need to add some headers to each request.
    Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'


@route("/spotify/playing")
def playing():
    creds = spotify.get_credentials()
    playing = spotify.get_current_playback(creds)
    if playing is None or playing == {}:
        return {}

    if playing["device"]["is_private_session"]:
        return {}

    track = playing["item"]

    return {
        "song": track["name"],
        "song_link": track["href"],
        "artist": track["artists"][0]["name"],
        "artist_link": track["artists"][0]["href"],
        "progress_ms": playing["progress_ms"],
        "duration_ms": track["duration_ms"],
        "progress": format_ms_as_str(playing["progress_ms"]),
        "duration": format_ms_as_str(track["duration_ms"])

    }


def format_ms_as_str(ms):
    minutes, seconds = format_ms(ms)
    return "{}:{}".format(minutes, seconds)


def format_ms(ms):
    as_seconds = ms / 1000
    minutes = int(as_seconds / 60)
    seconds = as_seconds - (60 * minutes)
    return minutes, int(seconds)


def main():
    run(host="206.189.24.92", port=9876, server='python_server')
    return
    if len(sys.argv) != 2:
        print("Pass either DEV or PROD")
        return

    if sys.argv[1] == "DEV":
        run(host="localhost", port=8080)
    elif sys.argv[1] == "PROD":
        run(host="206.189.24.92", port=9876, server='python_server')
    else:
        print("Provide PROD or DEV")


if __name__ == "__main__": main()
