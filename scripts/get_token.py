import webbrowser
from random import randint
from bottle import route, request, run
import threading
import time
import requests

CLIENT_SECRET = None
CLIENT_ID = None

def start():
    run(host='localhost', port=8080, debug=True)


@route('/spotify')
def spotify():
    if "code" not in request.params:
        print("Access to spotify denied: {}".format(request.params["error"]))
        return "Denied"

    code = request.params["code"]

    res = requests.post("https://accounts.spotify.com/api/token", {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": "http://localhost:8080/spotify",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    })

    if res.status_code != 200:
        print("Failed to get response code")
        print(res.json())
        return "Failed"

    refresh_token = res.json()["refresh_token"]
    template = """
        <pre>
export SPOTIFY_CLIENT_ID={}
export SPOTIFY_CLIENT_SECRET={}
export SPOTIFY_REFRESH_TOKEN={}
        </pre>
    """

    return template.format(CLIENT_ID, CLIENT_SECRET, refresh_token)


def main():
    global CLIENT_SECRET
    global CLIENT_ID

    has_app = None
    while has_app != "y" and has_app != "n":
        has_app = input("Have you created a client ID/Secret [y/n]")

    if has_app == "n":
        input("Press ENTER to go to dashboard and create new app")
        webbrowser.open("https://developer.spotify.com/dashboard/")

    input("IMPORTANT: Ensure redirect url is set to http://localhost:8080/spotify. ENTER to confirm")
    secret = str(randint(1, 100000))
    CLIENT_ID = input("Client ID: ")
    CLIENT_SECRET = input("Client Secret: ")

    auth_spotify = "https://accounts.spotify.com/en/authorize?response_type=code&redirect_uri=http://localhost:8080/spotify&client_id={}&scope=user-read-private%20user-read-email%20user-library-read%20user-top-read%20playlist-modify-public%20user-follow-read%20user-read-playback-state%20user-modify-playback-state%20user-read-recently-played%20playlist-modify-private%20user-follow-modify%20user-read-currently-playing%20playlist-read-collaborative%20user-library-modify%20playlist-read-private%20user-read-birthdate&state={}" \
        .format(CLIENT_ID, secret)

    t = threading.Thread(target=start)
    t.start()
    time.sleep(1)

    input("Allow app access to you account. Press ENTER to continue")
    webbrowser.open(auth_spotify)


if __name__ == "__main__": main()
