import pymongo
import read

from spotify import *
class DownloadException(Exception):
    pass
    
def insert(tracks):
    logging.info("Retrieved 50 songs from spotify")

    client = pymongo.MongoClient("localhost", 27017)    # Same in prod
    spotify = client.spotify

    # Get last track listened to stored in db
    # This is to ensure we don't duplicate items in database
    latest_track = spotify.tracks.find_one({},sort=[("played_at", pymongo.DESCENDING)])
    if latest_track:
        tracks = remove_tracks_before_inc(tracks, latest_track)
        logging.info("Got {} tracks to insert".format(len(tracks)))
    else:
        logging.info("Nothing played since last download, doing nothing...")
    if len(tracks) > 0:
        spotify.tracks.insert_many(tracks)
    client.close()  # TODO can we use with..as clause?

def remove_tracks_before_inc(tracks, stop_at_track):
    new = []
    for track in tracks:
        if track["played_at"] == stop_at_track["played_at"]:
            break
        new.append(track)

    return new

def pretty_recently_played_json(tracks):
    s = ""
    for item in tracks:
        s += "{} - {}\n".format(item["track"]["artists"][0]["name"], item["track"]["name"])
    return s
#chcp 65001

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='output.log')

    creds = get_credentials()
    j = get_recently_played(creds)
    insert(j["items"])

    # Update stuff
    read.update_albums()
    read.update_artists()
    read.update_features()

if __name__ == "__main__":main()