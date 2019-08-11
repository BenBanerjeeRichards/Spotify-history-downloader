import pymongo
import sys
import time

def main():
    client = pymongo.MongoClient("localhost", 27017)    # Same in prod
    spotify = client.spotify
    dry_run = False

    if len(sys.argv) > 1:
        print("Dry run only: NO CHANGES WILL BE MADE")
        dry_run = True
    else:
        print("CHANGES WILL BE MADE: Ctrl-C to cancel")
        time.sleep(1.5)
    print("Proceeding with fix duplicates...")

    prev_name = None
    prev_ts = None

    for track in spotify.tracks.find({}, sort=[("played_at", pymongo.DESCENDING)]):
        name = track["track"]["name"]
        ts = track["played_at"]
        if name == prev_name and ts == prev_ts:
            print("Deleting {} at {}".format(name, ts))

            if not dry_run:
                res = spotify.tracks.delete_one({"_id": track["_id"]})

        prev_name = name
        prev_ts = ts


if __name__=="__main__":main()