import pymongo

def basic():
    client = pymongo.MongoClient("localhost", 27017)   
    spotify = client.spotify

    tracks = spotify.tracks.find({},sort=[("played_at", pymongo.DESCENDING)])
    for track in tracks:
    	print("[{}] {} - {}".format(track["played_at"], track["track"]["artists"][0]["name"], track["track"]["name"]))

basic()