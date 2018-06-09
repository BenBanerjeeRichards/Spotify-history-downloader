import pymongo
import datetime
import dateutil.parser
def main():
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
    s = ""
    # First dump id,name pairs to file for checking 
    for track in spotify.tracks.find():
        s += "{},{},{}\n".format(track["_id"], track["track"]["name"], track["played_at"])
    open("ref.txt", "w+", encoding="utf-8").write(s)
    dt = datetime.datetime.now()
    
    n_fixed = 0
    for track in spotify.tracks.find():
        if type(track["played_at"]) != type(dt):
            n_fixed += 1
            dt = dateutil.parser.parse(track["played_at"])
            spotify.tracks.update({"_id": track["_id"]}, {"$set": {"played_at": dt}})
    print("Fixed {} tracks".format(n_fixed))

if __name__ == "__main__": main()
