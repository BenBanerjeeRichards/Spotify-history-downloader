from main import clean_track
import util


def main():
    spotify = util.get_spotify_db()
    all = spotify.tracks.find({})
    n = all.count()
    print("Processing {} docs".format(n))
    i = 0
    for track in all:
        i = i + 1
        clean = clean_track(track["track"])

        spotify.tracks.delete_one({"_id": track["_id"]})
        track["track"] = clean
        spotify.tracks.insert(track)

        if i % 100 == 0:
            print("[{}%] done".format(util.percent(i, n)))


if __name__ == '__main__':
    main()
