import util
import datetime
import spotify
db = util.get_spotify_db()


def new_songs(start: datetime.datetime, end: datetime.datetime) -> [str]:
    tracks = util.tracks_within_dates(db, start, end)

    previous_ids = db.tracks.find({
        "played_at": {
            "$lt": start
        }
    }).distinct("track.id")

    track_freq = util.track_frequency(tracks)

    new_tracks = []
    for track_id, count in track_freq:
        if track_id in previous_ids:
            continue

        new_tracks.append((track_id, count))

    favs = list(filter(lambda x: x[1] >= 3, new_tracks))
    return list(map(lambda x: x[0], favs))


def main():
    tracks = new_songs(datetime.datetime(2018, 8, 18), datetime.datetime(2018, 8, 25))
    creds = spotify.get_credentials()
    pl = spotify.create_playlist("benbanerjeerichards", "New tracks last week", False, False, None, creds)["id"]
    spotify.add_to_playlist("benbanerjeerichards", pl, tracks, creds)


if __name__ == '__main__':
    main()
