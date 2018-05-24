import csv
import time

import dateutil.parser


def load(file):
    with open(file, encoding="utf-8") as f:
        data = [{k: v for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)]

    for item in data:
        item["duration_ms"] = int(item["duration_ms"])
        item["danceability"] = float(item["danceability"])
        item["energy"] = float(item["energy"])
        item["key"] = int(item["key"])
        item["loudness"] = float(item["loudness"])
        item["mode"] = int(item["mode"])
        item["speechiness"] = float(item["speechiness"])
        item["acousticness"] = float(item["acousticness"])
        item["instrumentalness"] = float(item["instrumentalness"])
        item["liveness"] = float(item["liveness"])
        item["valence"] = float(item["valence"])
        item["tempo"] = float(item["tempo"])
        item["time_signature"] = int(item["time_signature"])
        item["track_popularity"] = int(item["track_popularity"])
        item["artist_popularity"] = float(item["artist_popularity"])
        item["played_at"] = dateutil.parser.parse(item["played_at"])
        item["played_timestamp"] = time.mktime(item["played_at"].timetuple())
    return data


def add_time_offsets(data):
    play_order = list(reversed(data))
    prev_expected_ending = play_order[0]["played_timestamp"] + play_order[0]["duration_ms"] / 1000
    play_order[0]["diff"] = 0
    for track in play_order[1:]:
        diff = track["played_timestamp"] - prev_expected_ending
        prev_expected_ending = track["played_timestamp"] + track["duration_ms"] / 1000
        track["diff"] = diff


def skipped_songs(data):
    for track in data:
        if track["diff"] < -5:
            print(track["name"])


def time_listening(songs):
    total_seconds = 0
    for track in songs:
        total_seconds += track["duration_ms"] / 1000
        if track["diff"] < -1.5:    # SKIP
            total_seconds += track["diff"]
    return total_seconds

def main():
    data = load("out.csv")
    add_time_offsets(data)
    print(data[-1])
    print(time_listening(data) / (60 * 60))

if __name__ == "__main__": main()
