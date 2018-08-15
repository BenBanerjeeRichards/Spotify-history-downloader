import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pymongo
import util
import datetime


def get_data():
    spotify = util.get_spotify_db()
    tracks = []
    for track in spotify.tracks.find():
        tracks.append({
            "id": track["track"]["id"],
            "name": track["track"]["name"],
            "artist": track["track"]["artists"][0]["name"],
            "date_played": track["played_at"].date()
        })

    return tracks


def track_ids():
    return util.get_spotify_db().tracks.distinct("track.id")


def get_dates(data):
    unique_dates = []
    for item in data:
        if item["date_played"] not in unique_dates:
            unique_dates.append(item["date_played"])
    return sorted(unique_dates)


def get_freq_matrix(data):
    ids = track_ids()
    dates = get_dates(data)
    F = np.zeros((len(ids), len(dates)))

    for track in data:
        i = ids.index(track["id"])
        j = dates.index(track["date_played"])
        F[i, j] += 1

    return F


def get_day_listen_totals(F):
    return np.sum(F, 0)


def get_song_listen_totals(F):
    return np.sum(F, 1)


def plot_day_listen_totals(F, dates):
    F_sums = get_day_listen_totals(F)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    plt.gca().xaxis.set_major_locator(mdates.WeekdayLocator())
    plt.plot(dates, np.cumsum(F_sums))
    plt.gcf().autofmt_xdate()
    plt.title("Cumulative Spotify song plays")
    plt.xlabel("Day")
    plt.ylabel("Cumulative Number of plays on day")
    plt.savefig("cumulative_plays.png")
    plt.show()



def augment_row_with_song_indicies(A):
    n_cols = np.size(A)
    B = np.zeros((1, n_cols))
    for i in range(n_cols):
        B[0, i] = i

    return np.vstack((A, B))


def plot_song_dist(F, dates, index, title, cum_sum=False):
    song = F[index, :]
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    plt.gca().xaxis.set_major_locator(mdates.WeekdayLocator())
    if cum_sum:
        song = np.cumsum(song)
    plt.plot(dates, song)
    plt.gcf().autofmt_xdate()
    plt.title(title)
    plt.xlabel("Day")
    plt.ylabel("Number of plays on day")
    plt.show()


def plot_zipf_curve(by_most_listened):
    plt.bar(np.arange(2022), by_most_listened[0, :])
    plt.title("Song plays by song")
    plt.xlabel("??")
    plt.ylabel("Number of plays of song")
    plt.show()


def index_to_track(index: int, ids, data):
    id = ids[index]
    for track in data:
        if track["id"] == id:
            return track


def plot_cdfs_top_played(n=49):
    for i in range(n):
        index = int(by_most_listened[1, i])
        track = index_to_track(index, ids, data)

        song = np.cumsum(F[index, :])
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
        plt.gca().xaxis.set_major_locator(mdates.WeekdayLocator())
        plt.plot(dates, song)
        plt.gcf().autofmt_xdate()
        plt.title(track["name"] + " - " + track["artist"])
        plt.xlabel("Day")
        plt.ylabel("Number of plays on day")
        plt.savefig("top/{}.png".format(i))
        plt.clf()



data = get_data()
dates = get_dates(data)
ids = track_ids()

F = get_freq_matrix(get_data())
F_sums = get_day_listen_totals(F)

F_song_sum = get_song_listen_totals(F)
song_sum = augment_row_with_song_indicies(F_song_sum)
by_most_listened = song_sum[:, song_sum[0, :].argsort()[::-1]]

plot_day_listen_totals(F, dates)

