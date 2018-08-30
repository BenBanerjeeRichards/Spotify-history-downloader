import util
import logging
from wordcloud import WordCloud
from collections import Counter
import datetime
import math

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S', filename='output.log')
logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

db = util.get_spotify_db()


def main():
    tracks = db.tracks.find({})
    all_genres = []
    today = datetime.datetime.now()
    freq = {}

    for track in tracks:
        # Get artist data
        artist = db.artists.find_one({"id": track["track"]["artists"][0]["id"]})

        if not artist:
            logging.info("No artist found for track {}".format(track))
            continue

        if len(artist["genres"]) == 0:
            continue

        genre = artist["genres"][0]
        all_genres.append(genre)

        days = (today - track["played_at"]).days
        freq_value = math.exp(-0.05 * days) + 1

        for genre in artist["genres"]:
            if genre not in freq:
                freq[genre] = freq_value
            else:
                freq[genre] += freq_value

        print("{}:{}".format(days, freq_value))

    genres_freqs = Counter(all_genres)
    wordcloud = WordCloud(width=1000, height=800).generate_from_frequencies(freq)

    image = wordcloud.to_image()
    image.show()


if __name__ == '__main__':
    main()
