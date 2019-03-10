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
    all_genres = []
    freq = {}
    genres = open("genres.txt", "r").read().split(",")

    for genre in genres:
        # Get artist data
        all_genres.append(genre)
        if genre not in freq:
            freq[genre] = 1
        else:
            freq[genre] += 1

    # genres_freqs = Counter(all_genres)
    wordcloud = WordCloud(width=1000, height=800).generate_from_frequencies(freq)

    image = wordcloud.to_image()
    image.show()


if __name__ == '__main__':
    main()
