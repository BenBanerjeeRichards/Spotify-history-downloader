import logging
import util
import zipfile
import datetime
import pymongo
import json
import os

CONFIG = util.config()["sounds_good"]


# 14418881


def write_to_archive(archive: zipfile.ZipFile, file_name: str, states):
    n = states.count()
    f = open(file_name, "w+", encoding="UTF-8")
    json.dump(list(states), f)
    f.close()
    logging.info("Wrote {} records to {}".format(n, file_name))
    archive.write(file_name, compress_type=zipfile.ZIP_DEFLATED)
    os.remove(file_name)
    return n


def archive():
    spotify = util.get_spotify_db()

    today = datetime.datetime.now()
    today = datetime.datetime(today.year, today.month, today.day, 0, 0, 0)
    today_start = int(today.timestamp()) * 1000

    to_archive = spotify.player.find({"timestamp": {"$lt": today_start}}).sort("timestamp", pymongo.ASCENDING)
    n_total = to_archive.count()

    current_date = datetime.datetime.fromtimestamp(int(to_archive[0]["timestamp"]) / 1000)
    current_date = datetime.datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)
    current_date_end_ts = (int(current_date.timestamp()) + 86400) * 1000
    zip_file = zipfile.ZipFile("archive.zip", mode="w", compression=zipfile.ZIP_DEFLATED)
    i = 0

    while current_date < today:
        states_for_date = spotify.player.find({
            "timestamp": {
                "$lt": current_date_end_ts,
                "$gt": current_date.timestamp() * 1000
            }
        }, {
            "_id": 0
        })

        n_day = states_for_date.count()

        file_name = "{}-{}-{}.json".format(current_date.year, current_date.month, current_date.day)
        i += write_to_archive(zip_file, file_name, states_for_date)
        logging.info("[{}%] Wrote {} states for day {}".format(util.percent(i, n_total), n_day, current_date))
        current_date = datetime.datetime.fromtimestamp((current_date_end_ts / 1000))
        current_date_end_ts = (int(current_date.timestamp()) + 86400) * 1000


def main():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='archive.log')

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger().addHandler(logging.StreamHandler())

    archive()


if __name__ == '__main__':
    main()
