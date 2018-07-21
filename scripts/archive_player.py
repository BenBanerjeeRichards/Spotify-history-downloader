import logging
import util
import zipfile
import datetime
import pymongo
import json
import os

CONFIG = util.config()["sounds_good"]


# 14418881


def move_to_archive():
    timestamp = 1000 * (datetime.datetime.now().timestamp() - 86400)
    spotify = util.get_spotify_db()
    print("Archiving")
    spotify.player.aggregate([
        {"$match": {"timestamp":
                    {"$lt": timestamp}}},
        {"$out": "player_archive"}
    ])

    print("Done")
    query = {"timestamp": {"$lt": timestamp}}
    input("Delete {} records moved to archive? [Ctrl-C] to Cancel, [ENTER] to proceed >"
          .format(spotify.player.find(query).count()))

    print("R.I.P")
    spotify.player.delete_many(query)


def write_to_archive(archive: zipfile.ZipFile, file_name: str, states):
    n = len(states)
    f = open(file_name, "w+", encoding="UTF-8")
    json.dump(states, f)
    f.close()
    logging.info("Wrote {} records to {}".format(n, file_name))
    archive.write(file_name, compress_type=zipfile.ZIP_DEFLATED)
    os.remove(file_name)
    return n


def archive():
    spotify = util.get_spotify_db()

    today = datetime.datetime.now()
    today_start = int(datetime.datetime(today.year, today.month, today.day, 0, 0, 0).timestamp()) * 1000

    to_archive = spotify.player.find({"timestamp": {"$lt": today_start}}).sort("timestamp", pymongo.ASCENDING)

    current_date = datetime.datetime.fromtimestamp(int(to_archive[0]["timestamp"]) / 1000)
    current_date = datetime.datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)
    current_date_end_ts = (int(current_date.timestamp()) + 86400) * 1000
    zip_file = zipfile.ZipFile("archive.zip", mode="w", compression=zipfile.ZIP_DEFLATED)

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
        logging.info("Found {} states for day {}, saving them all".format(n_day, current_date))

        file_name = "{}-{}-{}.json".format(current_date.year, current_date.month, current_date.day)
        write_to_archive(zip_file, file_name, states_for_date)

    day_records = []
    i = 0

    zipFile = zipfile.ZipFile("archive.zip", mode="w", compression=zipfile.ZIP_DEFLATED)
    num_records_total = to_archive.count()
    num_records_processed = 0
    current_date_end = None

    for state in to_archive:
        if current_date_end is not None and state["timestamp"] > current_date_end * 1000:
            i += 1
            n = len(day_records)
            # Write records to file then compress
            file_name = "{}-{}-{}-{}.json".format(current_date.year, current_date.month, current_date.day, i)
            f = open(file_name, "w+", encoding="UTF-8")
            json.dump(day_records, f)
            f.close()
            logging.info("Wrote {} records to {}".format(n, file_name))
            num_records_processed += n
            zipFile.write(file_name, compress_type=zipfile.ZIP_DEFLATED)
            os.remove(file_name)
            current_date = None
            day_records = []
            percent = util.percent(num_records_processed, num_records_total)

            print("[{}%] Wrote {} records to {}".format(percent, n, file_name))

        if current_date_end is None or current_date is None or state["timestamp"] > current_date_end * 1000:
            current_date = datetime.datetime.fromtimestamp(int(state["timestamp"]) / 1000)
            current_date = datetime.datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)
            current_date_end = (int(current_date.timestamp()) + 86400)
            logging.info("Current date: {}".format(current_date))

        state["_id"] = state["_id"].__str__()
        day_records.append(state)


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
