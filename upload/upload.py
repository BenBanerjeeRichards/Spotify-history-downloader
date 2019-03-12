import os
import datetime
from util import get_path, config
from db.db import DbStore
import logging


def write_basic_track_file(db: DbStore):
    contents = ""
    for track in db.get_basic_tracks():
        contents += "{},{},{}\n".format(track[0], track[1], track[2])

    open("tracks.txt", "w+").write(contents)


def write_csv(db: DbStore):
    db.export_plays_as_csv(get_path("upload/music.csv"))


def run_export():
    if not config()["export"]["enable"]:
        logging.info("Export disabled, not running...")
        return

    logging.info("Enabled, running yoyoyo okokokok")

    os.chdir(get_path("upload"))

    prev_music = ""
    if os.path.exists("music.csv"):
        prev_music = open("music.csv", "r").read()

    db = DbStore()
    write_csv(db)
    if open("music.csv", "r").read() != prev_music:
        logging.info("music.csv changed so reuploading to github")
        # I know should use subprocess
        os.system("rm main.sqlite")
        os.system("cp ../main.sqlite main.sqlite")
        os.system('sqlite3 main.sqlite ".dump" > main.sql')
        os.system("git add main.sql music.csv")

        os.system("git commit -m \"Data upload at {}\"".format(datetime.datetime.now().isoformat()))
        os.system("git push -u origin master")
    else:
        logging.info("tracks.txt the same, no new music to upload")


if __name__ == '__main__':
    run_export()
