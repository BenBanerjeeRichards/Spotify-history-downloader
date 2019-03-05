import os
import datetime
from util import config
from db.db import DbStore


def write_basic_track_file():
    db = DbStore()
    contents = ""
    for track in db.get_basic_tracks():
        contents += "{},{},{}\n".format(track[0], track[1],track[2])

    open("tracks.txt", "w+").write(contents)


def run_export():
    write_basic_track_file()
    cfg = config()
    os.chdir(cfg["upload"]["cwd"])
    # I know should use subprocess
    os.system("cp ../main.sqlite main.sqlite")
    os.system("git add main.sqlite tracks.txt")

    os.system("git commit -m \"Data upload at {}\"".format(datetime.datetime.now().isoformat()))
    os.system("git push -u origin master")


if __name__ == '__main__':
    run_export()
