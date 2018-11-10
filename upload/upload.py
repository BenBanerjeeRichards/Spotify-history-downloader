import os
import datetime


def run_export():
    # I know should use subprocess
    os.system("mongoexport --db spotify --collection tracks --out tracks.json")
    os.system("mongoexport --db spotify --collection events --out events.json")
    os.system("git add tracks.json events.json")
    os.system("git commit -m \"Data upload at {}\"".format(datetime.datetime.now()))
    os.system("git push -u origin master")


if __name__ == '__main__':
    run_export()