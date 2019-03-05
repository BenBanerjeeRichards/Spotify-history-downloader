import os
import datetime
from util import config


def run_export():

    cfg = config()
    os.chdir(cfg["upload"]["cwd"])
    # I know should use subprocess
    os.system("cp ../main.sqlite main.sqlite")
    os.system("git add main.sqlite")

    os.system("git commit -m \"Data upload at {}\"".format(datetime.datetime.now().isoformat()))
    os.system("git push -u origin master")


if __name__ == '__main__':
    run_export()