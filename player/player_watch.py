import psutil
import os 
import time
import logging
import subprocess
import sys

SCRIPT_NAME = "player.py"
LOCK_DIR = "player_lock"
SLEEP_SEC = 1
LOCK_FAIL_SLEEP_SEC = 2
STARTUP_WAIT_SEC = 3

def process_exists():   # NB: Very slow (~1-2sec)
    for pid in psutil.pids():
        p = None
        try:
            p = psutil.Process(pid)
        except psutil.NoSuchProcess:
            # This is expected
            # As short lived process may die before we get chance to call psutil
            continue
        if p.name().startswith("python"):
            if len(p.cmdline()) > 1 and  p.cmdline()[1] == SCRIPT_NAME:
                return True

    return False

def start_process(python_cmd, script_path):
    subprocess.Popen([python_cmd, script_path])

def try_remove_lock():
    try:
        os.rmdir(LOCK_DIR)
    except FileExistsError:
        logging.exception("Lock does't exist, no need to remove")


def loop(python_cmd, script_path):
    while True:
        # Use lock directory to protect against multiple player_watch scripts running

        try:
            os.mkdir(LOCK_DIR)
        except Exception:
            logging.exception("Failed to get lock")
            time.sleep(LOCK_FAIL_SLEEP_SEC)
        else:
            try:
                if not process_exists():
                    logging.info("No process exists...starting")
                    start_process(python_cmd, script_path)
                    time.sleep(STARTUP_WAIT_SEC)
                    logging.info("Process started. Verify = {}".format(process_exists()))
                time.sleep(SLEEP_SEC)
            except Exception:
                logging.exception("Error when checking/starting/unlocking:")
        finally:
            try_remove_lock()


def main():
    logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S', filename='player_watch.log')

    loop(sys.argv[1], sys.argv[2])

if __name__=="__main__":main()