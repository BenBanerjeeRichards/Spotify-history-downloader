import time

import pymongo
import logging
from spotify import get_current_playback, get_credentials

REQ_PER_SECOND = 5  # Goal number of updates of state per second
REFRESH_CRED_SECS = 30 * 60  # Refresh credentials every 30 minutes
BATCH_SIZE_INSERT = 3 * 60  # Once per minute
SLEEP_AFTER_FAILURE_BASE = 0.5

def get_player_state(creds):
    start = time.time()
    state_from_api = get_current_playback(creds)
    end = time.time()

    if state_from_api is not None:
        state = {
            "timestamp": state_from_api["timestamp"],
            "device": state_from_api["device"],
            "progress_ms": state_from_api["progress_ms"],
            "is_playing": state_from_api["is_playing"],
            "shuffle_state": state_from_api["shuffle_state"],
            "repeat_state": state_from_api["repeat_state"],
            "track_id": state_from_api["item"]["id"],
        }
    else:
        state = {
            # Estimate timestamp
            "timestamp": time.time() - (start + end) / 2,
            "progress_ms": 0,
            "is_playing": False,
        }
    return state


def store_player_states(states):
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
    spotify.player.insert_many(states)
    logging.info("Inserted {} states".format(len(states)))


def run():
    delay_ms = 100  # We update this to aim for REQ_PER_SECOND per second
    request_times = []

    # Batch insert to save time
    states = []
    batch_count = 0

    last_credential_time = time.time()
    creds = get_credentials()

    while True:
        batch_count += 1
        current_time = time.time()
        if current_time - last_credential_time > REFRESH_CRED_SECS:
            # REFRESH
            creds = get_credentials()
            last_credential_time = current_time

        state = get_player_state(creds)
        states.append(state)

        req_time = time.time() - current_time

        request_times.append(req_time)
        if len(request_times) > 10:
            request_times = list(reversed(request_times))[1:11]  # Use last 10 times

        # Calculate delay time
        avg_time = sum(request_times) / len(request_times)
        delay_ms = (1 - 3 * avg_time) / 3
        if delay_ms < 0:
            delay_ms = 0
        time.sleep(delay_ms)
        if batch_count > BATCH_SIZE_INSERT:
            store_player_states(states)
            batch_count = 0
            states = []

        if batch_count % 10 == 0:
            logging.info("Current sleep time: {}".format(delay_ms))
            logging.info("Current average request time: {}".format(avg_time))


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S', filename='player.log')

    try:
        run()
    except Exception as e:
        logging.exception("failed to get tracks")
        time.sleep(SLEEP_AFTER_FAILURE_BASE)
        main()  # Eventually reaches stack overflow of course

if __name__ == "__main__": main()
