import time

import pymongo
import logging
from spotify import get_current_playback, get_credentials

REQ_PER_SECOND = 10  # Goal number of updates of state per second
REFRESH_CRED_SECS = 30 * 60  # Refresh credentials every 30 minutes
BATCH_SIZE_INSERT = 3 * 60  # Once per minute
SLEEP_AFTER_FAILURE_BASE = 0.5
LOG_AFTER = 10

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
            "duration_ms": state_from_api["item"]["duration_ms"]
        }
    else:
        state = {
            # Estimate timestamp
            "timestamp": time.time() * 1000,
            "progress_ms": 0,
            "is_playing": False,
        }
    return state


def store_player_states(states):
    start = time.time()
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
    spotify.player.insert_many(states)
    logging.debug("[INSERT] Inserted {} states in {}".format(len(states), time.time() - start))


def run():
    delay_ms = 100  # We update this to aim for REQ_PER_SECOND per second
    request_times = []

    # Batch insert to save time
    states = []
    batch_count = 0

    last_credential_time = time.time()
    creds = get_credentials()

    # Used for logging purposes
    batch_start_time = time.time()

    log_count = 0

    while True:
        batch_count += 1
        log_count += 1
        
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
        delay_ms = (1 - REQ_PER_SECOND * avg_time) / REQ_PER_SECOND
        if delay_ms < 0:
            delay_ms = 0
        time.sleep(delay_ms)
        if batch_count > BATCH_SIZE_INSERT:
            store_player_states(states)
            batch_count = 0
            states = []

        if log_count == LOG_AFTER:
            log_count = 0
            end_b = time.time()
            dt = end_b - batch_start_time
            rate = LOG_AFTER / dt
            logging.debug("[STATUS] start={}, end={}s, dt = {}, sleep_time={}, avg_request_time={}, rate={}"
                .format(batch_start_time, end_b, dt, delay_ms, avg_time, rate))
            batch_start_time = end_b

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='player.log')
    
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    try:
        run()
    except Exception as e:
        logging.exception("failed to get tracks")
        time.sleep(SLEEP_AFTER_FAILURE_BASE)
        main()  # Eventually reaches stack overflow of course

if __name__ == "__main__": main()
