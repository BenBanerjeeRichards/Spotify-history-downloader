import time
import logging
import db.player_store as player_store

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S', filename='newplayer.log')

from spotify import get_current_playback, get_credentials
import util

CONFIG = util.config()["player"]

REQ_PER_SECOND = CONFIG["req_per_second"]
REFRESH_CRED_SECS = 30 * 60  # Refresh credentials every 30 minutes
BATCH_SIZE_INSERT = CONFIG["batch_size_insert"]
SLEEP_AFTER_FAILURE_BASE = 0.5
LOG_AFTER = CONFIG["log_after"]


def get_player_state(creds):
    start = time.time() * 1000
    state_from_api = get_current_playback(creds)
    end = time.time() * 1000

    if state_from_api is not None:
        state = {
            "api_timestamp": state_from_api["timestamp"],
            "device": state_from_api["device"],
            "progress_ms": state_from_api["progress_ms"],
            "is_playing": state_from_api["is_playing"],
            "shuffle_state": state_from_api["shuffle_state"],
            "repeat_state": state_from_api["repeat_state"],
            "track_id": state_from_api["item"]["id"],
            "duration_ms": state_from_api["item"]["duration_ms"],
            # estimate **actual** timestamp**
            # See https://github.com/spotify/web-api/issues/640 (possibily caching due to high request rate)
            "timestamp": (start + end) / 2.0,  
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
    player_store.store().store_states(states)
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
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger().addHandler(logging.StreamHandler())

    try:
        run()
    except Exception as e:
        logging.exception("failed to get tracks")
        time.sleep(SLEEP_AFTER_FAILURE_BASE)
        main()  # Eventually reaches stack overflow of course

if __name__ == "__main__": main()
