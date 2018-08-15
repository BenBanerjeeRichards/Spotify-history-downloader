import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S', filename='gen_events.log')

logging.getLogger().addHandler(logging.StreamHandler())

import pymongo
import datetime
import sys
import read
import util

# If we move onto next song 100 * SKIP_THRESH % way through
# then don't consider it a skip
CONFIG = util.config()["gen_events"]
SKIP_THRESH = CONFIG["skip_thresh"]
CLEAN_BATCH_SIZE = 1000
INACTIVE_TIME_THRESH_MS = CONFIG["inactive_time_thresh_ms"]  # If more than 1s apart create separate events
SEEK_LIMIT = CONFIG["seek_limit"]


def unix_to_iso(timestamp_ms):
    return datetime.datetime.fromtimestamp(
        timestamp_ms / 1000
    ).strftime('%Y-%m-%d %H:%M:%S')


def gen_events(initial_state, states):
    state = initial_state["state"]
    is_playing = None if "is_playing" not in state else state["is_playing"]
    track_id = None if "track_id" not in state else state["track_id"]
    is_shuffle = None if "shuffle_state" not in state else state["shuffle_state"]
    device = None if "device" not in state else state["device"]["name"]
    is_repeat = None if "repeat_state" not in state else state["repeat_state"]
    playing_song_duration = 0

    prev_progress = 0 if "prev_progress" not in initial_state else initial_state["prev_progress"]
    prev_timestamp = 0 if "prev_timestamp" not in initial_state else initial_state["prev_timestamp"]

    events = []

    for state in states:
        if is_playing != state["is_playing"]:
            is_playing = state["is_playing"]
            action_name = "play" if state["is_playing"] else "pause"
            events.append({
                "action": action_name,
                "state": state,
                "timestamp": state["timestamp"],
                "prev_progress": prev_progress,
                "prev_timestamp": prev_timestamp
            })

        # If track_id in event then some song is loaded on the player
        if "track_id" in state:
            if track_id != state["track_id"]:

                # Calculate how far through previous track we were
                prog_ratio = 1
                if playing_song_duration > 0:
                    prog_ratio = prev_progress / playing_song_duration

                action_name = "change_track"
                if prog_ratio < SKIP_THRESH:
                    action_name = "skip_track"

                events.append({
                    "action": action_name,
                    "prev_progress": prog_ratio,
                    "state": state,
                    "prev_track_id": track_id,
                    "timestamp": state["timestamp"],
                    "prev_timestamp": prev_timestamp
                })

                track_id = state["track_id"]

                playing_song_duration = state["duration_ms"]
                prev_progress = state["progress_ms"]

        if "shuffle_state" in state:
            if is_shuffle != state["shuffle_state"]:
                is_shuffle = state["shuffle_state"]
                action_name = "shuffle_{}".format(is_shuffle)
                events.append({
                    "action": action_name,
                    "state": state,
                    "timestamp": state["timestamp"],
                    "prev_progress": prev_progress,
                    "prev_timestamp": prev_timestamp
                })

        if "repeat_state" in state:
            if is_repeat != state["repeat_state"]:
                is_repeat = state["repeat_state"]
                action_name = "repeat_on" if is_repeat else "repeat_off"
                events.append({
                    "action": action_name,
                    "state": state,
                    "timestamp": state["timestamp"],
                    "prev_progress": prev_progress,
                    "prev_timestamp": prev_timestamp
                })

        if "device" in state:
            if device != state["device"]["name"]:
                device = state["device"]["name"]
                events.append({
                    "action": "connect_device",
                    "state": state,
                    "timestamp": state["timestamp"],
                    "prev_progress": prev_progress,
                    "prev_timestamp": prev_timestamp
                })

        # We can now generate accurate seek events
        if "api_timestamp" in state and state["is_playing"]:
            if prev_progress is not None:
                diff_progress = state["progress_ms"] - prev_progress
                diff_time = state["timestamp"] - prev_timestamp
                diff = diff_progress - diff_time
                if abs(diff) > SEEK_LIMIT:
                    events.append({
                        "action": "seek",
                        "prev_progress": prev_progress / state["duration_ms"],
                        "current_progress": state["progress_ms"] / state["duration_ms"],
                        "diff_amount_ms": diff,
                        "state": state,
                        "timestamp": state["timestamp"],
                        "prev_timestamp": prev_timestamp
                    })
        if "progress_ms" in state:
            prev_progress = state["progress_ms"]

        prev_timestamp = state["timestamp"]

    return events


def get_track_info(spotify, t_id, track_cache):
    if t_id in track_cache:
        return track_cache[t_id]
    else:
        track_info = spotify.full_tracks.find_one({"id": t_id})
        if track_info is None:
            print("Failed to get track info for track {}".format(t_id))
        else:
            info = {
                "track": track_info["name"],
                "artist": track_info["artists"][0]["name"]
            }

            track_cache[t_id] = info
            return info


def add_info_to_events():
    spotify = util.get_spotify_db()
    track_cache = {}

    events_without_track = spotify.events.find({"state.track_id": {"$exists": True}, "track": {"$exists": False}})
    events_without_prev_track = spotify.events.find({"prev_track_id": {"$exists": True}, "prev_track": {"$exists": False}})

    logging.info("Found {} without track, {} without prev track"
                 .format(events_without_track.count(), events_without_prev_track.count()))

    for event in events_without_track:
        t_id = event["state"]["track_id"]
        info = get_track_info(spotify, t_id, track_cache)

        spotify.events.update(
            {"_id": event["_id"]},
            {"$set":
                 {"track": info["track"],
                  "artist": info["artist"]}
             })

    logging.info("Finished adding info to track events")

    for event in events_without_prev_track:
        if "prev_track_id" not in event or event["prev_track_id"] is None:
            logging.info("No prev_track_id exists in event {}".format(event["_id"]))
            continue
        t_id = event["prev_track_id"]
        info = get_track_info(spotify, t_id, track_cache)

        spotify.events.update(
            {"_id": event["_id"]},
            {"$set":
                 {"prev_track": info["track"],
                  "prev_artist": info["artist"]}
             })

    logging.info("Finished adding info to prev track events")


def fix_duration():
    spotify = util.get_spotify_db()

    query = {
        "duration_ms": {"$exists": False},
        "track_id": {"$exists": True}
    }

    track = spotify.player.find_one(query);
    done = 0

    while track:
        t_id = track["track_id"]
        duration = spotify.full_tracks.find_one({"id": t_id})["duration_ms"]

        res = spotify.player.update_many(
            {"track_id": t_id},
            {"$set": {"duration_ms": duration}
             })

        done += res.modified_count
        print("Updated {}, cumulative total = {}".format(res.modified_count, done))

        track = spotify.player.find_one(query)


def fix_prev_track():
    spotify = util.get_spotify_db()
    events = spotify.events.find({"state.track_id": {"$exists": True}})
    prev_track_id = events[0]["state"]["track_id"]

    n = events.count()
    for i, event in enumerate(events[1:]):
        track_id = event["state"]["track_id"]
        if track_id != prev_track_id:
            spotify.events.update({"_id": event["_id"]}, {"$set": {"prev_track_id": prev_track_id}})

        prev_track_id = track_id

        if i % 1000:
            logging.info("Completed {}%".format(util.percent(i, n)))


def print_events(events):
    for e in events:
        ts = unix_to_iso(e["timestamp"])
        print("[{}]".format(ts), end="")
        print(" {}".format(e["action"]), end="")
        if e["action"] == "play" or e["action"] == "change_track":
            print(" {} by {}".format(e["track"], e["artist"]), end="")
        if e["action"] == "skip_track":
            print(" to {} by {} after {}%".format(e["track"], e["artist"], int(100 * e["prev_progress"])), end="")
        if e["action"] == "seek":
            print(" track {} by {} from {}% to {}% diff={}"
                  .format(e["track"], e["artist"], int(100 * e["prev_progress"]), int(100 * e["current_progress"]),
                          e["diff_amount_ms"]), end="")
        if e["action"] == "skip_track":
            print(" after {}%".format(int(100 * e["prev_progress"])), end="")
        if "prev_track_id" in e and e["prev_track_id"] is not None:
            print(" previous track = {}".format(e["prev_track"]), end="")
        print()


def refresh_events(spotify):
    events = spotify.events.find({}, sort=[("timestamp", pymongo.DESCENDING)])
    logging.info("Refreshing events")

    if events.count() > 0:
        after = events[0]["timestamp"]
        states = spotify.player.find({"timestamp": {"$gt": after}}, sort=[("timestamp", pymongo.ASCENDING)])
        logging.info("Processing events after {}({})".format(after, unix_to_iso(after)))
        initial_state = events[0]
    else:
        logging.info("Processing all events (no existing events)")
        states = spotify.player.find(sort=[("timestamp", pymongo.ASCENDING)])
        initial_state = {"state": {}}

    logging.info("Initial state for event gen: {}".format(initial_state.__str__()))
    logging.info("Num states to process = {}".format(states.count()))
    new_events = gen_events(initial_state, states)
    logging.info("Generated {} new events".format(len(new_events)))

    if len(new_events) > 0:
        spotify.events.insert_many(new_events)

    read.update_full_tracks()
    add_info_to_events()


def main():
    spotify = util.get_spotify_db()

    if len(sys.argv) <= 1:
        print("Provide action")
        return

    action = sys.argv[1]
    if action == "print":
        add_info_to_events()
        events = spotify.events.find()
        print_events(events)
    elif action == "refresh":
        refresh_events(spotify)

    else:
        print("Not sure what you mean mate")


if __name__ == "__main__": main()
