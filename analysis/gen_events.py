import pymongo
import datetime
import sys
import logging

# If we move onto next song 100 * SKIP_THRESH % way through
# then don't consider it a skip
SKIP_THRESH = 0.95
CLEAN_BATCH_SIZE = 1000
INACTIVE_TIME_THRESH_MS = 1000  # If more than 1s apart create separate events
SEEK_UPPER_LIMIT_MS = 1500
SEEK_LOWER_LIMIT_MS = -1 * SEEK_UPPER_LIMIT_MS


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
                if diff > SEEK_UPPER_LIMIT_MS or diff < SEEK_LOWER_LIMIT_MS:
                    events.append({
                        "action": "seek",
                        "prev_progress": prev_progress / playing_song_duration,
                        "current_progress": state["progress_ms"] / playing_song_duration,
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


def add_info_to_events(events):
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
    track_cache = {}

    for event in events:
        if "track_id" in event["state"]:
            t_id = event["state"]["track_id"]
            info = get_track_info(spotify, t_id, track_cache)
            event["track"] = info["track"]
            event["artist"] = info["artist"]
        if "prev_track_id" in event and event["prev_track_id"] is not None:
            t_id = event["prev_track_id"]
            info = get_track_info(spotify, t_id, track_cache)
            event["prev_track"] = info["track"]
            event["prev_artist"] = info["artist"]


def fix_duration():
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

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

        track = spotify.player.find_one(query);


def update_clean_events():
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    states = spotify.player_clean.find({}, sort=[("timestamp", pymongo.DESCENDING)])
    after = None
    if states.count() > 0:
        after = states[0]["timestamp"]
        logging.info("Processsing states after {}({})".format(after, unix_to_iso(after)))
    else:
        print("Processing all states")

    clean_events(after, spotify)


def print_events(events):
    for e in events:
        ts = unix_to_iso(e["timestamp"])
        print("[{}]".format(ts), end="")
        print(" {}".format(e["action"]), end="")
        if e["action"] == "play" or e["action"] == "change_track":
            print(" {} by {}".format(e["track"], e["artist"]), end="")
        if e["action"] == "skip":
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
        initial_state = {}

    logging.info("Initial state for event gen: {}".format(initial_state.__str__()))
    logging.info("Num states to process = {}".format(states.count()))
    new_events = gen_events(initial_state, states)
    logging.info("Generated {} new events".format(len(new_events)))

    if len(new_events) > 0:
        spotify.events.insert_many(new_events)


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='gen_events.log')
    logging.getLogger().addHandler(logging.StreamHandler())
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    if len(sys.argv) <= 1:
        print("Provide action")
        return

    action = sys.argv[1]
    if action == "print":
        events = spotify.events.find()
        add_info_to_events(events)
        print_events(events)
    elif action == "refresh":
        refresh_events(spotify)

    else:
        print("Not sure what you mean mate")


if __name__ == "__main__": main()
