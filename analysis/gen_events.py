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


def gen_events():
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    is_playing = None
    track_id = None
    is_shuffle = None
    device = None
    is_repeat = None
    prev_progress = 0
    playing_song_duration = 0

    # Seek tracking
    ts_at_start = None
    prev_progress = None
    prev_timestamp = None


    states = spotify.player.find({}, sort=[("timestamp", pymongo.ASCENDING)])
    events = []

    for state in states:
        event = {}
        new_event = False

        if is_playing != state["is_playing"]:
            is_playing = state["is_playing"]
            action_name = "play" if  state["is_playing"] else "pause"
            events.append({
                "action": action_name,
                "state": state,
                "timestamp": state["timestamp"]
            })

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
                    "timestamp": state["timestamp"]
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
                    "timestamp": state["timestamp"]
                })

        if "repeat_state" in state:
            if is_repeat != state["repeat_state"]:
                is_repeat = state["repeat_state"]
                action_name = "repeat_on" if is_repeat else "repeat_off"
                events.append({
                    "action": action_name,
                    "state": state,
                    "timestamp": state["timestamp"]
                })

        if "device" in state:
            if device != state["device"]["name"]:
                device = state["device"]["name"]
                events.append({
                    "action": "connect_device",
                    "state": state,
                    "timestamp": state["timestamp"]
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
                        "timestamp": state["timestamp"]
                    })
        if "progress_ms" in state:
            prev_progress = state["progress_ms"] 

        prev_timestamp = state["timestamp"]

    return events


def add_info_to_events(events):
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
    track_cache = {}

    for event in events:
        if "track_id" not in event["state"]:
            continue
        t_id = event["state"]["track_id"]

        if t_id in track_cache:
            event["track"] = track_cache[t_id]["track"]
            event["artist"] = track_cache[t_id]["artist"]
        else:
            track_info = spotify.full_tracks.find_one({"id": t_id})
            if track_info is None:
                print("Failed to get track info for track {}".format(t_id))
            else:
                info = {
                    "track": track_info["name"],
                    "artist": track_info["artists"][0]["name"]
                }
                event["track"] = info["track"]
                event["artist"] = info["artist"]
                track_cache[t_id] = info


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

def clean_events(after = None, spotify = None):
    if not spotify:
        client = pymongo.MongoClient("localhost", 27017)
        spotify = client.spotify

    query = {}
    if after is not None:
        query = {"timestamp": {"$gt": after}}

    states = spotify.player.find(query, sort=[("timestamp", pymongo.ASCENDING)])
    batch_count = 0
    batch = []
    total_inserted = 0
    count = states.count()
    states_seen = 0


    prev_inactive = False
    prev_timestamp = None
    inactive_batch_start = None
    for state in states:
        states_seen += 1
        batch_count += 1
        if batch_count >= CLEAN_BATCH_SIZE or states_seen == count:
            n = len(batch)
            total_inserted += n 

            # If we ended with inactive messages then IGNORE then in database.
            # They will be transferred across at another point
            # This prevents fragmenting them just due to times this function is run at

            if n > 0:
                spotify.player_clean.insert_many(batch)
            batch_count = 0
            batch = []

            logging.info("[{}%] Inserted batch of {}. Total = {}".format(int(100 * (states_seen / count)), n, total_inserted))
        
        record_inactive = "track_id" not in state and state["progress_ms"] == 0 and state["is_playing"] == False 
        
        # Below commenting notation just to visalize 
        # A = active record
        # I = inactive record
        # right most symbol is current state

        # IIIIII
        if prev_inactive and record_inactive:
            # Create new inactive event it time difference is too much
            if state["timestamp"] - prev_timestamp > INACTIVE_TIME_THRESH_MS:
                batch.append({
                    "type": "inactive",
                    "from_timestamp": inactive_batch_start,
                    "to_timestamp": prev_timestamp,
                    "timestamp": prev_timestamp # Duplicate so we can search all records
                })
                batch_count += 1
                inactive_batch_start = state["timestamp"]

        # AAAAI
        if not prev_inactive and record_inactive:
            # Start new inactive
            inactive_batch_start = state["timestamp"]

        # AAAAAA
        if not prev_inactive and not record_inactive:
            # Just add as normal
            batch.append(state)
            batch_count += 1

        # IIIIIIA
        if prev_inactive and not record_inactive:
            batch.append({
                "type": "inactive",
                "from_timestamp": inactive_batch_start,
                "to_timestamp": prev_timestamp,
                "timestamp": prev_timestamp
            })
            batch_count += 1

        prev_inactive = record_inactive
        prev_timestamp = state["timestamp"]

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
                .format(e["track"], e["artist"], int(100 * e["prev_progress"]), int(100 * e["current_progress"]), e["diff_amount_ms"]), end="")
        if e["action"] == "skip_track":
            print(" after {}%".format(int(100 * e["prev_progress"])), end="")
        print()

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='gen_events.log')
    logging.getLogger().addHandler(logging.StreamHandler())
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    states = spotify.player.find({"timestamp": {"$gt": 1527794070000}})

    if len(sys.argv) <= 1:
        print("Provide action")
        return
    
    action =sys.argv[1]
    if action == "print":
        events = gen_events()
        add_info_to_events(events)
        print_events(events)
    elif action == "update":
        update_clean_events()
    else:
        print("Not sure what you mean mate")

if __name__=="__main__":main()