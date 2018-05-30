import pymongo
import datetime
import sys
import logging

# If we move onto next song 100 * SKIP_THRESH % way through
# then don't consider it a skip
SKIP_THRESH = 0.95
CLEAN_BATCH_SIZE = 1000
INACTIVE_TIME_THRESH_MS = 1000  # If more than 1s apart create separate events

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
    progress = 0
    playing_song_duration = 0

    states = spotify.player.find({}, sort=[("timestamp", pymongo.ASCENDING)])
    events = []

    for state in states:
        event = {}
        new_event = False

        if is_playing != state["is_playing"]:
            new_event = True
            event["is_playing"] =  state["is_playing"]
            is_playing = state["is_playing"]

        if "track_id" in state:
            if track_id != state["track_id"]:
                new_event = True
                event["track_id"] = state["track_id"]
                track_id = state["track_id"]

                # Calculate how far through previous track we were
                if playing_song_duration > 0:
                    event["progress_ratio"] = progress / playing_song_duration
                playing_song_duration = state["duration_ms"]
                progress = state["progress_ms"]

        if "shuffle_state" in state:
            if is_shuffle != state["shuffle_state"]:
                new_event = True
                event["is_shuffle"] = state["shuffle_state"]
                is_shuffle = state["shuffle_state"]

        if "repeat_state" in state:
            if is_repeat != state["repeat_state"]:
                new_event = True
                event["repeat"] = state["repeat_state"]
                is_repeat = state["repeat_state"]

        if "device" in state:
            if device != state["device"]["name"]:
                new_event = True
                event["device"] = state["device"]["name"]
                device = state["device"]["name"]

        if new_event:
            event["timestamp"] = state["timestamp"]
            events.append(event)

        if "progress_ms" in state:
            progress = state["progress_ms"] 
    return events


def add_info_to_events(events):
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify
    track_cache = {}

    for event in events:
        if "track_id" in event:
            t_id = event["track_id"]

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

        if "repeat" in e:
            print("Repeat {}".format(e["repeat"]))
        if "is_shuffle" in e:
            if e["is_shuffle"]:
                print("Shuffle on")
            else:
                print("Shuffle off")
        if "is_playing" in e and e["is_playing"] == True:
            print("Play")
        if "device" in e:
            print("Started listening on device {}".format(e["device"]))
        if "track" in e:
            # Skip?
            if "progress_ratio" in e:
                if e["progress_ratio"] < SKIP_THRESH:
                    print("Skip after {}%".format(int(e["progress_ratio"] * 100)))
            print("Started playing {} by {}".format(e["track"], e["artist"]))
        if "is_playing" in e and e["is_playing"] == False:
            print("Stopped playing")

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S', filename='gen_events.log')
    logging.getLogger().addHandler(logging.StreamHandler())

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