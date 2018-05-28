import pymongo

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

def main():
    events = gen_events()
    add_info_to_events(events)
    for e in events:
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
                if e["progress_ratio"] < .95:
                    print("Skip after {}%".format(int(e["progress_ratio"] * 100)))
            print("Started playing {} by {}".format(e["track"], e["artist"]))
        if "is_playing" in e and e["is_playing"] == False:
            print("Stopped playing")

if __name__=="__main__":main()