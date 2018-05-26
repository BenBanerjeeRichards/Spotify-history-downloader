import pymongo

def gen_events():
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    is_playing = None
    track_id = None
    is_shuffle = None
    device = None
    is_repeat = None

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
                track_info = spotify.tracks.find_one({"track": {"id": t_id}})
                if track_info is None:
                    # If new track not in db (definitly possible, recently played requires > 30 secs)
                    print("Failed to get track info for track {}".format(t_id))
                else:
                    info = {
                        "track": track_info["name"],
                        "artist": track_info["artists"][0]["name"]
                    }
                    event["track"] = info["track"]
                    event["artist"] = info["artist"]
                    track_cache[t_id] = info

def main():
    events = gen_events()
    add_info_to_events(events)
    for e in events:
        print(e)

if __name__=="__main__":main()