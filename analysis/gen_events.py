import pymongo

def gen_events():
    client = pymongo.MongoClient("localhost", 27017)
    spotify = client.spotify

    is_playing = None
    track_id = None

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

        if new_event:
            event["timestamp"] = state["timestamp"]
            events.append(event)
    return events


def main():
    for e in gen_events():
        print(e)

if __name__=="__main__":main()