import util
from typing import List, Tuple, Dict
import pymongo


class MongoStore:

    def __init__(self):
        self.spotify = util.get_spotify_db()

    def store_states(self, states):
        self.spotify.player.insert_many(states)

    def player_distinct_track_ids(self) -> List[str]:
        return self.spotify.player.find().distinct("track_id")

    def player_states_after_time_asc(self, after_timestamp: float):
        return self.spotify.player.find({"timestamp": {"$gt": after_timestamp}},
                                        sort=[("timestamp", pymongo.ASCENDING)])

    def player_get_states_asc_timestamp(self):
        return self.spotify.player.find(sort=[("timestamp", pymongo.ASCENDING)])



def store():
    return MongoStore()
