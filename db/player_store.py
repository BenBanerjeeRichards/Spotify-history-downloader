import time
from typing import List, Tuple, Dict
import pymongo
import sqlite3
import util
import logging


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


class Sqlite3Store:

    def __init__(self):
        cfg = util.config()
        self.conn = sqlite3.connect(util.get_path(cfg["db"]["player_sqlite_file"]))
        self.conn.execute("""
        create table if not exists player (
          timestamp          int,
          api_timestamp      real,
          track_id           text,
          progress_ms        int,
          duration_ms        int,
          is_playing         int,
          repeat             int,
          shuffle_state      int,
          device_id          text,
          device_active      int,
          volume_percent     int,
          is_private_session int,
          device_type        text,
          device_name        text
          ); 
        """)

        self.conn.commit()

    def store_states(self, states):
        state_tuples = list(map(lambda x: self._state_to_tuple(x), states))
        self.conn.executemany("INSERT INTO player VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", state_tuples)
        self.conn.commit()

    def player_distinct_track_ids(self) -> List[str]:
        res = self.conn.execute("select track_id from player")
        ids = []
        for item in res.fetchall():
            # If we got an empty request
            if item[0] is not None:
                ids.append(item[0])

        return list(set(ids))

    def player_states_after_time_asc(self, after_timestamp: float, limit=None):
        count = self.conn.execute("select count(*) from player where timestamp > ? order by timestamp ASC",
                                  (after_timestamp,))
        n = count.fetchall()[0][0]
        logging.info("COUNT events = {}".format(n))
        print("COUNT IS {}".format(n))

        res = self.conn.execute("select * from player where timestamp > ? order by timestamp ASC", (after_timestamp,))
        return self._result_to_json(res)

    def player_get_states_asc_timestamp(self, after_timestamp: float, limit=None):
        if limit is None:
            res = self.conn.execute("select * from player order by timestamp ASC")
        else:
            res = self.conn.execute("select * from player order by timestamp ASC limit {}".format(limit))
        return self._result_to_json(res)

    def _result_to_json(self, res):
        jsons = []
        for item in res.fetchall():
            jsons.append(self._tuple_to_json(item))

        return jsons

    def _tuple_to_json(self, tuple):
        return self._remove_nulls({
            "timestamp": tuple[0],
            "api_timestamp": tuple[1],
            "track_id": tuple[2],
            "progress_ms": tuple[3],
            "duration_ms": tuple[4],
            "is_playing": tuple[5],
            "repeat_state": tuple[6],
            "shuffle_state": tuple[7],
            "device": {
                "id": tuple[8],
                "is_active": tuple[9],
                "volume_percent": tuple[10],
                "is_private_session": tuple[11],
                "type": tuple[12],
                "name": tuple[13],
            }
        })

    def _remove_nulls(self, hm: Dict):
        keys = hm.keys()
        to_remove = []
        for k in keys:
            if hm[k] is None:
                to_remove.append(k)

        for k in to_remove:
            del hm[k]

        return hm

    def _state_to_tuple(self, state):
        # Convert to tuple used by sqlite
        return (
            state.get("timestamp"),
            state.get("api_timestamp"),
            state.get("track_id"),
            state.get("progress_ms"),
            state.get("duration_ms"),
            state.get("is_playing"),
            state.get("repeat_state"),
            state.get("shuffle_state"),
            self._safe_subget(state, "device", "id"),
            self._safe_subget(state, "device", "is_active"),
            self._safe_subget(state, "device", "volume_percent"),
            self._safe_subget(state, "device", "is_private_session"),
            self._safe_subget(state, "device", "type"),
            self._safe_subget(state, "device", "name"),
        )

    def delete_states(self, limit_timestamp=None):
        # Delete old states
        timestamp = (time.time() - 86400) * 1000
        if limit_timestamp is None:
            self.conn.execute("delete from player where timestamp < ?", (timestamp,))
        else:
            self.conn.execute("delete from player where timestamp < ? and timestamp >= ?",
                              (timestamp, limit_timestamp,))

        self.conn.commit()
        self.conn.execute("vacuum")
        self.conn.commit()

    def _safe_subget(self, item, k1, k2):
        if item is None:
            return None

        if item.get(k1) is None:
            return None

        return item.get(k1).get(k2)


def store():
    return Sqlite3Store()
