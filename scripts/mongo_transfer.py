from db.db import DbStore
import util


def import_from_mongo():
    db = DbStore()

    i = 0
    for track in util.get_spotify_db().tracks.find():
        db.add_play_from_mongo(track)
        i += 1

        if i % 100 == 0:
            print("Added {}".format(i))


def import_context_from_mongo():
    db = DbStore()
    for i, track in enumerate(util.get_spotify_db().tracks.find()):
        dt = track["played_at"].isoformat()
        context = track.get("context")
        if context is not None and "uri" in context:
            db.add_context(dt, context["uri"])

        if i % 100 == 0:
            print("Added {}".format(i))

    db.conn.commit()


def move_events():
    db = DbStore()
    total = util.get_spotify_db().events.find().count()
    for i, event in enumerate(util.get_spotify_db().events.find()):
        if i % 100 == 0:
            print("Moving events {}%".format(util.percent(i, total)))

        db.add_event(event)
    db.commit()

