import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pymongo
import util
import datetime
import spotify
from dateutil.parser import parse


tracks = spotify.get_saved_tracks(spotify.get_credentials())
date = parse(tracks[-1]["added_at"])
date = date.replace(hour=0, minute=0, second=0, microsecond=0)

totals = [0]

for track in reversed(tracks):
    added = parse(track["added_at"])
    if added > date:
        while added > date:
            totals.append(0)
            date += datetime.timedelta(days=1)
            print(date)
        totals.append(1)
    else:
        totals[-1] += 1

print(totals)


print(len(tracks))

plt.plot(np.cumsum(totals))
plt.savefig("saved.png")
