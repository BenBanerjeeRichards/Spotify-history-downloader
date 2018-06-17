import datetime


# Compare to second accuracy (never need any better in this application)
def datetimes_equal(dt1: datetime.datetime, dt2: datetime.datetime) -> bool:
    return dt1.year == dt2.year \
           and dt1.month == dt2.month \
           and dt1.day == dt2.day \
           and dt1.hour == dt2.hour \
           and dt1.minute == dt2.minute \
           and dt1.second == dt2.second
