from bottle import response, run, hook, route
import sys
from analysis.day import *


@route("/spotify/day")
def spotify():
    return get_stats(datetime.datetime.now())


@route("/spotify/day/<year>/<month>/<day>")
def spotify(year, month, day):
    return get_stats(datetime.datetime(int(year), int(month), int(day)))


@hook('after_request')
def enable_cors():
    """
    You need to add some headers to each request.
    Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'


def main():
    if len(sys.argv) != 2:
        print("Pass either DEV or PROD")
        return

    if sys.argv[1] == "DEV":
        run(host="localhost", port=8080)
    elif sys.argv[1] == "PROD":
        run(host="206.189.24.92", port=9876)
    else:
        print("Provide PROD or DEV")


if __name__ == "__main__": main()
