import os
import requests
import pymongo
import logging
import time

class DownloadException(Exception):
    pass

class Credentials:
	def __init__(self, client_id, client_secret, refresh):
		self.client_id = client_id
		self.client_secret = client_secret
		self.refresh_token =  refresh

	def __str__(self):
		return "client_id: {}\nclient_secret:{}\nrefresh_token:{}".format(self.client_id, self.client_secret, self.refresh_token)

def insert(tracks):
	logging.info("Retrieved 50 songs from spotify")

	client = pymongo.MongoClient("localhost", 27017)	# Same in prod
	spotify = client.spotify

	# Get last track listened to stored in db
	# This is to ensure we don't duplicate items in database
	latest_track = spotify.tracks.find_one({},sort=[("played_at", pymongo.DESCENDING)])
	if latest_track:
		tracks = remove_tracks_before_inc(tracks, latest_track)
		logging.info("Got {} tracks to insert".format(len(tracks)))
	else:
		logging.info("Nothing played since last download, doing nothing...")
	if len(tracks) > 0:
		spotify.tracks.insert_many(tracks)
	client.close()	# TODO can we use with..as clause?

def get_access_token(credentials, attempts=0):
	if attempts > 10:
		logging.error("Failed to get access token after 10 attempts")
		raise DownloadException("Failed to get access token")

	form_data = {
		"grant_type": "refresh_token", 
		"refresh_token": credentials.refresh_token,
		"client_id": credentials.client_id,
		"client_secret": credentials.client_secret
		}

	try:
		response = requests.post("https://accounts.spotify.com/api/token", data=form_data)
		response.raise_for_status()
	except Exception as e:
		logging.exception("Failed to get access token, retrying")
		time.sleep(pow(attempts, 1.5))
		return get_access_token(credentials, attempts + 1)

	logging.info("Got spotify access token")
	return response.json()["access_token"]

def get_credentials():
	return Credentials(os.environ["SPOTIFY_CLIENT_ID"], os.environ["SPOTIFY_CLIENT_SECRET"], os.environ["SPOTIFY_REFRESH_TOKEN"])

def get_recently_played(creds, access_token):
	res = requests.get("https://api.spotify.com/v1/me/player/recently-played", params={"limit": 50}, headers={"Authorization": "Bearer {}".format(access_token)})
	return res.json()

def remove_tracks_before_inc(tracks, stop_at_track):
	new = []
	for track in tracks:
		if track["played_at"] == stop_at_track["played_at"]:
			break
		new.append(track)

	return new

def pretty_recently_played_json(tracks):
	s = ""
	for item in tracks:
		s += "{} - {}\n".format(item["track"]["artists"][0]["name"], item["track"]["name"])
	return s
#chcp 65001

def main():
	try:
		logging.basicConfig(filename='output.log',level=logging.DEBUG)
		creds = get_credentials()
		token = get_access_token(creds)
		j = get_recently_played(creds, token)
		insert(j["items"])
	except Exception as e:
		print("SPOTIY SCRAPE FAILED")
		print(e)

if __name__=="__main__":main()