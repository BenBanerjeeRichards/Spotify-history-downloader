# Spotify history downloader

Continually download your spotify listening history. Gets around the 50 song history limit of the spotify API.

* Download the songs you listen to for > 30 seconds (this is your normal spotify listening history)
* Generate detailed events (sub second accuracy) for spotify including play, skip, seek, pause, repeat, shuffle and spotify connect

Also contains scripts for useful things:

* Import last three months of listening history from spotify data export (gdpr.py)
* Move songs from one playlist to another if not saved within 30 days of being added (scripts/sounds_good.py)

Coming soon:

* Ben's Big Data Analysis to tell you everything you already knew about your music taste  

## Environment variables 
Store your Spotify API credentials and Mongo db credentials in these 
environment variables. Get client id and secret from your spotify app on
the dev dashboard.

SPOTIFY_CLIENT_ID - the client id of your developer app

SPOTIFY_CLIENT_SECRET - client secret

SPOTIFY_REFRESH_TOKEN - obtain this using get_token.py (standalone script, run as `python3 get_token.py`) and follow the prompts

SPOTIFY_MONGO_USERNAME - mongo db username

SPOTIFY_MONGO_PASSWORD - mongo db password

Mongo db information is not needed if you have not set up credentials on mongodb. 
If you are hosting the project on a server then it is recommended that you 
configure the firewall to block access to your mongodb instance. 