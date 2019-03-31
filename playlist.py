#!/usr/bin/env python3

from spotify import create_playlist, add_to_playlist, get_credentials, Credentials, get_profile, get_songs_in_playlist
import sys
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id")
    parser.add_argument("--overwrite", action="store_true",
                        help="Clear any items in the existing playlist before adding new songs")
    parser.add_argument("--dedup", action="store_true", help="Remove duplicates in playlist")

    args = parser.parse_args()
    creds = get_credentials()
    username = get_profile(creds)["id"]

    ids = []
    for line in sys.stdin:
        ids.append(line.replace("\n", "").replace("\r", ""))

    if not args.id:
        if len(sys.argv) == 1:
            playlist_name = "Untitled"
        else:
            playlist_name = sys.argv[1]

        p_id = create_playlist(username, playlist_name, False, False, "", creds)["id"]
        add_to_playlist(username, p_id, ids, creds)
    else:
        tracks = [] if args.overwrite else get_songs_in_playlist(username, args.id, creds)
        tracks = tracks + ids

        if args.dedup:
            # We want to delete items later on in the playlist and retain order
            deduped = []
            for track in tracks:
                if track not in deduped:
                    deduped.append(track)

            tracks = deduped

        # Now write new tracks
        # Overwrite playlist
        add_to_playlist(username, args.id, tracks, creds, replace=True)


if __name__ == '__main__':
    main()
