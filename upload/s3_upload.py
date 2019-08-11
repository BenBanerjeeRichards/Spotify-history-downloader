import boto3
import os
from util import get_path
import hashlib
import logging

BUCKET_NAME = "spotify-data-bbr"
OBJECT_NAME = "main.sql"


def dump_checksum():
    if os.path.exists("main.sql"):
        return hashlib.md5(open("main.sql", "rb").read()).hexdigest()


def upload():
    os.chdir(get_path("upload"))
    prev_hash_checksum = dump_checksum()

    # I know should use subprocess
    os.system("rm main.sqlite")
    os.system("cp ../main.sqlite main.sqlite")
    os.system('sqlite3 main.sqlite ".dump" > main.sql')

    # Check if database has changed
    checksum = dump_checksum()
    if dump_checksum() != prev_hash_checksum:
        logging.info("Database changed: dump checksum changed from {} to {}. Uploading to s3".format(prev_hash_checksum,
                                                                                                     checksum))
        s3 = boto3.resource('s3')
        obj = s3.Object(BUCKET_NAME, OBJECT_NAME)
        obj.put(Body=open("main.sql", "rb").read())
    else:
        logging.info("Dump not changed with checksum={}".format(checksum))
