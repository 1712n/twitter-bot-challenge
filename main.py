import logging
import tweepy
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
import os
import time


def autoreconnect(func):
    """A decorator to handle AutoReconnect exceptions.

    Tries to reconnect 5 times with increasing wait times, then fails. Number of reconnects 
    can be changed via MONGODB_RECONNECT_ATTEMPTS environment variable.
    """

    def _autoreconnect(*args, **kwargs):
        max_attempts = os.environ.get("MONGODB_RECONNECT_ATTEMPTS", 5)
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except AutoReconnect:
                logging.warning("Connecting to the database failed. Trying to reconnect...")
                time.sleep(pow(2, attempt))
        return func(*args, **kwargs)

    return _autoreconnect


@autoreconnect
def connect_db():
    """Connects to MongoDB using credentials from environment vars.

    Returns:
    MongoClient: database client
    """

    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    address = os.environ["MONGODB_ADDRESS"]
    uri = f"mongodb+srv://{user}:{password}@{address}"

    client = MongoClient(uri)
    
    return client


def main():
    db_client = connect_db()


if __name__ == '__main__':
    main()

