import logging
import sys

import pymongo
from pymongo.errors import PyMongoError

import os


class Database:
    def __init__(self):
        logging.info("Opening connection to DB")

        user = os.environ["MONGODB_USER"]
        password = os.environ["MONGODB_PASSWORD"]
        address = os.environ["MONGODB_ADDRESS"]
        uri = f"mongodb+srv://{user}:{password}@{address}"

        try:
            self.client = pymongo.MongoClient(uri)
        except PyMongoError as err:
            logging.error(f"Failed to connect to DB: {err}")
            sys.exit(1)

    def ohlcv(self):
        return self.client["metrics"]["ohlcv_db"]

    def posts(self):
        return self.client["metrics"]["posts_db"]
