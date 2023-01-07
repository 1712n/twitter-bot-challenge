import pymongo
import os
import sys
import logging

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]


class Database:
    def __init__(self):
        try:
            self.uri = f"mongodb+srv://{user}:{password}@{address}"
            self.client = pymongo.MongoClient(self.uri)
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Cannot connect to the database: {e}")
            sys.exit(1)

    def ohlcv(self):
        return self.client["metrics"]["ohlcv_db"]

    def posts(self):
        return self.client["metrics"]["posts_db"]