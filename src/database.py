import logging
from pprint import pprint

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER_ADDRESS


class MongoDatabase:
    def __init__(self):
        try:
            self.uri = f'mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER_ADDRESS}'
            self.client = MongoClient(self.uri)
            logging.info('Connected to MongoDB Server.')
        except PyMongoError as e:
            logging.error(f"Cannot connect to the database: {e}")
            exit(1)

    def ohlcv(self):
        return self.client["metrics"]["ohlcv_db"]

    def posts(self):
        return self.client["metrics"]["posts_db"]
