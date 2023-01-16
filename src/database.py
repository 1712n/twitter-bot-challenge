from pymongo import MongoClient
from pymongo.errors import PyMongoError
from config import MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER_ADDRESS
import logging

class MongoDatabase:
    def __init__(self):
        try:
            self.cluster = MongoClient(f'mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER_ADDRESS}')
            logging.info('Connected to MongoDB Server.')

        except PyMongoError as e:
            logging.error(f"Cannot connect to the database: {e}")
            exit(1)

    def posts(self):
        self.cluster["metrics"].command('ping')
        return self.cluster["metrics"]["posts_db"]

    def ohlcv(self):
        return self.cluster["metrics"]["ohlcv_db"]


database = MongoDatabase()
a = database.posts()
print(a)