import logging

from pymongo import MongoClient
from pymongo.errors import PyMongoError


class MongoDbClient:
    def __init__(self, user: str, password: str, cluster_address: str):
        uri = f"mongodb+srv://{user}:{password}@{cluster_address}"
        try:
            self._client = MongoClient(uri)
        except PyMongoError as error:
            logging.error(f"Can not connect to MongoDB: {error}")
            raise
        logging.info("Connected to MongoDB cluster")

    def get_collection(self, collection_name: str):
        try:
            collection = self._client["metrics"][collection_name]
        except PyMongoError as error:
            logging.error(f"Can not get collection {collection_name} from 'metrics' db: {error}")
            raise
        return collection

