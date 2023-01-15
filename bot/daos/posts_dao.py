import logging
from datetime import datetime
from typing import List

from pymongo.errors import PyMongoError

from bot.mongo_db_client import MongoDbClient


class PostsDao:
    def __init__(self, mongodb_client: MongoDbClient):
        self._db = mongodb_client.get_collection("posts_db")

    def latest_posted_pairs(self, pairs: List[str]) -> List[str]:
        pipeline = [
            {
                '$match': {
                    'pair': {
                        '$not': {
                            '$type': 'array'
                        },
                        '$in': pairs
                    }
                }
            }, {
                '$group': {
                    '_id': '$pair',
                    'time': {
                        '$max': '$time'
                    }
                }
            }, {
                '$sort': {
                    'time': 1
                }
            }
        ]
        try:
            return [p["_id"] for p in self._db.aggregate(pipeline)]
        except PyMongoError as error:
            logging.error(f"Can not get latest posted pairs. Pipeline: {pipeline}. Error: {error}")
            raise

    def save_tweet(self, pair: str, message: str) -> str:
        try:
            result = self._db.insert_one({
                "time": datetime.now(),
                "tweet": message,
                "pair": pair
            })
        except PyMongoError as error:
            logging.error(f"Can not save tweet. Error: {error}")
            raise
        return str(result.inserted_id)
