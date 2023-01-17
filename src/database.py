from pymongo import MongoClient, DESCENDING
from pymongo.errors import PyMongoError
from config import MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER_ADDRESS
import logging

class MongoDatabase:
    def __init__(self):
        try:
            self.cluster = MongoClient(f'mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER_ADDRESS}')
            self.ohlcv_db = self.cluster["metrics"]["posts_db"]
            self.posts_db = self.cluster["metrics"]["ohlcv_db"]
            logging.info('Connected to MongoDB!')

        except PyMongoError as e:
            logging.error(f"Cannot connect to the database: {e}")
            exit(1)

    def posts(self):
        return self.posts_db

    def ohlcv(self):
        return self.ohlcv_db
    
    def add_post(self, post):
        self.posts_db.insert_one(post)
        

    def get_top_pairs_by_amount(self, amount: int = 100) -> list[str]:
        result = list(self.ohlcv_db.aggregate([
            {
                "$match": {
                    "granularity": "1h"
                }
            },
            {
                "$group": {
                    "_id": {
                        "pair": {"$toUpper": {"$concat": ["$pair_symbol", "-", "$pair_base"]}},
                    },
                    "volume_sum": {
                        "$sum": {
                            "$toDouble": "$volume"
                        }
                    }
                }
            },
            {
                "$sort": {
                    "volume_sum": DESCENDING
                }
            },
            {
                "$limit": amount
            }
        ]))

        if len(result) == 0:
            raise ValueError("No pairs found!")

        return [item['_id']['pair'] for item in result]


    def get_latest_posted_pairs(self, top_pairs: list[str]) -> list[tuple]:
        result = self.posts_db.aggregate([
            {
                "$match": {
                    "pair": {"$in": top_pairs}
                }
            },
            {
                "$group": {
                    "_id": "$pair",
                    "datetime": {
                        "$max": "$time"
                    }
                }
            },
            {
                "$sort": {
                    "datetime": DESCENDING
                }
            },
        ])
        return [(item["_id"], item['datetime']) for item in result if isinstance(item["_id"], str)]
    

    def get_market_venues(self, pair_to_post: str) -> list[dict]:
        pair_symbol = pair_to_post.split('-')[0].lower()
        pair_base = pair_to_post.split('-')[1].lower()
        result = list(self.ohlcv_db.aggregate([
            {
                "$match": {
                    "granularity": "1h",
                    "pair_symbol": pair_symbol,
                    "pair_base": pair_base
                }
            },
            {
                "$group": {
                    "_id": "$marketVenue",
                    "volume": {
                        "$max":{
                            "datetime": "$timestamp",
                            "value": {
                                "$toDouble": "$volume"
                            },
                        }
                    }
                }
            },
            {
                "$sort": {
                    "volume.value": pymongo.DESCENDING
                }
            }
        ]))

        return result


