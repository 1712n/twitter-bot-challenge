import datetime
import logging

import pymongo
from pymongo.errors import PyMongoError
from tweepy import TweepyException

from database import MongoDatabase
from twitter import Twitter


class MarketCapBot:
    def __init__(self, db: MongoDatabase, twitter: Twitter):
        self.twitter = twitter
        self.ohlcv_db = db.ohlcv()
        self.posts_db = db.posts()
        logging.info('Bot started...')

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
                    "volume_sum": pymongo.DESCENDING
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
                    "time": {
                        "$max": "$time"
                    }
                }
            },
            {
                "$sort": {
                    "time": pymongo.DESCENDING
                }
            },
        ])
        return [(item["_id"], item['time']) for item in result if isinstance(item["_id"], str)]

    

    def run(self):
        try:
            top_pairs = self.get_top_pairs_by_amount()
            latest_posted_pairs = self.get_latest_posted_pairs(top_pairs)

            print(latest_posted_pairs)

        except (TweepyException, PyMongoError) as e:
            logging.error(f'Cannot Run a bot because of: {e}')
            exit(1)
