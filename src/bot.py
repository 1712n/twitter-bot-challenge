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
                    "datetime": {
                        "$max": "$time"
                    }
                }
            },
            {
                "$sort": {
                    "datetime": pymongo.DESCENDING
                }
            },
        ])
        return [(item["_id"], item['datetime']) for item in result if isinstance(item["_id"], str)]


    def get_pair_to_post(self, top_pairs: list[str], latest_posted_pairs: list[tuple]) -> str:
        not_posted_pairs = [pair for pair, time in latest_posted_pairs if pair not in top_pairs]
        if len(not_posted_pairs) == 0:
            return sorted(latest_posted_pairs, key=lambda x: x[1])[0][0]
        return not_posted_pairs.pop()

    
    def get_pair_market_venues(self, pair_to_post: str) -> list[dict]:
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
                        "$max": {
                            "datetime": "$timestamp",
                            "value": {"$toDouble": "$volume"},
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

    
    def get_message_to_post(self, pair_to_post: str, pair_market_venues: list[dict]) -> str:
        message_to_post = f"Top Market Venues for {pair_to_post}:\n"

        total_volume = sum(item['volume']['value'] for item in pair_market_venues)

        for item in pair_market_venues[:5]:
            message_to_post += f"{item['_id'].capitalize()} {round(item['volume']['value'] / total_volume * 100, 2)}%\n"
        return message_to_post


    def run(self):
        try:
            top_pairs = self.get_top_pairs_by_amount()
            latest_posted_pairs = self.get_latest_posted_pairs(top_pairs)
            pair_to_post = self.get_pair_to_post(top_pairs, latest_posted_pairs)
            pair_market_venues = self.get_pair_market_venues(pair_to_post)
            message_to_post = self.get_message_to_post(pair_to_post, pair_market_venues)

            print(message_to_post)

        except (TweepyException, PyMongoError) as e:
            logging.error(f'Cannot Run a bot because of: {e}')
            exit(1)
