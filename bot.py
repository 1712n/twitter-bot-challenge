from datetime import datetime
from typing import NamedTuple
from pprint import pprint

import pymongo

from database import MongoDatabase


class TwitterMarketCapBot:
    def __init__(self, db: MongoDatabase):
        self.db = db

    def get_top_pairs(self, amount: int = 100) -> set[str]:
        granularity = {"$match": {"granularity": "1h"}}
        data = {
            "$group": {
                "_id": {
                    "pair": {"$toUpper": {"$concat": ["$pair_symbol", "-", "$pair_base"]}},
                },
                "volume": {
                    "$sum": {"$toDouble": "$volume"}
                }
            }
        }
        sorting = {
            "$sort": {
                "volume": pymongo.DESCENDING
            }
        }
        amount = {"$limit": amount}
        result = self.db.ohlcv().aggregate([
            granularity,
            data,
            sorting,
            amount
        ])
        return {item['_id']['pair'] for item in result}

    def get_latest_posted_pairs(self, top_pairs: set[str]) -> set[str]:
        matching = {
            "$match": {
                "pair": {"$in": list(top_pairs)}
            }
        }
        latest = {
            "$group": {
                "_id": "$pair",
                "time": {"$max": "$time"}
            }
        }
        sort_latest = {
            "$sort": {
                "time": pymongo.DESCENDING
            }
        }

        result = self.db.posts().aggregate([
            matching,
            latest,
            sort_latest,
        ])
        return {item["_id"] for item in result if isinstance(item["_id"], str)}

    def get_not_posted_pairs(self, top_pairs: set[str], latest_posted_pairs: set[str]) -> set[str]:
        return top_pairs - latest_posted_pairs

