from pymongo import MongoClient
from tweepy import API as TwitterAPI

from tcbot.logging import logger
from tcbot.utils import get_percentage, get_timestamp, sort_dict


class TCBot:
    def __init__(self, mongodb_client: MongoClient, twitter_api: TwitterAPI, db_name = "metrics"):
        logger.debug("Initializing TCBot...")

        self.database = mongodb_client.get_database(db_name)
        self.twitter_api = twitter_api


    def get_pair_to_post(self, granularity = '1h'):
        logger.debug("Using database '{}'...", self.database.name)

        logger.debug("Getting pair to post through aggregation pipelines...")
        result = self.database.ohlcv_db.aggregate([
            {
                "$match": {
                    "granularity": granularity
                }
            },
            {
                "$group": {
                    "_id": {
                        "pair_symbol": "$pair_symbol",
                        "pair_base": "$pair_base",
                        "market_venue": "$marketVenue"
                    },
                    "total_volume": {
                        "$sum": {
                            "$toDouble": "$volume"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "$concat": [
                            "$_id.pair_symbol",
                            "-",
                            "$_id.pair_base"
                        ]
                    },
                    "total_volume": {
                        "$sum": "$total_volume"
                    },
                    "markets": {
                        "$push": {
                            "k": "$_id.market_venue",
                            "v": "$total_volume"
                        }
                    }
                }
            },
            {
                "$project": {
                    "total_volume": 1,
                    "markets": {
                        "$arrayToObject": "$markets"
                    }
                }
            },
            {
                "$sort": {
                    "total_volume": -1
                }
            },
            {
                "$limit": 100
            },
            {
                "$addFields": {
                    "_id": {
                        "$toUpper": "$_id"
                    }
                }
            },
            {
                "$lookup": {
                    "from": "posts_db",
                    "localField": "_id",
                    "foreignField": "pair",
                    "let": {
                        "pair": "$_id"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$pair",
                                        "$$pair"
                                    ]
                                }
                            }
                        },
                        {
                            "$sort": {
                                "timestamp": -1
                            }
                        },
                        {
                            "$limit": 1
                        }
                    ],
                    "as": "last_post"
                }
            },
            {
                "$unwind": {
                    "path": "$last_post",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$sort": {
                    "total_volume": -1,
                    "last_post.timestamp": 1
                }
            },
            {
                "$limit": 1
            }
        ])

        result = list(result)

        if len(result) > 0:
            return result[0]
        else:
            return None


    def get_message_to_post(self, pair_to_post):
        total_volume = pair_to_post['total_volume']

        markets = pair_to_post['markets']
        markets = sort_dict(markets, by_key=False, reverse=True)

        message_to_post = f"Top Market Venues for {pair_to_post['_id']}"

        for name, volume in markets.items():
            market = name.capitalize()
            percentage = get_percentage(volume, total_volume)
            message_to_post += f"\n{market} {percentage}%"

        return message_to_post


    def post_tweet(self, message: str):
        logger.debug("Posting the following message:")

        for line in message.splitlines():
            logger.debug("\t\033[3m{}\033[0m", line)

        thread_id = get_timestamp()
        logger.debug("Posted to {}", thread_id)

        return thread_id


    def start(self):
        logger.info("TCBot has started!")

        pair_to_post = self.get_pair_to_post()

        if pair_to_post:
            logger.info("The pair '{}' needs to be posted!", pair_to_post['_id'])
            message_to_post = self.get_message_to_post(pair_to_post)
            thread_id = self.post_tweet(message_to_post)
        else:
            logger.info("No pair to post found :(")

        logger.info("TCBot is exiting. Good-bye!")

        self.database.client.close()
