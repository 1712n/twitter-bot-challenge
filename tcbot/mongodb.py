import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database

from tcbot.logging import logger


def get_mongodb_client():
    load_dotenv()

    address = os.environ["MONGO_DB_ADDRESS"]
    username = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]

    uri = f"mongodb+srv://{username}:{password}@{address}"

    return MongoClient(uri)


def get_pair_to_post(database: Database):
    logger.debug("Processing documents through aggregation pipelines...")

    return database.ohlcv_db.aggregate([
        {
            "$match": {
                "granularity": "1h"
            }
        },
        {
            "$group": {
                "_id": {
                    "pair_symbol": "$pair_symbol",
                    "pair_base": "$pair_base",
                    "market": "$marketVenue"
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
                        "name": "$_id.market",
                        "volume": "$total_volume"
                    }
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
