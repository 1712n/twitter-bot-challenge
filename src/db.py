import os
import logging
from typing import List
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]
uri = f"mongodb+srv://{user}:{password}@{address}"

try:
    client = MongoClient(uri)
    db = client.metrics
except PyMongoError as err:
    logging.error("Database connection failure: %s", err)


def get_top_pairs_by_volume(limit: int = 100, extra_condition={}):
    logging.info("Getting pairs with top compound volume.")
    try:
        query_result = db.ohlcv_db.aggregate(
            [
                {
                    "$group": {
                        "_id": {
                            "$toUpper": {"$concat": [
                                "$pair_symbol", "-", "$pair_base"
                            ]}
                        },
                        "compound_volume": {
                            "$sum": {
                                "$convert": {
                                    "input": "$volume",
                                    "to": "double",
                                    "onError": "Cannot convert to double",
                                    "onNull": "Input was null or empty",
                                }
                            }
                        },
                    }
                },
                {"$match": extra_condition},
                {"$sort": {"compound_volume": -1}},
                {"$limit": limit},
            ],
        )
    except PyMongoError as err:
        logging.error("Faild to get data from database: %s", err)
    pairs_w_top_volume = [pair["_id"] for pair in query_result]
    logging.info("Top pairs are collected")
    return pairs_w_top_volume


def get_pair_to_post(pairs: List):
    logging.info("Selecting pairs with oldest timestamp")
    try:
        query_result = db.posts_db.aggregate(
            [
                {"$match": {"pair": {"$in": pairs}}},
                {
                    "$project": {
                        "pair": 1,
                        "timestamp": {"$ifNull": ["$time", "$timestamp"]},
                    }
                },
                {
                    "$group": {
                        "_id": "$pair",
                        "latest post": {"$min": "$timestamp"}
                    }
                },
                {"$sort": {"latest post": 1}},
                {"$limit": 5},
            ]
        )
    except PyMongoError as err:
        logging.error("Faild to get data from database: %s", err)
    pairs_w_oldest_timetamp = [pair["_id"] for pair in query_result]
    logging.info("Selecting pair with max compound volume among them")
    max_volume_among_oldest = get_top_pairs_by_volume(
        limit=1, extra_condition={"_id": {"$in": pairs_w_oldest_timetamp}}
    )
    logging.info("Getting a pair to post")
    pair_to_post = max_volume_among_oldest[0]
    return pair_to_post


def get_message_to_post(pair: str):
    logging.info("Getting coresponding market vanues for selected pair")
    try:
        pair_markets = list(
            db.ohlcv_db.aggregate(
                [
                    {
                        "$project": {
                            "pair": {
                                "$toUpper": {
                                    "$concat": [
                                        "$pair_symbol", "-", "$pair_base"
                                    ]
                                }
                            },
                            "marketVenue": 1,
                            "volume": 1,
                        }
                    },
                    {"$match": {"pair": pair}},
                    {
                        "$group": {
                            "_id": "$marketVenue",
                            "volume": {
                                "$sum": {
                                    "$convert": {
                                        "input": "$volume",
                                        "to": "double",
                                        "onError": "Cannot convert to double",
                                        "onNull": "Input was null or empty",
                                    }
                                }
                            },
                        }
                    },
                    {"$sort": {"volume": -1}},
                ]
            )
        )
    except PyMongoError as err:
        logging.error("Faild to get data from database: %s", err)
    logging.info("Composing message to post")
    message_components = [f"Top Market Venues for {pair}:", ]
    cmpd_volume = sum(market["volume"] for market in pair_markets)
    for i, mkt in enumerate(pair_markets):
        if i == 5:
            break
        message_components.append(
            f"{mkt['_id'].capitalize()} {(mkt['volume']/cmpd_volume)*100:.2f}%"
        )
    if len(pair_markets) > 5:
        other_markets_volume = sum(mkt["volume"] for mkt in pair_markets[5:])
        message_components.append(
            f"Others {(other_markets_volume/cmpd_volume)*100:.2f}%"
        )
    message_to_post = "\n".join(message_components)
    logging.info("Message is ready for posting")
    return message_to_post


def get_origin_tweet_id(pair: str):
    logging.info("Checking previous posts for given pair")
    try:
        query_result = list(db.posts_db.aggregate(
            [
                {"$match": {"pair": pair}},
                {
                    "$project": {
                        "pair": 1,
                        "tweet_id": {"$ifNull": ["$tweet_id", None]}
                    }
                },
                {"$limit": 1}
            ]
        ))
    except PyMongoError as err:
        logging.error("Faild to get data from database: %s", err)
    origin_tweet_id = None
    if query_result:
        logging.info(
            "%s was published earlier, the thread will continue", pair
        )
        origin_tweet_id = query_result[0]["tweet_id"]
    return origin_tweet_id
