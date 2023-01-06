import logging
from typing import List
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError
from datetime import datetime


def get_db_client(uri: str):
    logging.info("Connecting to Database..")
    try:
        client = MongoClient(uri)
    except PyMongoError as err:
        logging.error("Database connection failure: %s", err)
        raise
    logging.info("Сonnected to Database!")
    return client


def get_top_pairs_by_volume(
    db: Database, limit: int = 100, extra_condition={}
):
    logging.info("Getting pairs with top compound volume..")
    try:
        fetched_data = db.ohlcv_db.aggregate(
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
        logging.error("Failed to get data from database: %s", err)
        raise
    pairs_w_top_volume = [pair["_id"] for pair in fetched_data]
    logging.info("Top pairs are collected!")
    return pairs_w_top_volume


def get_pair_to_post(db: Database, pairs: List):
    logging.info("Selecting pairs with oldest timestamp..")
    try:
        fetched_data = db.posts_db.aggregate(
            [
                {"$unwind": {"path": "$pair"}},
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
                        "latest_post": {"$max": "$timestamp"}
                    }
                },
                {"$sort": {"latest_post": 1}},
                {"$limit": 5},
            ]
        )
    except PyMongoError as err:
        logging.error("Failed to get data from database: %s", err)
        raise
    pairs_w_oldest_timetamp = [pair["_id"] for pair in fetched_data]
    logging.info("Selecting pair with max compound volume among them..")
    max_volume_among_oldest = get_top_pairs_by_volume(
        db=db,
        limit=1,
        extra_condition={"_id": {"$in": pairs_w_oldest_timetamp}}
    )
    logging.info("Getting a pair to post..")
    pair_to_post = max_volume_among_oldest[0]
    return pair_to_post


def get_message_to_post(db: Database, pair: str):
    logging.info("Getting coresponding market vanues for selected pair..")
    try:
        fetched_data = list(
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
        logging.error("Failed to get data from database: %s", err)
        raise
    logging.info("Composing message to post..")
    message_components = [f"Top Market Venues for {pair}:", ]
    cmpd_volume = sum(market["volume"] for market in fetched_data)
    for i, mkt in enumerate(fetched_data):
        if i == 5:
            break
        message_components.append(
            f"{mkt['_id'].capitalize()} {(mkt['volume']/cmpd_volume)*100:.2f}%"
        )
    if len(fetched_data) > 5:
        other_markets_volume = sum(mkt["volume"] for mkt in fetched_data[5:])
        message_components.append(
            f"Others {(other_markets_volume/cmpd_volume)*100:.2f}%"
        )
    message_to_post = "\n".join(message_components)
    logging.info("Message is ready for posting!")
    return message_to_post


def get_origin_tweet_id(db: Database, pair: str):
    logging.info("Checking previous posts for given pair")
    try:
        fetched_data = list(db.posts_db.aggregate(
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
        logging.error("Failed to get data from database: %s", err)
        raise
    origin_tweet_id = None
    if fetched_data:
        logging.info(
            "%s was published earlier, the thread will continue..", pair
        )
        origin_tweet_id = fetched_data[0]["tweet_id"]
    return origin_tweet_id


def add_new_post_to_db(db: Database, pair: str, tweet_id: str, text: str):
    logging.info("Inserting new post to database..")
    try:
        inserted_document = db.posts_db.insert_one(
            {
                "pair": pair,
                "tweet_id": tweet_id,
                "text": text,
                "time": datetime.now(),
            }
        )
    except PyMongoError as err:
        logging.error("Failed to insert data in database: %s", err)
        raise
    return inserted_document.inserted_id
