import os
from typing import List
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

try:
    uri = f"mongodb+srv://{user}:{password}@{address}"
    client = MongoClient(uri)
    db = client.metrics
except Exception:
    print("Database connection failed")
    raise


def get_top_pairs_by_volume(limit: int = 100):
    top_pairs_by_volume = db.ohlcv_db.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "$toUpper": {
                            "$concat": ["$pair_symbol", "-", "$pair_base"]
                        }
                    },
                    "compound_volume": {
                        "$sum": {
                            "$convert": {
                                "input": "$volume",
                                "to": "double",
                                "onError": "Cannot convert input to double",
                                "onNull": "Input was null or empty",
                            }
                        }
                    },
                }
            },
            {"$sort": {"compound_volume": -1}},
            {"$limit": limit},
        ],
    )
    top_pairs_list = [pair["_id"] for pair in top_pairs_by_volume]
    return top_pairs_list


def get_pair_to_post(top_pairs: List):
    pair_to_post = list(db.posts_db.aggregate(
        [
            {"$match": {"pair": {"$in": top_pairs}}},
            {
                "$project": {
                    "pair": 1,
                    "timestamp": {
                        "$ifNull": ["$time", "$timestamp"]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$pair",
                    "latest post": {
                        "$min": "$timestamp"
                    }
                }
            },
            {"$sort": {"latest post": 1}},
            {"$limit": 1}
        ]
    ))[0]
    return pair_to_post["_id"]


def get_message_to_post(pair_to_post: str):
    pair_markets = list(db.ohlcv_db.aggregate(
        [
            {
                "$project": {
                    "pair": {
                        "$toUpper": {
                            "$concat": ["$pair_symbol", "-", "$pair_base"]
                        }
                    },
                    "marketVenue": 1,
                    "volume": 1
                }
            },
            {"$match": {"pair": pair_to_post}},
            {
                "$group": {
                    "_id": "$marketVenue",
                    "volume": {
                        "$sum": {
                            "$convert": {
                                "input": "$volume",
                                "to": "double",
                                "onError": "Cannot convert input to double",
                                "onNull": "Input was null or empty",
                            }
                        }
                    }
                }
            },
            {"$sort": {"volume": -1}}
        ]
    ))
    message_to_post = f"Top Market Venues for {pair_to_post}:\n"
    pair_compound_volume = sum(m["volume"] for m in pair_markets)
    for i, market in enumerate(pair_markets):
        if i == 5:
            break
        message_to_post += f"{market['_id'].capitalize()} {(market['volume']/pair_compound_volume)*100:.2f}%\n"
    if len(pair_markets) > 5:
        other_markets_volume = sum(m["volume"] for m in pair_markets[5:])
        message_to_post += f"Others {(other_markets_volume/pair_compound_volume)*100:.2f}%\n"
    return message_to_post


top_pairs = get_top_pairs_by_volume()
pair_to_post = get_pair_to_post(top_pairs)
message = get_message_to_post(pair_to_post)
print(message)
