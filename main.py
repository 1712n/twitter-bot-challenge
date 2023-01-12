import os

from dotenv import load_dotenv
from pymongo import MongoClient

import pprint

def get_top_pairs(pairs_col):
    pairs_total_vol_list = list(pairs_col.aggregate(
        [
            {
                "$group": {
                    "_id": {"$toUpper": {"$concat": ["$pair_symbol", "-", "$pair_base"]}},
                    "volume_sum": {"$sum": {"$toDouble": "$volume"}},
                }
            },
            {"$sort": {"volume_sum": -1}},
            {"$limit": 100},
        ],
    ))
    return pairs_total_vol_list


def get_posts_for_pairs(posts_col, pairs_list):
    posts_list = list(posts_col.aggregate(
            [
                {
                    # fast operation getting advantage of time_-1 index
                    '$sort': {
                        'time': -1
                    }
                }, {
                    '$group': {
                        '_id': {
                            # taking into account that some fields are arrays
                            '$cond': {
                                'if': {
                                    '$isArray': '$pair'
                                }, 
                                'then': {
                                    '$first': '$pair'
                                }, 
                                'else': '$pair'
                            }
                        },
                        # saving latest post
                        'lastPost': {
                            '$first': '$$ROOT'
                        }
                    }
                },
                # filtering posts for requested ones
                {"$match": {"_id": {"$in": pairs_list}}},
                {
                    '$sort': {
                        'lastPost.time': 1
                    }
                }
            ],
        )
    )
    return posts_list


load_dotenv()

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = MongoClient(uri)

ohlcv_col = client["metrics"]["ohlcv_db"]
posts_col = client["metrics"]["posts_db"]

top_100_pairs_vol = get_top_pairs(ohlcv_col)

top_pairs = [x['_id'] for x in top_100_pairs_vol]

corresponding_posts = get_posts_for_pairs(posts_col, top_pairs)
pprint.pprint(corresponding_posts)