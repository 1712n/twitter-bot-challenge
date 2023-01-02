import pymongo
import os

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = pymongo.MongoClient(uri)

if __name__ == '__main__':
    ############### part 1 of the algorithm

    select_1h_granularity = {"$match": {"granularity": "1h"}}
    limit_data = {
        "$group": {
            "_id": {
                "pair": {"$toUpper": {"$concat": ["$pair_symbol", "-", "$pair_base"]}},
            },
            "volume": {
                "$sum": {"$toDouble": "$volume"}
            }
        }
    }
    sort_data = {
        "$sort": {
            "volume": pymongo.DESCENDING
        }
    }
    top_100 = {"$limit": 100}

    res = client["metrics"]["ohlcv_db"].aggregate([
        select_1h_granularity,
        limit_data,
        sort_data,
        top_100
    ])

    top_pairs = [(x["_id"]["pair"], x["volume"]) for x in res]

    ####################### part 2 of the algorithm

    match_pairs = {
        "$match": {
            "pair": {"$in": [x[0] for x in top_pairs]}
        }
    }
    find_latest = {
        "$group": {
            "_id": "$pair",
            "time": {
                "$max": "$time"
            }
        }
    }
    sort_latest = {
        "$sort": {
            "time": pymongo.DESCENDING
        }
    }

    res = client["metrics"]["posts_db"].aggregate([
        match_pairs,
        find_latest,
        sort_latest,
    ])

    latest_pairs = [(x["_id"], x["time"]) for x in res]

