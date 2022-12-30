import pymongo
import os

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = pymongo.MongoClient(uri)

if __name__ == '__main__':
    select_1h_granularity = {"$match": {"granularity": "1h"}}
    limit_data = {
        "$group": {
            "_id": {
                "pair": {"$concat": ["$pair_symbol", "-", "$pair_base"]},
                "marketVenue": "$marketVenue"
            },
            "volume": {
                "$sum": {"$toDouble": "$volume"}
            }
        }
    }
    debug_limit = {"$limit": 100}


    res = client["metrics"]["ohlcv_db"].aggregate([
        select_1h_granularity,
        limit_data,
        debug_limit
    ])

    for x in res:
        print(x)