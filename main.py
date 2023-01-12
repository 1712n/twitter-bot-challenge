import os
from dotenv import load_dotenv
from pymongo import MongoClient


def get_top_pairs(pairs_col):
    # aggregation pipeline
    pairs_total_vol = list(pairs_col.aggregate(
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
    return pairs_total_vol


load_dotenv()

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = MongoClient(uri)

ohlcv_col = client["metrics"]["ohlcv_db"]
posts_col = client["metrics"]["posts_db"]

top_100_pairs = get_top_pairs(ohlcv_col)
print(top_100_pairs)