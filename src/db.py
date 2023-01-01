import os
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


def get_top_hundred_pairs(limit: int = 100):
    ohlcv_docs = db.ohlcv_db.aggregate(
        [
            {"$match": {}},
            {
                "$group": {
                    "_id": {"$concat": ["$pair_symbol", "-", "$pair_base"]},
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
    top_hundred_pairs = [x["_id"].upper() for x in ohlcv_docs]
    return top_hundred_pairs
