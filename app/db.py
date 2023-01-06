import logging
import sys
from datetime import datetime, timedelta
from typing import Iterable, List

import config
import pymongo


# Connect to MongoDB
def get_mongo_client():
    """
        Method to return a MongoDB client
    """
    mongo_user = config.MONGODB_USER
    mongo_pass = config.MONGODB_PASSWORD
    mongo_cluster = config.MONGO_DB_ADDRESS

    try:
        logging.info("Connecting to MongoDB...")
        client = pymongo.MongoClient(
            f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_cluster}")
        return client.metrics
    except pymongo.errors.ConnectionFailure:
        logging.error("Connection to MongoDB failed!")
    except pymongo.errors.PyMongoError as e:
        logging.error("Error connecting to MongoDB: %s" % e)
        sys.exit(1)


def get_mongo_collection(db_name: str) -> pymongo.collection.Collection:
    """
        Method to return a MongoDB Collection
    """
    client = get_mongo_client()
    db = client[db_name]
    return db


def mongodb_read_error_handler(func):
    """
        Decorator to handle MongoDB read errors
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except pymongo.errors.PyMongoError as e:
            logging.error("Error reading from MongoDB in %s:\n %s" %
                          (func.__name__, e))
            sys.exit(1)
        except Exception as e:
            logging.error("Error in %s :\n %s" % (func.__name__, e))
            sys.exit(1)

    return wrapper


def mongodb_write_error_handler(func):
    """
        Decorator to handle MongoDB write errors
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except pymongo.errors.PyMongoError as e:
            logging.error("Error writing to MongoDB in %s:\n %s" %
                          (func.__name__, e))
            sys.exit(1)
        except Exception as e:
            logging.error("Error in %s :\n %s" % (func.__name__, e))
            sys.exit(1)

    return wrapper


@mongodb_read_error_handler
def get_top_pairs(ohlcv_db: pymongo.collection.Collection,
                  max_num: int = 100,
                  last_hours: int = 1) -> List:
    """
        Function to return at most the top max_num distinct pairs by compound volume from ohlcv db in the last last_hours
    """
    logging.info("Getting top pairs by volume...")
    ohlcv_documents = ohlcv_db.aggregate([
        {
            "$match": {
                "timestamp": {
                    "$gte": datetime.utcnow() - timedelta(hours=last_hours)
                },
            }
        },
        {
            "$group": {
                "_id": {
                    "$concat": ["$pair_symbol", "-", "$pair_base"]
                },
                "volume_sum": {
                    "$sum": {
                        "$toDouble": "$volume"
                    }
                },
            }
        },
        {
            "$sort": {
                "volume_sum": pymongo.DESCENDING
            }
        },
        {
            "$limit": max_num
        },
    ], )
    top_pairs_by_volume = [x['_id'].upper() for x in ohlcv_documents]

    return top_pairs_by_volume


@mongodb_read_error_handler
def get_latest_posted_pairs(posts_db: pymongo.collection.Collection,
                            top_pairs: List) -> List:
    """
        Function to return latest distinct pairs posted among the top pairs by volume sorted from latest to oldest
    """

    logging.info("Getting latest posted pairs...")
    posts_documents = posts_db.aggregate([
        {
            "$match": {
                "pair": {
                    "$in": top_pairs
                }
            }
        },
        {
            "$group": {
                "_id": "$pair",
                "time": {
                    "$max": "$time"
                },
            }
        },
        {
            "$sort": {
                "time": pymongo.DESCENDING
            }
        },
    ], )

    latest_posted_pairs = [
        x['_id'] for x in posts_documents if not isinstance(x['_id'], list)
    ]

    return latest_posted_pairs


def get_pair_to_post(top_pairs: List,
                     latest_posted_pairs: Iterable = None) -> str:
    """
        Function to return the pair to post
    """
    logging.info("Getting pair to post...")
    if (latest_posted_pairs is None) or len(latest_posted_pairs) == 0:
        # If there are no latest posted pairs among top pairs, return the first pair by volume
        return top_pairs[0]
    elif len(latest_posted_pairs) == len(top_pairs):
        # If all top pairs have been posted, return the oldest posted pair
        return latest_posted_pairs[-1]
    else:
        # If there are posted pairs among top pairs,
        # return the first pair by volume among them
        # if this pair is not the last posted one (to avoid posting the same pair twice in a row)
        latest_posted_pairs_set = set(latest_posted_pairs)
        for pair in top_pairs:
            # yapf: disable
            if pair in latest_posted_pairs_set and pair != latest_posted_pairs[0]:
                return pair
            # yapf: enable


if __name__ == "__main__":

    # top = ['A','B','C']
    # assert('A' == get_pair_to_post(top,latest_posted_pairs={}))
    # assert('C' == get_pair_to_post(top,last_posted = 'A',latest_posted_pairs=['A','C']))
    ohlcv_db = get_mongo_collection('ohlcv_db')
    posts_db = get_mongo_collection('posts_db')
    top_pairs = get_top_pairs(ohlcv_db)
    print(top_pairs)
    latest_posted_pairs = get_latest_posted_pairs(top_pairs=top_pairs,
                                                  posts_db=posts_db)
    print(latest_posted_pairs)
    pair_to_post = get_pair_to_post(top_pairs=top_pairs,
                                    latest_posted_pairs=latest_posted_pairs)
    print(pair_to_post)
