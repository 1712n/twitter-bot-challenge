import logging
import sys
from datetime import datetime, timedelta
from typing import Iterable, List, Set

import config
import pymongo

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
if config.DEBUG:
    logging.basicConfig(level=logging.DEBUG,format=log_format)
else:
    logging.basicConfig(level=logging.INFO,format=log_format)
    
# Connect to MongoDB
mongo_user = config.MONGODB_USER
mongo_pass = config.MONGODB_PASSWORD
mongo_cluster = config.MONGO_DB_ADDRESS

try:
    logging.info("Connecting to MongoDB...")

    client = pymongo.MongoClient(f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_cluster}")
    metrics = client.metrics
    ohlcv_db = metrics.ohlcv_db
    posts_db = metrics.posts_db

except pymongo.errors.ConnectionFailure:
    logging.error("Connection to MongoDB failed!")
except pymongo.errors.PyMongoError as e:
    logging.error("Error connecting to MongoDB: %s" %e)
    sys.exit(1)
else:
    logging.info("Connected to MongoDB!")


def mongodb_read_error_handler(func):
    """
        Decorator to handle MongoDB read errors
    """
    def wrapper(*args,**kwargs):
        try:
            return func(*args,**kwargs)
        except pymongo.errors.PyMongoError as e:
            logging.error("Error reading from MongoDB in %s:\n %s" %(func.__name__,e))
            sys.exit(1)
        except Exception as e:
            logging.error("Error in %s :\n %s" %(func.__name__,e))
            sys.exit(1)
    return wrapper

def mongodb_write_error_handler(func):
    """
        Decorator to handle MongoDB write errors
    """
    def wrapper(*args,**kwargs):
        try:
            return func(*args,**kwargs)
        except pymongo.errors.PyMongoError as e:
            logging.error("Error writing to MongoDB in %s:\n %s" %(func.__name__,e))
            sys.exit(1)
        except Exception as e:
            logging.error("Error in %s :\n %s" %(func.__name__,e))
            sys.exit(1)
    return wrapper


@mongodb_read_error_handler
def get_top_pairs(max_num:int=100,last_hours:int=1) -> List:
    """
        Function to return at most the top max_num distinct pairs by compound volume from ohlcv db in the last last_hours
    """
    logging.info("Getting top pairs by volume...")
    ohlcv_documents = ohlcv_db.aggregate(
            [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.utcnow() - timedelta(hours=last_hours)
                        },
                    }
                },
                {
                    "$group": {
                        "_id": {"$concat": ["$pair_symbol", "-", "$pair_base"]},
                        "volume_sum": {"$sum": {"$toDouble": "$volume"}},
                    }
                },

                {"$sort": {"volume_sum": pymongo.DESCENDING}},
                {"$limit": max_num},
            ],
        )
    top_pairs_by_volume = map(lambda x: str.upper(x['_id']),ohlcv_documents)

    return top_pairs_by_volume


@mongodb_read_error_handler
def get_latest_posted_pairs(top_pairs:List,max_num:int=5) -> Set:
    """
        Function to return at most the top max_num distinct pairs by latest post time from posts db
    """
    posted_pairs_among_top = posts_db.find({'pair' : {'$in':top_pairs}}).sort('time', pymongo.DESCENDING).distinct('pair')

    latest_posted_pairs_set = set()
    latest_posted_pair = list()
    # At most top max_num distinct latest posted pairs among top pairs
    for pair in posted_pairs_among_top:
        if pair not in latest_posted_pairs_set:
            latest_posted_pairs_set.add(pair)
            latest_posted_pair.append(pair)
        if len(latest_posted_pairs_set) == max_num:
            break
    return latest_posted_pairs_set


@mongodb_read_error_handler
def get_latest_posted_pair() -> str:
    """
        Function to return the latest posted pair
    """
    document = posts_db.find({}).sort('time', pymongo.DESCENDING).limit(1)
    return document[0].get('pair')

def get_pair_to_post(top_pairs:List,last_posted:str=None,latest_posted_pairs:Iterable = None) -> str:
    """
        Function to return the pair to post
    """
    logging.info("Getting pair to post...")
    if (latest_posted_pairs is None) or len(latest_posted_pairs) == 0:
        # the first pair by volume
        return top_pairs[0]
    if last_posted is None:
        last_posted = get_latest_posted_pair()
    for pair in top_pairs:
        if pair in latest_posted_pairs and not pair == last_posted:
            # Choose the first pair by volume among those latest posted pairs
            return pair
    else:
        # Or the oldest posted amoung top pairs
        return latest_posted_pairs[-1]
if __name__ == "__main__":

    top = ['A','B','C']
    assert('A' == get_pair_to_post(top,latest_posted_pairs={}))
    assert('C' == get_pair_to_post(top,last_posted = 'A',latest_posted_pairs=['A','C']))
    print(ohlcv_db.count_documents({}))
    print(posts_db.count_documents({}))