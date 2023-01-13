import os
import logging

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

from src.db_querries import *


logger = logging.getLogger(__name__)
logging.basicConfig(format='%(name)s: %(levelname)s: %(message)s', level=logging.DEBUG)


def find_pair_with_largest_vol(ordered_pairs_vol_dict, pairs_set):
    for pair in ordered_pairs_vol_dict.keys():
        if pair in pairs_set:
            return {pair, ordered_pairs_vol_dict[pair]}
    raise Exception("Pair intendened to de in orederd dictionary, containing top 100 pairs")


def main():
    load_dotenv()

    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    address = os.environ["MONGO_DB_ADDRESS"]

    uri = f"mongodb+srv://{user}:{password}@{address}"
    
    client = MongoClient(uri)
    try:
        # The ping command is cheap and does not require auth.
        client["metrics"].command('ping')
    except PyMongoError:
        logging.error("Database connection failure:")
        raise


    ohlcv_col = client["metrics"]["ohlcv_db"]
    posts_col = client["metrics"]["posts_db"]

    # TODO: handle possible exeptions
    pairs_vol_dict = get_top_pairs(ohlcv_col)
    # TODO: handle possible exeptions
    cor_posts_dict = get_posts_for_pairs(posts_col, list(pairs_vol_dict.keys()))


    db_pairs_set = set(pairs_vol_dict.keys())
    posts_pairs_set = set(cor_posts_dict.keys())
    # pairs wich lack posts at all
    diff = db_pairs_set.difference(posts_pairs_set)

    # TODO: handle possible exeptions
    top_unpublished_pair = find_pair_with_largest_vol(pairs_vol_dict, diff)
    print(top_unpublished_pair)

    # print("pairs set - posts set")
    # print(len(db_pairs_set.difference(posts_pairs_set)))
    # print("posts set - pairs set")
    # print(len(posts_pairs_set.difference(db_pairs_set)))
    # print("intersection")
    # print(len(db_pairs_set.intersection(posts_pairs_set)))


if __name__ == "__main__":
    main()     