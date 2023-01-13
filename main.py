import os
import logging

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

import src.db_querries as querry


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(name)s: %(levelname)s: %(message)s', level=logging.DEBUG)


def find_pair_with_largest_vol(ordered_pairs_vol_dict, pairs_set):
    for pair in ordered_pairs_vol_dict.keys():
        if pair in pairs_set:
            return pair, ordered_pairs_vol_dict[pair]
    raise Exception(
        "Pair intendened to de in orederd dictionary, containing top 100 pairs")


def compose_message(pair: str, total_vov: float, pair_market_stats: OrderedDict):
    message = f"Top Market Venues for {pair}:\n"
    remains = 100
    for market in pair_market_stats.keys():
        market_vol_percens = pair_market_stats[market] / total_vov * 100
        remains -= market_vol_percens

        # forming a message
        message += f"{market.title()} {market_vol_percens:.2f} \n"

    if remains >= 0.01:
        message += f"Others {remains:.2f} \n"

    return message


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
    pairs_vol_dict = querry.get_top_pairs(ohlcv_col)
    # TODO: handle possible exeptions
    pairs_last_posts_dict = querry.get_posts_for_pairs(
        posts_col, list(pairs_vol_dict.keys()))

    db_pairs_set = set(pairs_vol_dict.keys())
    posts_pairs_set = set(pairs_last_posts_dict.keys())
    # pairs wich lack posts at all
    diff = db_pairs_set.difference(posts_pairs_set)

    if len(diff) > 0:
        # TODO: handle possible exeptions
        pair, vol = find_pair_with_largest_vol(pairs_vol_dict, diff)
    else:
        pair = next(iter(pairs_last_posts_dict))
        vol = pairs_vol_dict[pair]

    # TODO: handle possible exeptions
    pair_market_stats = querry.gather_pair_data(ohlcv_col, pair)

    message = compose_message(pair, vol, pair_market_stats)
    print(message)


if __name__ == "__main__":
    main()
