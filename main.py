import os
import logging
from collections import OrderedDict

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

import src.db_querries as querry


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(name)s: %(levelname)s: %(message)s', level=logging.DEBUG)


class TopPairsByVolume(OrderedDict):
    def diff(self, external_keys_set: set) -> set:
        return set(self.keys()).difference(external_keys_set)

    def find_largest_among(self, pairs_set: set):
        for pair in self.keys():
            if pair in pairs_set:
                return pair, self[pair]
        # TODO rewrite exeption text
        raise Exception(
            "Pair intendened to de in oredered dictionary, containing top 100 pairs")


class OldestLastPostsForPairs(OrderedDict):
    def oldes_pair(self) -> str:
        return next(iter(self))


class PairMarketStats(OrderedDict):
    def compose_message(self, pair: str, total_vol: float):
        message = f"Top Market Venues for {pair}:\n"
    
        remains = 100
        for market in self.keys():
            market_vol_percens = self[market] / total_vol * 100
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
    pairs_vollume_dict = TopPairsByVolume(querry.get_top_pairs(ohlcv_col))
    # TODO: handle possible exeptions
    pairs_last_posts_dict = OldestLastPostsForPairs(querry.get_posts_for_pairs(
        posts_col, list(pairs_vollume_dict.keys())))


    # pairs wich lack posts at all
    diff = pairs_vollume_dict.diff(set(pairs_last_posts_dict.keys()))


    if len(diff) > 0:
        # TODO: handle possible exeptions
        pair, vol = pairs_vollume_dict.find_largest_among(diff)
    else:
        pair = pairs_last_posts_dict.oldes_pair()
        vol = pairs_vollume_dict[pair]


    if type(pair) != str or type(vol) != float:
        logger.error("Coudn't deside between unposted and oldest posted pairs")
        raise Exception("Wrong types!")


    # TODO: handle possible exeptions
    pair_market_stats = PairMarketStats(querry.gather_pair_data(ohlcv_col, pair))

    message = pair_market_stats.compose_message(pair, vol)
    print(message)


if __name__ == "__main__":
    main()
