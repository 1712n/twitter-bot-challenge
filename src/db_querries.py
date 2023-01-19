import logging
from pymongo import collection
from collections import OrderedDict

from src.db_datatypes import *


logger = logging.getLogger(__name__)


def get_top_pairs(pairs_col: collection.Collection) -> TopPairsByVolume:
    pairs_total_vol_list = pairs_col.aggregate(
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
    )

    pairs_dict = TopPairsByVolume()
    for pair in pairs_total_vol_list:
        pairs_dict[pair['_id']] = pair['volume_sum']

    logger.info(
        f"Top pairs querry executed successfully: {len(pairs_dict.keys())} pair-vollume's total")

    return pairs_dict


def get_posts_for_pairs(posts_col: collection.Collection, pairs_list: list) -> OldestLastPostsForPairs:
    posts_list = posts_col.aggregate(
        [
            {
                # fast operation getting advantage of time_-1 index
                '$sort': {
                    'time': -1
                }
            }, {
                '$unwind': {
                    'path': '$pair'
                },
            }, {
                '$match': {
                    'pair': {
                        '$type': 'string'
                    }
                }
            },  {
                '$group': {
                    '_id': '$pair',
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

    posts_dict = OldestLastPostsForPairs()
    for post in posts_list:
        posts_dict[post['_id']] = post['lastPost']

    logger.info(
        f"Lattest post for pairs querry executed successfully: {len(posts_dict.keys())} pair-post's total")
    return posts_dict


def gather_pair_data(pairs_col: collection.Collection, symbol: str, base: str) -> PairMarketStats:
    pair_data = pairs_col.aggregate([
        {
            '$match': {
                'pair_symbol': symbol,
                'pair_base': base
            }
        }, {
            '$group': {
                '_id': '$marketVenue',
                'venue_vol': {
                    '$sum': {
                        '$toDouble': '$volume'
                    }
                }
            }
        }, {
            '$sort': {
                'venue_vol': -1
            }
        }, {
            '$limit': 5
        }
    ])

    pair_stats = PairMarketStats()
    for market in pair_data:
        pair_stats[market['_id']] = market['venue_vol']

    logger.info(
        f"Top 5 markets vollume querry for pair {symbol}-{base} executed successfully: {len(pair_stats.keys())} pair-markets's total")
    return pair_stats
