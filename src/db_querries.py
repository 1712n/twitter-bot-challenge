import logging
from pymongo import collection
from collections import OrderedDict


logger = logging.getLogger(__name__)


def get_top_pairs(pairs_col: collection.Collection) -> OrderedDict:
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

    pairs_dict = OrderedDict()
    for pair in pairs_total_vol_list:
        pairs_dict[pair['_id']] = pair['volume_sum']

    return pairs_dict


def get_posts_for_pairs(posts_col: collection.Collection, pairs_list: list) -> OrderedDict:
    posts_list = posts_col.aggregate(
        [
            {
                # fast operation getting advantage of time_-1 index
                '$sort': {
                    'time': -1
                }
            }, {
                '$group': {
                    '_id': {
                        # taking into account that some fields are arrays
                        '$cond': {
                            'if': {
                                '$isArray': '$pair'
                            },
                            'then': {
                                '$first': '$pair'
                            },
                            'else': '$pair'
                        }
                    },
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

    posts_dict = OrderedDict()
    for post in posts_list:
        posts_dict[post['_id']] = post['lastPost']
    return posts_dict


def gather_pair_data(pairs_col: collection.Collection, pair: str) -> OrderedDict:
    try:
        symbol, base = pair.lower().split('-')
    except:
        logger.error("Pair expected to be string in 'PAIR_SYMBOL-PAIR_BASE' format")
        raise 

    pair_data = pairs_col.aggregate([
        {
            '$match': {
                'pair_symbol': symbol
            }
        }, {
            '$match': {
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

    pair_stats = OrderedDict()
    for market in pair_data:
        pair_stats[market['_id']] = market['venue_vol']

    return pair_stats
