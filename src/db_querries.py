from collections import OrderedDict


def get_top_pairs(pairs_col):
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
        pairs_dict[pair['_id']]=pair['volume_sum']

    return pairs_dict


def get_posts_for_pairs(posts_col, pairs_list):
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
        posts_dict[post['_id']]=post['lastPost']  
    return posts_dict