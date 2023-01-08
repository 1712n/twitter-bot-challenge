from datetime import datetime, timezone, timedelta

import certifi
from pymongo import MongoClient

from dto import Pair, PairPost, AggregatePairVenue, Post

_MONGODB_USER = 'twitter-bot-challenge-user'
_MONGODB_PASSWORD = '1Dci5pk0UHGBUzpN'
_MONGO_DB_ADDRESS = 'loadtests.mjmdg.mongodb.net'

_TEMPLATE = 'mongodb+srv://{}:{}@{}/'

_client = MongoClient(_TEMPLATE.format(_MONGODB_USER, _MONGODB_PASSWORD, _MONGO_DB_ADDRESS),
                      tls=True,
                      tlsCAFile=certifi.where())
_db = _client['metrics']


class OhlcvDao:

    def get_top_pairs(self) -> list[Pair]:
        date = (datetime.now(timezone.utc) - timedelta(minutes=60))
        _match = {
            '$match': {
                'timestamp': {
                    '$gte': date
                }
            }
        }

        group = {
            "$group": {
                "_id": {
                    "pair": {
                        "$toUpper": {
                            "$concat": ["$pair_symbol", "-", "$pair_base"]
                        }
                    },
                    "volume": {
                        "$sum": {
                            "$toDouble": "$volume"
                        }
                    }

                }
            }
        }

        sort = {"$sort": {"volume": -1}}

        limit = {
            "$limit": 100
        }
        pipeline = [_match, group, limit, sort]
        result = _db.get_collection('ohlcv_db').aggregate(pipeline)
        return list(map(lambda val: Pair.build(val), result))

    def get_aggregated_venues(self, pair_post: PairPost):
        date = (datetime.now(timezone.utc) - timedelta(minutes=60))
        _match = {
            '$match': {
                'timestamp': {
                    '$gte': date
                }
            }
        }
        project = {
            '$project': {
                'pair': {
                    '$toUpper': {
                        '$concat': [
                            '$pair_symbol', '-', '$pair_base'
                        ]
                    }
                },
                'marketVenue': 1
            }
        }
        _match_pair = {
            '$match': {
                'pair': {
                    '$in': [pair_post.pair]
                }
            }
        }

        group = {
            "$group": {
                "_id": {
                    "pair": "$pair",
                    "venue": "$marketVenue",
                },
                "count": {"$count": {}},

            }
        }

        sort = {
            '$sort': {
                'count': -1
            }
        }

        pipeline = [_match, project, _match_pair, group, sort]
        result = _db.get_collection('ohlcv_db').aggregate(pipeline)

        return list(map(lambda val: AggregatePairVenue.build(val), result))


class PostDao:

    def get_last_posts(self, pairs: list[Pair]) -> list[PairPost]:
        _match = {
            '$match': {
                'pair': {
                    '$in': list(map(lambda val: val.pair, pairs))
                }
            }
        }

        sort = {
            '$sort': {
                'time': -1
            }
        }

        group = {
            '$group': {
                '_id': '$pair',
                'time': {
                    '$max': '$time'
                },
                'tweet_id': {
                    '$first': '$tweet_id'
                }
            }
        }
        pipeline = [_match, sort, group, sort]
        posts = _db.get_collection('posts_db').aggregate(pipeline)
        res = list(map(lambda val: PairPost.build(val), posts))
        grouped: dict[str, list[PairPost]] = {}
        for i in res:
            if i.pair in grouped:
                grouped[i.pair].append(i)
            else:
                grouped[i.pair] = [i]
        posts = []
        for key, values in grouped.items():
            if len(values) > 1:
                pair_post = max(values, key=lambda val: val.time)
                posts.append(pair_post)
            else:
                posts.append(values[0])
        return posts

    def save_post(self, post: Post):
        _db.get_collection('posts_db').insert_one(post.to_dict())
