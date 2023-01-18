from pymongo import MongoClient
from pymongo.results import InsertOneResult
from bson import ObjectId
import datetime

class PairToPost():
    """
    Represents a pair to post and performs required mongodb operations
    """
    def __init__(self, user: str, password: str, address: str) -> None:
        self.client = MongoClient(f"mongodb+srv://{user}:{password}@{address}")
        self.db = self.client['metrics']
        self.col_ohlcv = self.db['ohlcv_db']
        self.col_posts = self.db['posts_db']
        self.pair_document = {}
        self.last_inserted_id = None

    def get_pair_to_post(self, days_amount: int=1, top_selection_limit: int=100) -> None:
        """
        This method performs an aggregation pipeline to get the pair_to_post \
        data which is stored as a dict in pair_document property of a class instance.

        Arguments:
        days_amount - 1st pos. arg. - number of days to limit an initial \
        dataset. 1 by default.

        top_selection_limit - 2nd  pos. arg. - amount of unique pairs \
        sorted by desc order to get top n pairs.
        """
        cmd_cursor = self.col_ohlcv.aggregate(
            [
                { # initial selection by granularity and days_amount
                    '$match': {
                        'granularity': '1h',
                        '$expr': {
                            '$gt': [
                                '$timestamp', {
                                    '$dateSubtract': {
                                        'startDate': '$$NOW', 
                                        'unit': 'day', 
                                        'amount': days_amount
                                    }
                                }
                            ]
                        }
                    }
                },
                { # grouping by pair to get a list of unique pairs with compound volume per pair
                    '$group': {
                        '_id': {
                            'pair_base': '$pair_base',
                            'pair_symbol': '$pair_symbol'
                        }, 
                        'pair_compound_volume': {
                            '$sum': {
                                '$toDouble': '$volume'
                            }
                        }
                    }
                },
                { # sorting by compound volume in descending order to get a top-chart
                    '$sort': {'pair_compound_volume': -1}
                },
                { # limitation to get n top pairs(100 by default)
                    '$limit': top_selection_limit
                },
                { # this is a left outer join of ohlcv_db with posts_db to get the latest post for every pair.
                  # There are comments for all substages in a nested pipeline.
                    '$lookup': {
                        'from': 'posts_db', 
                        'let': {
                            'ohlcv_pair_base': '$_id.pair_base', 
                            'ohlcv_pair_symbol': '$_id.pair_symbol'
                        }, 
                        'pipeline': [
                            { # select the only posts with the same pair to join
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            {
                                                '$concat': [
                                                    {
                                                        '$toUpper': '$$ohlcv_pair_symbol'
                                                    }, '-', {
                                                        '$toUpper': '$$ohlcv_pair_base'
                                                    }
                                                ]
                                            }, '$pair'
                                        ]
                                    }
                                }
                            },
                            { # timestamp of the post may be stored either in timestamp or time fields.
                              # So the switch operator picks the right field.
                                '$project': {
                                    'timestamp': {
                                        '$switch': {
                                            'branches': [
                                                {
                                                    'case': {
                                                        '$and': [
                                                            {'$ne': ['$time', None]},
                                                            {'$eq': ['date', {'$type': '$time'}]}
                                                            ]
                                                        },
                                                    'then': '$time'
                                                }
                                            ],
                                            'default': '$timestamp'
                                        }
                                    }, 
                                    'pair': 1, 
                                    'tweet_id': 1, 
                                    'tweet_text': 1,
                                    'text': 1,
                                    'message': 1
                                }
                            },
                            { # sort by timestamp in desc order to get the latest correlated post at the top
                                '$sort': {'timestamp': -1}
                            },
                            { # pick the only one post - the latest one
                                '$limit': 1
                            }
                        ],
                        'as': 'latest_post'
                    }
                },
                { # sort all pairs by the latest post timestamp in asc order to get the pair with the oldest timestamp.
                  # Then sort pairs with the same timestamp by compound volume in desc order.
                  # So we get the pair with the biggest volume among pairs with the oldest timestamp.
                    '$sort': {
                        'latest_post.0.timestamp': 1,
                        'pair_compound_volume': -1
                    }
                },
                { # pick the right pair
                    '$limit': 1
                },
                { # compose the document in more convenient structure.
                  # Leave the only required fields. Turn a latest_post array into a subdocument.
                    '$project': {
                        '_id': 0, 
                        'pair_base': '$_id.pair_base', 
                        'pair_symbol': '$_id.pair_symbol',
                        'latest_post': {
                            '$arrayToObject': {
                                '$objectToArray': {
                                    '$arrayElemAt': [
                                        '$latest_post', 0
                                    ]
                                }
                            }
                        }
                    }
                },
                { # at this stage we find pair-correlated ohlcv-documents to get compound volume per market + percent value.
                  # There are comments for all substages in a nested pipeline.
                    '$lookup': {
                        'from': 'ohlcv_db', 
                        'let': {
                            'outer_base': '$pair_base',
                            'outer_symbol': '$pair_symbol',
                            # here we check the timestamp of the latest post for null value
                            # and define a default value in this case.
                            # It can be usefull when no post was found at a stage of first lookup,
                            # but we need a timestamp to rely on to get the only actual ohlcv docs.
                            'latest_post_timestamp': {
                                '$ifNull': [
                                    '$latest_post.timestamp', {
                                        '$dateSubtract': {
                                            'startDate': '$$NOW', 
                                            'unit': 'day', 
                                            'amount': days_amount
                                        }
                                    }
                                ]
                            }
                        }, 
                        'as': 'markets', 
                        'pipeline': [
                            { # find correlated docs by pair, timestamp and granularity
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {'$eq': ['$pair_base', '$$outer_base']},
                                            {'$eq': ['$pair_symbol', '$$outer_symbol']},
                                            {'$gt': ['$timestamp', '$$latest_post_timestamp']},
                                            {'$eq': ['$granularity', '1h']}
                                        ]
                                    }
                                }
                            },
                            { # group docs by markets to get compound value per market
                                '$group': {
                                    '_id': '$marketVenue', 
                                    'market_comp_vol': {
                                        '$sum': {
                                            '$toDouble': '$volume'
                                        }
                                    }
                                }
                            },
                            { # applying this stage to do two actions at once:
                              # 1) sorting markets by comp volume in desc order to get top-chart
                              # 2) add additional pair_comp_vol field to every market
                              #  to calculate a market vol percent at the next stage
                                '$setWindowFields': {
                                    'sortBy': {
                                        'market_comp_vol': -1
                                    }, 
                                    'output': {
                                        'pair_comp_vol': {
                                            '$sum': '$market_comp_vol', 
                                            'window': {
                                                'documents': ['unbounded', 'unbounded']
                                            }
                                        }
                                    }
                                }
                            },
                            { # define the more convenient structure for every market in an array for a final doc.
                                '$project': {
                                    '_id': 0, 
                                    'marketVenue': '$_id', 
                                    'pair_comp_vol': 1, 
                                    'market_comp_vol': 1,
                                    # getting the percent value of comp vol per market
                                    'market_comp_vol_percent': {
                                        '$round': [
                                            {'$divide': [{'$multiply': ['$market_comp_vol', 100]}, '$pair_comp_vol']},2
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        )
        self.pair_document = cmd_cursor.next()
        cmd_cursor.close()

    def check_new_ohlcv_documents(self, pair_base: str, pair_symbol: str, latest_post_datetime: "datetime") -> bool:
        """It's an auxiliary method. Checks if there is at least one new ohlcv documents for a pair since latest_post_datetime"""
        result = self.col_ohlcv.find_one({"pair_base": pair_base, "pair_symbol": pair_symbol, "timestamp": {"$gt": latest_post_datetime}})
        return True if result != None else False

    def count_new_ohlcv_documents(self, pair_base: str, pair_symbol: str, latest_post_datetime: "datetime") -> int:
        """It's an auxiliary method. Returns a count of new ohlcv documents for a pair since latest_post_datetime"""
        return self.col_ohlcv.count_documents({"pair_base": pair_base, "pair_symbol": pair_symbol, "timestamp": {"$gt": latest_post_datetime}})

    def add_post_to_collection(self, new_post_data: dict) -> bool:
        """Add a record into posts_db with info about a recently published tweet."""
        if new_post_data["pair_to_post_id"] != id(self):
            print("pair_to_post_id doesn't match")
            return
        insert_result = self.col_posts.insert_one({
            "pair": new_post_data["pair"],
            "text": new_post_data["text"],
            "timestamp": new_post_data["timestamp"],
            "tweet_id": new_post_data["tweet_id"]
        })
        self.last_inserted_id = insert_result.inserted_id
        return isinstance(insert_result, InsertOneResult) and isinstance(insert_result.inserted_id, ObjectId)