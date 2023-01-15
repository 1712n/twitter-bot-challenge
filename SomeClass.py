from pymongo import MongoClient
import datetime

class SomeClass():
    """
    class description will be here later.
    'SomeClass' is a draft name. THE BETTER NAME FOR THE CLASS IS STILL BEING FORMULATED.
    """
    def __init__(self, user: str, password: str, address: str) -> None:
        self.user = user
        self.password = password
        self.address = address
        self.uri = f"mongodb+srv://{self.user}:{self.password}@{self.address}"
        self.client = MongoClient(self.uri)
        self.db = self.client['metrics']
        self.col_ohlcv = self.db['ohlcv_db']
        self.col_posts = self.db['posts_db']
        self.pair_to_post_data = {}
        self.message_body = ""

    def get_pair_to_post_data(self, days_amount: int=1, top_selection_limit: int=100) -> None:
        """
        methods description will be here later
        """
        cmd_cursor = self.col_ohlcv.aggregate(
            [
                {
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
                {
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
                {
                    '$sort': {
                        'pair_compound_volume': -1
                    }
                },
                {
                    '$limit': top_selection_limit
                },
                {
                    '$lookup': {
                        'from': 'posts_db', 
                        'let': {
                            'ohlcv_pair_base': '$_id.pair_base', 
                            'ohlcv_pair_symbol': '$_id.pair_symbol'
                        }, 
                        'pipeline': [
                            {
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
                            }, {
                                '$project': {
                                    'timestamp': {
                                        '$switch': {
                                            'branches': [
                                                {
                                                    'case': {
                                                        '$and': [
                                                            {
                                                                '$ne': [
                                                                    '$time', None
                                                                ]
                                                            }, {
                                                                '$eq': [
                                                                    'date', {
                                                                        '$type': '$time'
                                                                    }
                                                                ]
                                                            }
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
                            }, {
                                '$sort': {
                                    'timestamp': -1
                                }
                            }, {
                                '$limit': 1
                            }
                        ],
                        'as': 'latest_post'
                    }
                },
                {
                    '$sort': {
                        'latest_post.0.timestamp': 1,
                        'pair_compound_volume': -1
                    }
                },
                {
                    '$limit': 1
                },
                {
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
                {
                    '$lookup': {
                        'from': 'ohlcv_db', 
                        'let': {
                            'outer_base': '$pair_base', 
                            'outer_symbol': '$pair_symbol', 
                            'latest_post_timestamp': {
                                '$ifNull': [
                                    '$latest_post.timestamp', {
                                        '$dateSubtract': {
                                            'startDate': '$$NOW', 
                                            'unit': 'day', 
                                            'amount': 1
                                        }
                                    }
                                ]
                            }
                        }, 
                        'as': 'markets', 
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {
                                                '$eq': [
                                                    '$pair_base', '$$outer_base'
                                                ]
                                            }, {
                                                '$eq': [
                                                    '$pair_symbol', '$$outer_symbol'
                                                ]
                                            }, {
                                                '$gt': [
                                                    '$timestamp', '$$latest_post_timestamp'
                                                ]
                                            }, {
                                                '$eq': [
                                                    '$granularity', '1h'
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }, {
                                '$group': {
                                    '_id': '$marketVenue', 
                                    'market_comp_vol': {
                                        '$sum': {
                                            '$toDouble': '$volume'
                                        }
                                    }
                                }
                            }, {
                                '$setWindowFields': {
                                    'sortBy': {
                                        'market_comp_vol': -1
                                    }, 
                                    'output': {
                                        'pair_comp_vol': {
                                            '$sum': '$market_comp_vol', 
                                            'window': {
                                                'documents': [
                                                    'unbounded', 'unbounded'
                                                ]
                                            }
                                        }
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': 0, 
                                    'marketVenue': '$_id', 
                                    'pair_comp_vol': 1, 
                                    'market_comp_vol': 1,
                                    'market_comp_vol_percent': {
                                        '$round': [
                                            {
                                                '$divide': [
                                                    {
                                                        '$multiply': [
                                                            '$market_comp_vol', 100
                                                        ]
                                                    }, '$pair_comp_vol'
                                                ]
                                            }, 2
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        )
        self.pair_to_post_data = cmd_cursor.next()
        cmd_cursor.close()

    def compose_message_to_post(self) -> None:
        """Composes a message body for a tweet post"""
        title = f"Top Market Venues for {self.pair_to_post_data.get('pair_symbol').upper()}-{self.pair_to_post_data.get('pair_base').upper()}:\n"
        top_5_markets =""
        other_markets_percent = 0
        if len(self.pair_to_post_data.get("markets")) >=5:
            for i in range(5):
                top_5_markets += f"{self.pair_to_post_data.get('markets')[i].get('marketVenue')} {self.pair_to_post_data.get('markets')[i].get('market_comp_vol_percent')}%\n"
            for i in range(5, len(self.pair_to_post_data.get("markets"))):
                other_markets_percent += self.pair_to_post_data.get("markets")[i].get("market_comp_vol_percent")
        else:
            for market in self.pair_to_post_data.get('markets'):
                top_5_markets += f"{market.get('marketVenue')} {market.get('market_comp_percent')}%\n"
        self.message_body = title + top_5_markets + f"Others {other_markets_percent:.2f}%\n"

    def check_new_ohlcv_documents(self, pair_base: str, pair_symbol: str, latest_post_datetime: "datetime") -> bool:
        """It's an auxiliary method. Checks if there is at least one new ohlcv documents for a pair since latest_post_datetime"""
        result = self.col_ohlcv.find_one({"pair_base": pair_base, "pair_symbol": pair_symbol, "timestamp": {"$gt": latest_post_datetime}})
        return True if result != None else False

    def count_new_ohlcv_documents(self, pair_base: str, pair_symbol: str, latest_post_datetime: "datetime") -> int:
        """It's an auxiliary method. Returns a count of new ohlcv documents for a pair since latest_post_datetime"""
        return self.col_ohlcv.count_documents({"pair_base": pair_base, "pair_symbol": pair_symbol, "timestamp": {"$gt": latest_post_datetime}})


