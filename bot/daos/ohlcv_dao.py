from typing import List, Dict

from bot.mongo_db_client import MongoDbClient


class OhlcvDao:
    def __init__(self, mongodb_client: MongoDbClient, granularity: str = "1h"):
        # TODO add validation of granularity
        self._granularity = granularity
        self._db = mongodb_client.get_collection("ohlcv_db")

    def top_by_compound_volume(self, limit: int) -> List[str]:
        pipeline = [
            {
                '$match': {
                    'granularity': self._granularity
                }
            }, {
                '$group': {
                    '_id': {
                        '$toUpper': {
                            '$concat': [
                                '$pair_symbol', '-', '$pair_base'
                            ]
                        }
                    },
                    'compoundVolume': {
                        '$sum': {
                            '$toDouble': '$volume'
                        }
                    }
                }
            },  {
                '$sort': {
                    'compoundVolume': -1
                }
            }, {
                '$limit': 100
            }
        ]

        return [p["_id"] for p in self._db.aggregate(pipeline)]

    def volume_by_markets(self, pair: str) -> Dict[str, int]:
        pair_symbol, pair_base = pair.lower().split("-")
        pipeline = [
            {
                '$match': {
                    'pair_symbol': pair_symbol,
                    'pair_base': pair_base,
                    'granularity': self._granularity
                }
            }, {
                '$group': {
                    '_id': '$marketVenue',
                    'venueVolume': {
                        '$sum': {
                            '$toDouble': '$volume'
                        }
                    }
                }
            }, {
                '$sort': {
                    'venueVolume': -1
                }
            }
        ]
        return {x["_id"]: x["venueVolume"] for x in self._db.aggregate(pipeline)}
