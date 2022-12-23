import datetime
import unittest

from pymongo import MongoClient

from main import (
    compose_message,
    compose_tweet,
    get_thread_id,
    get_database,
    select_biggest_market,
    sort_by_oldest_timestamp,
)


class TestPostTweet(unittest.TestCase):
    def setUp(self):
        self.client = MongoClient()
        self.database = get_database("test")
        self.ohlcv_db = self.database.ohlcv_db
        self.posts_db = self.database.posts_db

        data_ohlcv = [
            {"pair": "BTC-USDC", "market": "Binance", "volume": 30.10},
            {"pair": "BTC-USDC", "market": "Coinbase", "volume": 20.20},
            {"pair": "BTC-USDC", "market": "Kraken", "volume": 10.30},
            {"pair": "BTC-USDC", "market": "Bitstamp", "volume": 5.40},
            {"pair": "BTC-USDC", "market": "Huobi", "volume": 2.50},
        ]
        self.data_posts = [
            {
                "pair": "BTC-USDC",
                "timestamp": datetime.datetime.now() - datetime.timedelta(days=3),
                "thread_id": 3,
            },
            {
                "pair": "ETH-USDC",
                "timestamp": datetime.datetime.now() - datetime.timedelta(days=7),
                "thread_id": 2,
            },
            {
                "pair": "LTC-USDC",
                "timestamp": datetime.datetime.now() - datetime.timedelta(days=14),
                "thread_id": 1,
            },
        ]

        self.ohlcv_db.insert_many(data_ohlcv)
        self.posts_db.insert_many(self.data_posts)

    def tearDown(self):
        self.client.drop_database("test")

    def test_auxiliary_functions(self):

        pairs = sort_by_oldest_timestamp(self.data_posts)
        self.assertEqual(pairs, ["LTC-USDC", "ETH-USDC", "BTC-USDC"])
        biggest_market_pair = select_biggest_market(self.ohlcv_db, pairs)
        self.assertEqual(biggest_market_pair, "BTC-USDC")
        message_to_post = compose_message(self.ohlcv_db, biggest_market_pair)
        self.assertEqual(
            message_to_post,
            "Top Market Venues for BTC-USDC:\nBinance 30.10%\nCoinbase 20.20%\n"
            "Kraken 10.30%\nBitstamp 5.40%\nHuobi 2.50%\nOthers 31.50%",
        )

    def test_compose_tweet(self):
        self.assertEqual(compose_tweet(self.ohlcv_db, self.posts_db), "BTC-USDC")

    def test_post_tweet(self):
        pair_to_post = compose_tweet(self.ohlcv_db, self.posts_db)
        self.assertEqual(get_thread_id(self.posts_db, pair_to_post), 3)


if __name__ == "__main__":
    unittest.main()
