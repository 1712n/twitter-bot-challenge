import pytest

from src.db_datatypes import *
from tests.test_db_datatypes import *
from main import MarketCapBot
from datetime import datetime


class MockMarketCapBot(MarketCapBot):
    def __init__(self, pairs_volume, pairs_last_posts):
        # rewrite init function so it willn't initialize pymongo and tweepy client
        self.ohlcv_col = None
        self.posts_col = None

        self.pairs_vollume_dict = pairs_volume
        self.pairs_last_posts_dict = pairs_last_posts


def test_MarketCapBot(mocker, sample_top_pairs, sample_oldest_posts, sample_market_stats):
    test_bot_empty = MockMarketCapBot(
        TopPairsByVolume(), OldestLastPostsForPairs())

    with pytest.raises(Exception, match="Incorrect data gathered!"):
        test_bot_empty.get_pair_to_post()

    # Unused code but theoretically...
    # test_bot = MockMarketCapBot(sample_top_pairs, sample_oldest_posts)

    # assert test_bot.pairs_vollume_dict == sample_top_pairs
    # assert test_bot.pairs_last_posts_dict == sample_oldest_posts

    # mocker.patch(
    #     'src.db_querries.gather_pair_data',
    #     return_value=sample_market_stats
    # )


'''
i'm feel lazy to cover all MarketCapBot with tests, so i implemented 
last_post data validation since i feel like it's most vulnerable for 
database corruption part, 
'''
last_posts_validation_testdata = [
    ({'tweet_text': 'message', 'time': datetime.now()}, True),
    ({'tweet_text': 'message', 'time': datetime.now(), 'tweet_id': 123}, True),
    ({'tweet_text': 'message', 'time': None, 'tweet_id': 123}, False),
    ({'tweet_text': 123, 'time': 123}, False),
    ({}, False),
    ("message", False)
]


@pytest.mark.parametrize("last_post, expected_valid", last_posts_validation_testdata)
def test_validation(last_post, expected_valid):
    test_bot_empty = MockMarketCapBot(
        TopPairsByVolume(), OldestLastPostsForPairs())

    assert test_bot_empty.last_post_validate(last_post) == expected_valid
