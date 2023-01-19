import pytest

from src.db_datatypes import *


@pytest.fixture
def sample_top_pairs():
    return TopPairsByVolume({'A-B': 10000, 'B-C': 2000, 'C-D': 300, 'D-E': 40, 'E-F': 5})


def test_top_pairs(sample_top_pairs):
    posted_pairs = {'A-B', 'B-C', 'D-E'}
    unposted_pairs = sample_top_pairs.diff(posted_pairs)

    expected = {'E-F', 'C-D'}
    assert unposted_pairs == expected

    expected = 'C-D', 300
    assert sample_top_pairs.find_largest_among(unposted_pairs)


@pytest.fixture
def sample_oldest_posts():
    return OldestLastPostsForPairs({'C-D': "massage", 'A-B': "massage", 'B-C': "massage",  'D-E': "massage", 'E-F': "massage"})


def test_olsest_posts(sample_oldest_posts):
    assert OldestLastPostsForPairs().first() == None

    assert sample_oldest_posts.first() == 'C-D'


@pytest.fixture
def sample_market_stats():
    return PairMarketStats({"Binance": 30.10,
                            "Coinbase": 20.20,
                            "Kraken": 10.30,
                            "Bitstamp": 5.40,
                            "Huobi": 2.50})


def test_pair_markets(sample_market_stats):
    expected = "Top Market Venues for BTC-USDC:\nBinance 30.10%\nCoinbase 20.20%\nKraken 10.30%\nBitstamp 5.40%\nHuobi 2.50%\nOthers 31.50%"
    assert sample_market_stats.compose_message('BTC-USDC', 100) == expected
