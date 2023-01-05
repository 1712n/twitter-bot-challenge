import pytest
from typing import List
from datetime import datetime, timedelta
from src.db import (
    get_top_pairs_by_volume,
    get_pair_to_post,
    get_message_to_post,
    get_origin_tweet_id
)


def test_get_top_pairs_by_volume(demo_db):
    demo_data = [
        {
            "pair_symbol": "pair",
            "pair_base": f"n{i}",
            "volume": f"{10 * i}"
        } for i in [1, 1, 1, 2, 2, 2, 3, 3, 3]
    ]
    demo_db.ohlcv_db.insert_many(demo_data)
    result = get_top_pairs_by_volume(db=demo_db)
    expected = ["PAIR-N3", "PAIR-N2", "PAIR-N1"]
    assert result == expected


def test_get_pair_to_post(demo_db):
    demo_data = [
        {
            "pair_symbol": "pair",
            "pair_base": f"n{i}",
            "volume": f"{10 * i}"
        } for i in [1, 1, 1, 2, 2, 2, 3, 3, 3]
    ]
    demo_db.ohlcv_db.insert_many(demo_data)
    demo_data2 = [
        {
            "timestamp": datetime.now() - timedelta(hours=i),
            "pair": f"PAIR-N{i}",
            "tweet_text": "Text",
            "tweet_id": f"00000000000{i}"

        } for i in range(10)
    ]
    demo_db.posts_db.insert_many(demo_data2)
    selected_pairs = ["PAIR-N3", "PAIR-N2", "PAIR-N1"]
    result = get_pair_to_post(db=demo_db, pairs=selected_pairs)
    expected = "PAIR-N3"
    assert result == expected


def test_get_message_to_post(demo_db):
    demo_data = [
        {
            "pair_symbol": "pair",
            "pair_base": f"n2",
            "volume": f"{10 * i}",
            "marketVenue": f"market{i}"

        } for i in range(1, 3)
    ]
    demo_db.ohlcv_db.insert_many(demo_data)
    pair = "PAIR-N2"
    result = get_message_to_post(demo_db, pair)
    expected = "Top Market Venues for PAIR-N2:\nMarket2 66.67%\nMarket1 33.33%"
    assert result in expected


def test_get_origin_tweet_id(demo_db):
    demo_data = [
        {
            "timestamp": datetime.now() - timedelta(hours=i),
            "pair": f"PAIR-N{i}",
            "tweet_text": "Text",
            "tweet_id": f"00000000000{i}"

        } for i in range(10)
    ]
    demo_db.posts_db.insert_many(demo_data)
    pair = "PAIR-N8"
    result = get_origin_tweet_id(demo_db, pair)
    expected = "000000000008"
    assert result == expected