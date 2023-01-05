from src.db import (
    get_top_pairs_by_volume,
    get_pair_to_post,
    get_message_to_post,
    get_origin_tweet_id
)
from .factory import (
    factory,
    OhlcvFactory,
    PostFactory
)


def test_get_top_pairs_by_volume(demo_db):
    demo_ohlcv_data = factory.build_batch(
        dict,
        size=6,
        FACTORY_CLASS=OhlcvFactory,
        pair_base=factory.Iterator(["A", "B", "C"]),
    )
    demo_db.ohlcv_db.insert_many(demo_ohlcv_data)
    result = get_top_pairs_by_volume(db=demo_db)
    expected = ['PAIR-C', 'PAIR-B', 'PAIR-A']
    assert result == expected


def test_get_pair_to_post(demo_db):
    demo_ohlcv_data = factory.build_batch(
        dict,
        size=6,
        FACTORY_CLASS=OhlcvFactory,
        pair_base=factory.Iterator(["A", "B", "C"]),
    )
    demo_post_data = factory.build_batch(
        dict,
        size=6,
        FACTORY_CLASS=PostFactory,
        pair=factory.Iterator(["PAIR-A", "PAIR-C", "PAIR-D", "PAIR-F"]),
    )
    demo_db.ohlcv_db.insert_many(demo_ohlcv_data)
    demo_db.posts_db.insert_many(demo_post_data)
    selected_pairs = ["PAIR-A", "PAIR-B", "PAIR-C"]
    result = get_pair_to_post(db=demo_db, pairs=selected_pairs)
    expected = "PAIR-C"
    assert result == expected


def test_get_message_to_post(demo_db):
    demo_ohlcv_data = factory.build_batch(
        dict,
        size=6,
        FACTORY_CLASS=OhlcvFactory,
        pair_base=factory.Iterator(["A", "B", "C"]),
        marketVenue=factory.Iterator(["market1", "market2"])
    )
    demo_db.ohlcv_db.insert_many(demo_ohlcv_data)
    selected_pair = "PAIR-C"
    result = get_message_to_post(db=demo_db, pair=selected_pair)
    expected = "Top Market Venues for PAIR-C:\nMarket2 55.56%\nMarket1 44.44%"
    assert result == expected


def test_get_origin_tweet_id(demo_db):
    demo_post_data = factory.build_batch(
        dict,
        size=6,
        FACTORY_CLASS=PostFactory,
        pair=factory.Iterator(["PAIR-A", "PAIR-B", "PAIR-C", "PAIR-D"]),
    )
    demo_db.posts_db.insert_many(demo_post_data)
    selected_pair = "PAIR-C"
    result = get_origin_tweet_id(db=demo_db, pair=selected_pair)
    expected = "00000003"
    assert result == expected
