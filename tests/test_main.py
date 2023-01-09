import string
from main import main
from .factory import (
    factory,
    OhlcvFactory,
    PostFactory
)


def test_main(demo_db):
    new_tweet_returns = set()

    def fake_new_tweet(pair, **kwargs):
        new_tweet_returns.add(pair)

    demo_ohlcv_data = factory.build_batch(
        dict,
        size=26,
        FACTORY_CLASS=OhlcvFactory,
        pair_base=factory.Iterator([i for i in string.ascii_letters]),
        marketVenue=factory.Iterator([f"market{i}" for i in range(1, 7)])
    )
    demo_post_data = factory.build_batch(
        dict,
        size=26,
        FACTORY_CLASS=PostFactory,
        pair=factory.Iterator([f"PAIR-{i}" for i in string.ascii_uppercase]),
    )
    demo_db.ohlcv_db.insert_many(demo_ohlcv_data)
    demo_db.posts_db.insert_many(demo_post_data)

    for _ in range(10):
        main(
            db=demo_db,
            twitter_client=None,
            new_tweet_func=fake_new_tweet
        )

    assert len(new_tweet_returns) == 10
