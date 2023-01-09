import factory
from factory import Faker
from datetime import datetime, timedelta


class OhlcvFactory(factory.Factory):
    pair_symbol = "pair"
    pair_base = Faker("pair_base")
    volume = factory.Sequence(lambda x: (1000 + 100*x))
    marketVenue: Faker("marketVenue")


class PostFactory(factory.Factory):
    timestamp = factory.Sequence(
        lambda x: (datetime.now() - timedelta(days=x))
    )
    pair = Faker("pair")
    tweet_id = factory.Sequence(lambda x: f"0000000{1 + x}")
