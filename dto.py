from datetime import datetime

class Post:

    def __init__(self, time, tweet_id, tweet_text, pair):
        self.time = time
        self.tweet_id = tweet_id
        self.tweet_text = tweet_text
        self.pair = pair

    def to_dict(self):
        return {
            'pair': self.pair,
            'tweet_id': self.tweet_id,
            'tweet_text': self.tweet_text,
            'time': self.time
        }

class Pair:

    def __init__(self, pair: str, volume: float):
        self.pair = pair
        self.volume = volume

    @staticmethod
    def build(val: dict) -> 'Pair':
        return Pair(val['_id']['pair'], val['_id']['volume'])


class PairPost:

    def __init__(self, pair: str, tweet_id: str, time: datetime):
        self.pair = pair
        self.tweet_id = tweet_id
        self.time = time

    @staticmethod
    def build(val: dict) -> 'PairPost':
        if isinstance(val['_id'], list):
            pair = val['_id'][0]
        else:
            pair = val['_id']
        return PairPost(pair, val['tweet_id'], val['time'])


class AggregatePairVenue:

    def __init__(self, pair: str, venue: str, count: int):
        self.pair = pair
        self.venue = venue
        self.count = count

    @staticmethod
    def build(val: dict) -> 'AggregatePairVenue':
        return AggregatePairVenue(val['_id']['pair'], val['_id']['venue'], val['count'])
