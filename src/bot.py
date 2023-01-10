import datetime
import logging

import pymongo
from pymongo.errors import PyMongoError
from tweepy import TweepyException

from database import MongoDatabase
from twitter import Twitter


class TwitterMarketCapBot:
    def __init__(self, db: MongoDatabase, twitter: Twitter):
        self.db = db
        self.twitter = twitter
        logging.info('Bot started...')

    def get_top_pairs(self, amount: int = 100) -> set[str]:
        granularity = {"$match": {"granularity": "1h"}}
        data = {
            "$group": {
                "_id": {
                    "pair": {"$toUpper": {"$concat": ["$pair_symbol", "-", "$pair_base"]}},
                },
                "volume": {
                    "$sum": {"$toDouble": "$volume"}
                }
            }
        }
        sorting = {
            "$sort": {
                "volume": pymongo.DESCENDING
            }
        }
        qty = {"$limit": amount}
        result = self.db.ohlcv().aggregate([
            granularity,
            data,
            sorting,
            qty
        ])
        return {item['_id']['pair'] for item in result}

    def get_latest_posted_pairs(self, top_pairs: set[str]) -> set[tuple]:
        matching = {
            "$match": {
                "pair": {"$in": list(top_pairs)}
            }
        }
        latest = {
            "$group": {
                "_id": "$pair",
                "time": {"$max": "$time"}
            }
        }
        sort_latest = {
            "$sort": {
                "time": pymongo.DESCENDING
            }
        }

        result = self.db.posts().aggregate([
            matching,
            latest,
            sort_latest,
        ])
        return {(item["_id"], item['time']) for item in result if isinstance(item["_id"], str)}

    def get_not_posted_pairs(self, top_pairs: set[str], latest_posted_pairs: set[tuple]) -> set[str]:
        latest_posted_pairs_set = {pair for pair, time in latest_posted_pairs}
        return top_pairs - latest_posted_pairs_set

    def get_pair_to_post(self, top_pairs: set[str], latest_posted_pairs: set[tuple]) -> str:
        """
        - if there are pairs that were not posted ever we return any of them
        - else we return oldest from latest posted pairs
        """
        not_posted_pairs = self.get_not_posted_pairs(top_pairs, latest_posted_pairs)
        if not_posted_pairs:
            return not_posted_pairs.pop()
        return sorted(latest_posted_pairs, key=lambda x: x[1])[0][0]

    def get_pair_market_venues(self, pair_to_post: str) -> list[dict]:
        pair_symbol = pair_to_post.split('-')[0].lower()
        pair_base = pair_to_post.split('-')[1].lower()

        granularity = {"$match": {"granularity": "1h"}}
        pair = {
            "$match": {
                "pair_symbol": pair_symbol,
                "pair_base": pair_base
            }
        }
        group = {
            "$group": {
                "_id": "$marketVenue",
                "volume": {
                    "$max": {
                        "time": "$timestamp",
                        "value": {"$toDouble": "$volume"},
                    }
                }
            }
        }
        sorting = {
            "$sort": {
                "volume.value": pymongo.DESCENDING
            }

        }
        result = self.db.ohlcv().aggregate([
            granularity,
            pair,
            group,
            sorting
        ])
        return list(result)

    def get_message_to_post(self, pair_to_post: str, pair_market_venues: list[dict]) -> str:
        message_to_post = f"Top Market Venues for {pair_to_post}:\n"
        total_volume = sum(item['volume']['value'] for item in pair_market_venues)
        if len(pair_market_venues) <= 6:
            for item in pair_market_venues:
                message_to_post += f"{item['_id'].capitalize()} {item['volume']['value'] / total_volume * 100:.2f}%\n"
        else:
            for i, item in enumerate(pair_market_venues):
                if i == 5:
                    break
                message_to_post += f"{item['_id'].capitalize()} {item['volume']['value'] / total_volume * 100:.2f}%\n"
            total_volume_left = sum(item['volume']['value'] for item in pair_market_venues[5:])
            message_to_post += f"Others {total_volume_left / total_volume * 100:.2f}%"
        return message_to_post

    def save_message_to_posts_db(self, posted_pair: str, tweet_id: str, tweet_message: str) -> None:
        current_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        post = {
            'pair': posted_pair,
            'tweet_text': tweet_message,
            'time': datetime.datetime.fromisoformat(current_time),
            'tweet_id': tweet_id
        }
        self.db.posts().insert_one(post)

    def tweet_message(self, pair_to_post: str, message_to_post: str) -> str:
        parent_tweet = self.db.posts().find(
            {'pair': pair_to_post, 'tweet_id': {'$exists': True}}
        ).sort('time', pymongo.DESCENDING).limit(1)
        parent_tweet = list(parent_tweet)
        if len(parent_tweet) == 0:
            response = self.twitter.tweet(message_to_post)
            return response.data.get('id')
        else:
            response = self.twitter.reply(message_to_post, parent_tweet[0]['tweet_id'])
            return response.data.get('id')

    def run(self):
        try:
            top_pairs = self.get_top_pairs()
            latest_posted_pairs = self.get_latest_posted_pairs(top_pairs)
            pair_to_post = self.get_pair_to_post(top_pairs, latest_posted_pairs)
            pair_market_venues = self.get_pair_market_venues(pair_to_post)
            message_to_post = self.get_message_to_post(pair_to_post, pair_market_venues)
            tweet_id = self.tweet_message(pair_to_post, message_to_post)
            self.save_message_to_posts_db(pair_to_post, tweet_id, message_to_post)
            logging.info(f'Successfully posted pair {pair_to_post} to Twitter and saved tweet to posts_db.')
        except (TweepyException, PyMongoError) as e:
            logging.error(f'Got problem while running our bot: {e}')
            exit(1)
