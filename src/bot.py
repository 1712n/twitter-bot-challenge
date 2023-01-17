import datetime
import logging

import pymongo
from pymongo.errors import PyMongoError
from tweepy import TweepyException

from database import MongoDatabase
from twitter import Twitter


class MarketCapBot:
    def __init__(self, db: MongoDatabase, twitter: Twitter):
        self.twitter = twitter
        self.db = db
        logging.info('Bot is running...')


    def get_pair_to_post(self, top_pairs: list[str], latest_posted_pairs: list[tuple]) -> str:
        not_posted_pairs = [pair for pair, time in latest_posted_pairs if pair not in top_pairs]
        if len(not_posted_pairs) == 0:
            return sorted(latest_posted_pairs, key=lambda x: x[1])[0][0]
        return not_posted_pairs.pop()

    
    def prepare_message(self, pair_to_post: str, pair_market_venues: list[dict]) -> str:
        message = f"Top Market Venues for {pair_to_post}:\n"

        total_volume = sum(item['volume']['value'] for item in pair_market_venues)

        for item in pair_market_venues[:5]:
            message += f"{item['_id'].capitalize()} {round(item['volume']['value'] / total_volume * 100, 2)}%\n"
        
        total_left = sum(item['volume']['value'] for item in pair_market_venues[5:])
        message += f"Others {round(total_left / total_volume * 100, 2)}%"

        return message

    
    def tweet_message(self, pair_to_post: str, message_to_post: str) -> str:
        existed_tweet = list(self.db.posts().find(
            {'pair': pair_to_post, 'tweet_id': {'$exists': True}}
        ).sort('time', pymongo.DESCENDING).limit(1))

        if len(existed_tweet) == 0:
            response = self.twitter.new_tweet(message_to_post)
            return response.data.get('id')
        else:
            response = self.twitter.reply_to(message_to_post, existed_tweet[0]['tweet_id'])
            return response.data.get('id')

        
    def save_message_to_posts_db(self, pair: str, tweet_id: str, tweet_text: str) -> None:
        current_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        post = {
            'pair': pair,
            'tweet_text': tweet_text,
            'time': datetime.datetime.fromisoformat(current_time),
            'tweet_id': tweet_id
        }
        self.db.add_post(post)

    

    def run(self):
        try:
            top_pairs = self.db.get_top_pairs_by_amount()
            latest_posted_pairs = self.db.get_latest_posted_pairs(top_pairs)
            pair_to_post = self.get_pair_to_post(top_pairs, latest_posted_pairs)
            market_venues = self.db.get_market_venues(pair_to_post)
            message_to_post = self.prepare_message(pair_to_post, market_venues)
            tweet_id = self.tweet_message(pair_to_post, message_to_post)
            self.save_message_to_posts_db(pair_to_post, tweet_id, message_to_post)
            logging.info(f'Successfully posted pair {pair_to_post} to Twitter and saved to posts_db!')


        except (TweepyException, PyMongoError) as e:
            logging.error(f'Cannot Run a bot because of: {e}')
            exit(1)
