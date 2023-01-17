from tweepy import TweepyException, Client
from config import TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_CONSUMER_SECRET, TWITTER_CONSUMER_KEY
import logging


class Twitter:
    def __init__(self):
        try:
            self.client = Client(
                consumer_key=TWITTER_CONSUMER_KEY,
                consumer_secret=TWITTER_CONSUMER_SECRET,
                access_token=TWITTER_ACCESS_TOKEN,
                access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
            )
            logging.info('Connected to Twitter!')
        except TweepyException as e:
            logging.error(f"Error while connecting to Twitter Client: {e}")
            exit(1)

    def new_tweet(self, text):
        return self.client.create_tweet(text=text)

    def reply_to(self, text, parent):
        return self.client.create_tweet(text=text, in_reply_to_tweet_id=parent)
