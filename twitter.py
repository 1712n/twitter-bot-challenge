import tweepy
import os

consumer_key = os.environ["TW_CONSUMER_KEY"]
consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"]
access_token = os.environ["TW_ACCESS_TOKEN"]
access_token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]


class Twitter:
    def __init__(self):
        self.client = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )

    def post_new_tweet(self, response):
        self.client.create_tweet(text=response)

    def post_reply(self, response, parent):
        self.client.create_tweet(text=response, in_reply_to_tweet_id=parent)
