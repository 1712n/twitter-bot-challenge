import logging

import tweepy
from tweepy import TweepyException


class TwitterClient:
    def __init__(self,
                 access_token: str,
                 access_token_secret: str,
                 consumer_key: str,
                 consumer_key_secret: str):
        try:
            self._client = tweepy.Client(
                access_token=access_token,
                access_token_secret=access_token_secret,
                consumer_key=consumer_key,
                consumer_secret=consumer_key_secret
            )
        except c as error:
            logging.error(f"Can not create Twitter client: {error}")
            raise

    def origin_tweet_id(self, pair: str) -> str | None:
        bot_id = self._client.get_me().data.id
        for tweet in tweepy.Paginator(
                self._client.get_users_tweets,
                id=bot_id,
                user_auth=True,
                max_results=100,  # 100 is max by API
                exclude="retweets").flatten(limit=10000):
            if pair in tweet["text"]:
                return tweet["id"]
        return None

    def post(self, tweet: str, origin_tweet_id: str | None) -> str:
        try:
            return self._client.create_tweet(text=tweet, in_reply_to_tweet_id=origin_tweet_id).data["id"]
        except TweepyException as error:
            logging.error(f"Can not post tweet: {error}")
            raise
