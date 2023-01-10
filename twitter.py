import tweepy

from config import TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_CONSUMER_SECRET, TWITTER_CONSUMER_KEY


class Twitter:
    def __init__(self):
        self.client = tweepy.Client(
            consumer_key=TWITTER_CONSUMER_KEY,
            consumer_secret=TWITTER_CONSUMER_SECRET,
            access_token=TWITTER_ACCESS_TOKEN,
            access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
        )

    def tweet(self, text):
        return self.client.create_tweet(text=text)

    def reply(self, text, parent):
        return self.client.create_tweet(text=text, in_reply_to_tweet_id=parent)
