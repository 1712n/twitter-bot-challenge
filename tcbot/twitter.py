import tweepy

from tcbot import env


class InvalidTwitterClientError(Exception):
    def __init__(self):
        default_message = "Could not get a Twitter client. " \
        "Please check your credentials, your network connection, and try again."
        super().__init__(default_message)


def get_twitter_client():
    try:
        consumer_key = env.TW_CONSUMER_KEY
        consumer_secret = env.TW_CONSUMER_KEY_SECRET
        access_token = env.TW_ACCESS_TOKEN
        access_token_secret = env.TW_ACCESS_TOKEN_SECRET

        return tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )
    except:
        return None
