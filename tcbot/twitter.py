import tweepy

from tcbot import env


def get_twitter_client():
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
