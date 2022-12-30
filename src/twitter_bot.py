import os
import tweepy
from dotenv import load_dotenv

load_dotenv()


try:
    client = tweepy.Client(
        consumer_key = os.environ["TW_CONSUMER_KEY"],
        consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"],
        access_token = os.environ["TW_ACCESS_TOKEN"],
        access_token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]
    )
except Exception:
    print("Twitter connection failed")
    raise
