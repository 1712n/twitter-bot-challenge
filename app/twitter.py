import logging

import config
import tweepy

try:
    logging.info("Connecting to Twitter...")
    client = tweepy.Client(
        consumer_key=config.TW_CONSUMER_KEY,
        consumer_secret=config.TW_CONSUMER_KEY_SECRET,
        access_token=config.TW_ACCESS_TOKEN,
        access_token_secret=config.TW_ACCESS_TOKEN_SECRET,
    )
except Exception as e:
    logging.error("Error connecting to Twitter:", e)

if __name__ == "__main__":
    print(client.get_recent_tweets_count("BTC-USD"))