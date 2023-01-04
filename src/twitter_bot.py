import os
import tweepy
import logging
from dotenv import load_dotenv
from tweepy.errors import TweepyException

load_dotenv()

try:
    client = tweepy.Client(
        consumer_key=os.environ["TW_CONSUMER_KEY"],
        consumer_secret=os.environ["TW_CONSUMER_KEY_SECRET"],
        access_token=os.environ["TW_ACCESS_TOKEN"],
        access_token_secret=os.environ["TW_ACCESS_TOKEN_SECRET"]
    )
except TweepyException as err:
    logging.error("Twitter connection failed: %s", err)


def new_tweet(pair: str, text: str, origin_tweet_id: str | None):
    if not origin_tweet_id:
        logging.info(
            "Origin tweet id isn't specified in db. Getting from twitter.."
            )
        try:
            bot_id = client.get_me().data.id
            paginator = tweepy.Paginator(
                client.get_users_tweets,
                id=bot_id,
                exclude=['retweets', 'replies'],
                max_results=100,
                user_auth=True
            )
        except TweepyException as err:
            logging.error("Failed to get data from Twitter: %s", err)
            raise
        for page in paginator.flatten():
            if pair in page.data["text"]:
                origin_tweet_id = page.data["id"]
                logging.info("Origin tweet id is found")
                break
    logging.info("Creating new tweet..")
    try:
        new_tweet = client.create_tweet(
            text=text, in_reply_to_tweet_id=origin_tweet_id
            )
    except TweepyException as err:
        logging.error("Failed to publish new tweet: %s", err)
        raise
    return new_tweet.data["id"]
