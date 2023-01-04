import logging
import tweepy
from tweepy.client import Client
from tweepy.errors import TweepyException


def get_twitter_client(
    consumer_key, consumer_secret, access_token, access_token_secret
):
    logging.info("Getting twitter client..")
    try:
        client = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )
    except TweepyException as err:
        logging.error("Twitter connection failed: %s", err)
    logging.info("Сonnected to Twitter!")
    return client


def new_tweet(
    client: Client, pair: str, text: str, origin_tweet_id: str | None
):
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
                logging.info("Origin tweet id is found!")
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
