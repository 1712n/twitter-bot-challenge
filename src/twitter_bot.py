import os
import tweepy
import logging
from dotenv import load_dotenv

from db import db

load_dotenv()


try:
    client = tweepy.Client(
        consumer_key=os.environ["TW_CONSUMER_KEY"],
        consumer_secret=os.environ["TW_CONSUMER_KEY_SECRET"],
        access_token=os.environ["TW_ACCESS_TOKEN"],
        access_token_secret=os.environ["TW_ACCESS_TOKEN_SECRET"]
    )
except Exception:
    print("Twitter connection failed")
    raise


def new_tweet(pair: str, message_to_post: str):
    logging.info("Checking previous posts for given pair")
    query_result = list(db.posts_db.aggregate(
        [
            {"$match": {"pair": pair}},
            {
                "$project": {
                    "pair": 1,
                    "tweet_id": {"$ifNull": ["$tweet_id", None]}
                }
            },
            {"$limit": 1}
        ]
    ))
    origin_tweet_id = None
    if query_result:
        logging.info("f{pair} was published earlier, the thread will continue")
        origin_tweet_id = query_result[0]["tweet_id"]
        if not origin_tweet_id:
            logging.info(
                "Origin tweet id isn't specified in db. Getting from twitter.."
                )
            bot_id = client.get_me().data.id
            paginator = tweepy.Paginator(
                client.get_users_tweets,
                id=bot_id,
                exclude=['retweets', 'replies'],
                max_results=100,
                user_auth=True
            )
            for page in paginator.flatten():
                if pair in page.data["text"]:
                    origin_tweet_id = page.data["id"]
                    logging.info("Origin tweet id is found")
                    break
    logging.info("Creating new tweet..")
    new_tweet = client.create_tweet(
        text=message_to_post, in_reply_to_tweet_id=origin_tweet_id
        )
    logging.info("New tweet is created!")
    return new_tweet.data["id"]
