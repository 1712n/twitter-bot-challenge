import json
import logging
import os
from datetime import datetime

import pymongo
import pymongo.errors
from dotenv import load_dotenv
from twitter import OAuth, Twitter, TwitterError


def sort_by_oldest_timestamp(posts):
    """
    Function to sort a list of pairs by the oldest timestamp
    """
    sorted_posts = sorted(posts, key=lambda x: x["timestamp"])
    return [post["pair"] for post in sorted_posts]


def select_biggest_market(ohlcv_db, pairs):
    """
    Function to select the biggest market among a list of pairs
    """
    compound_volumes = ohlcv_db.find({"pair": {"$in": pairs}}, {"pair": 1, "volume": 1})
    biggest_market_pair = max(compound_volumes, key=lambda x: x["volume"])
    return biggest_market_pair["pair"]


def compose_message(ohlcv_db, pair):
    """
    Function to compose the message to be posted for a given pair
    """
    market_values = ohlcv_db.find({"pair": pair}, {"market": 1, "volume": 1})
    message = "Top Market Venues for {}:\n".format(pair)
    total_volume = 0
    for value in market_values:
        market_name = value["market"]
        volume = value["volume"]
        total_volume += volume
        message += "{} {:.2f}%\n".format(market_name, volume)
    message += "Others {:.2f}%".format(100 - total_volume)
    return message


def get_thread_id(posts_db, pair):
    """
    Function to get the thread ID for the tweets of a given pair
    """
    thread = posts_db.find_one({"pair": pair}, {"thread_id": 1})
    return thread["thread_id"]


def post_tweet_to_thread(message, thread_id):
    """
    Function to post a tweet to a thread
    """
    twitter = Twitter(auth=oauth)
    return twitter.statuses.update(status=message, in_reply_to_status_id=thread_id)


def post_new_tweet(message):
    """
    Function to post a new tweet
    """
    twitter = Twitter(auth=oauth)
    return twitter.statuses.update(status=message)


def compose_tweet(ohlcv_db, posts_db):
    """
    Function to compose a new tweet
    """
    try:
        top_100_pairs = ohlcv_db.find().sort("volume", pymongo.DESCENDING).limit(100)
        top_100_pairs = [post["pair"] for post in top_100_pairs]
    except pymongo.errors.PyMongoError as e:
        logging.error("Error querying OHLCV database: %s", e)
        return

    try:
        latest_posts = list(posts_db.find({"pair": {"$in": top_100_pairs}}))
    except pymongo.errors.PyMongoError as e:
        logging.error("Error querying posts database: %s", e)
        return

    try:
        pairs_to_post = sort_by_oldest_timestamp(latest_posts)
    except pymongo.errors.PyMongoError as e:
        logging.error("Error sorting posts: %s", e)
        return

    try:
        pair_to_post = select_biggest_market(ohlcv_db, pairs_to_post)
    except pymongo.errors.PyMongoError as e:
        logging.error("Error selecting posts: %s", e)
        return

    return pair_to_post


def post_tweet(ohlcv_db, posts_db):
    """
    Function to post a new single tweet or tweet in thread
    """
    pair_to_post = compose_tweet(ohlcv_db, posts_db)
    try:
        message_to_post = compose_message(ohlcv_db, pair_to_post)
    except pymongo.errors.PyMongoError as e:
        logging.error("Error during compose message: %s", e)
        return
    try:
        count_tweet = len(list(posts_db.find({"pair": pair_to_post})))
    except pymongo.errors.PyMongoError as e:
        logging.error("Error counting posts: %s", e)
        return

    if count_tweet > 0:
        try:
            thread_id = get_thread_id(posts_db, pair_to_post)
            new_post = post_tweet_to_thread(message_to_post, thread_id)
        except pymongo.errors.PyMongoError as e:
            logging.error("Error finding thread: %s", e)
            return
        except TwitterError as e:
            logging.error("Error creating tweet in thread: %s", e)
            return

    else:
        try:
            new_post = post_new_tweet(message_to_post)
        except TwitterError as e:
            logging.error("Error creating tweet in thread: %s", e)
            return

    try:
        thread_id = json.loads(new_post)
        posts_db.insert_one(
            {
                "pair": pair_to_post,
                "message": message_to_post,
                "timestamp": datetime.now(),
                "thread_id": thread_id["id"],
            }
        )
    except pymongo.errors.PyMongoError as e:
        logging.error("Error adding a post to db: %s", e)


if __name__ == "__main__":
    log_format = (
        "%(asctime)s::%(levelname)s::%(name)s::" "%(filename)s::%(lineno)d::%(message)s"
    )
    logging.basicConfig(level="DEBUG", format=log_format)

    load_dotenv()
    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    address = os.environ["MONGO_DB_ADDRESS"]
    token = os.environ["TW_ACCESS_TOKEN"]
    token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]
    consumer_key = os.environ["TW_CONSUMER_KEY"]
    consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"]

    uri = f"mongodb+srv://{user}:{password}@{address}"
    client = pymongo.MongoClient(uri)

    ohlcv_db = client.ohlcv_db
    posts_db = client.posts_db

    oauth = OAuth(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        token=token,
        token_secret=token_secret,
    )

    post_tweet(ohlcv_db, posts_db)
