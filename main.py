"""
Main script for the Twitter bot challenge.
"""
import sys
import logging
from pymongo.errors import PyMongoError
import pandas as pd
import tweepy
from functions.config import DEBUG
from functions.database import (
    get_mongodb_client,
    get_pair_to_post,
    get_message_to_post,
    add_post_to_db,
)
from functions.twitter import get_tweepy_client, post_tweet


def main():
    """Main function."""

    level = logging.INFO
    if DEBUG:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logging.info("Starting main function.")

    try:
        mongo_client = get_mongodb_client()
    except PyMongoError:
        logging.exception("Failed to connect to MongoDB:")
        return -1

    try:
        tweepy_client = get_tweepy_client()
    except tweepy.TweepyException:
        logging.exception("Failed to connect to Twitter:")
        return -1

    ohlcv_db = mongo_client["metrics"]["ohlcv_db"]
    posts_db = mongo_client["metrics"]["posts_db"]

    try:
        pair, post_id, pair_symbol, pair_base = get_pair_to_post(ohlcv_db, posts_db)
    except PyMongoError:
        logging.exception("Failed to get pair to post:")
        return -1

    if pd.isna(post_id):
        post_id = None

    try:
        message_to_post = get_message_to_post(
            pair, pair_symbol, pair_base, ohlcv_db
        ).strip()
    except PyMongoError:
        logging.exception("Failed to construct message:")
        return -1

    try:
        tweet_id = post_tweet(message_to_post, post_id, posts_db, tweepy_client)
    except PyMongoError:
        logging.exception("Failed to post tweet:")
        return -1
    except tweepy.TweepyException:
        logging.exception("Failed to post tweet:")
        return -1

    try:
        add_post_to_db(pair, tweet_id, message_to_post, posts_db)
    except PyMongoError:
        logging.exception("Failed to add post to db:")
        try:
            tweepy_client.delete_tweet(id=tweet_id)
        except tweepy.TweepyException:
            logging.exception("Failed to delete tweet:")
        return -1

    return 0


if __name__ == "__main__":
    sys.exit(main())