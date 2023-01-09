import os
import logging
from dotenv import load_dotenv
from pymongo.database import Database
from tweepy.client import Client
from typing import Callable
from src.db import (
    get_db_client,
    get_top_pairs_by_volume,
    get_pair_to_post,
    get_message_to_post,
    get_origin_tweet_id,
    add_new_post_to_db
)
from src.twitter_bot import (
    get_twitter_client,
    new_tweet
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)5s: %(lineno)3s: %(levelname)s >> %(message)s",
    level=logging.INFO
)

load_dotenv()


def main(db: Database, twitter_client: Client, new_tweet_func: Callable):
    logging.info("Starting script..")
    top_pairs = get_top_pairs_by_volume(db)
    pair_to_post = get_pair_to_post(db, top_pairs)
    message_to_post = get_message_to_post(db, pair_to_post)
    origin_tweet_id = get_origin_tweet_id(db, pair_to_post)
    new_tweet_id = new_tweet_func(
        client=twitter_client,
        pair=pair_to_post,
        text=message_to_post,
        origin_tweet_id=origin_tweet_id
    )
    logging.info("New tweet with id=%s was created!", new_tweet_id)
    new_post_id = add_new_post_to_db(
        db=db,
        pair=pair_to_post,
        tweet_id=new_tweet_id,
        text=message_to_post
    )
    logging.info("New post with id=%s was added to database!", new_post_id)


if __name__ == "__main__":
    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    address = os.environ["MONGO_DB_ADDRESS"]
    uri = f"mongodb+srv://{user}:{password}@{address}"

    consumer_key = os.environ["TW_CONSUMER_KEY"]
    consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"]
    access_token = os.environ["TW_ACCESS_TOKEN"]
    access_token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]

    db_client = get_db_client(uri)
    db = db_client["metrics"]
    twitter_client = get_twitter_client(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret
    )
    main(db, twitter_client, new_tweet)
