import logging
import tweepy
from .config import (
    TW_ACCESS_TOKEN,
    TW_ACCESS_TOKEN_SECRET,
    TW_CONSUMER_KEY,
    TW_CONSUMER_KEY_SECRET,
)


def get_tweepy_client():
    """Get tweepy API object."""
    logging.info("Getting tweepy client.")
    tweepy_client = tweepy.Client(
        consumer_key=TW_CONSUMER_KEY,
        consumer_secret=TW_CONSUMER_KEY_SECRET,
        access_token=TW_ACCESS_TOKEN,
        access_token_secret=TW_ACCESS_TOKEN_SECRET,
    )
    logging.info("Tweepy client created.")
    return tweepy_client


def post_tweet(message_to_post, post_id, posts_db, client: tweepy.Client):
    """
    Post tweet. Return tweet id.

    Keyword arguments:
    message_to_post -- message to post
    post_id -- id of the last post for the pair in posts_db
    posts_db -- posts database
    client -- tweepy client
    """
    logging.info("Starting post_tweet function.")
    reply_tweet_id = None
    if post_id is not None:
        # reply to existing tweet
        logging.info("Searching for parent tweet id to reply to.")
        post = posts_db.find_one({"_id": post_id})
        if "tweet_id" in post:
            reply_tweet_id = post["tweet_id"]
            logging.info("Found parent tweet id %s saved in posts_db.", reply_tweet_id)
        else:
            # search for post text in user's tweets
            logging.info("Searching for parent tweet id in user's tweets.")

            # get user id
            user_id = client.get_me().data.id

            # search for matching tweet text
            next_token = None
            while True:
                tweets = client.get_users_tweets(
                    id=user_id, pagination_token=next_token, user_auth=True
                )
                for tweet in tweets.data:
                    if tweet.text == message_to_post:
                        reply_tweet_id = tweet.id
                        logging.info("Found parent tweet id %s.", reply_tweet_id)
                        break
                if reply_tweet_id is not None or "next_token" not in tweets.meta:
                    logging.info("Parent tweet id not found.")
                    break
                next_token = tweets.meta["next_token"]

    # post tweet
    logging.info("Posting tweet.")
    tweet = client.create_tweet(
        text=message_to_post, in_reply_to_tweet_id=reply_tweet_id
    )
    logging.info("Tweet posted with id %s.", tweet.data["id"])
    return tweet.data["id"]
