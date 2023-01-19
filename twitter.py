import os
import logging
import tweepy
from tweepy.errors import *


def connect_twitter():
    """Authenticates to Twitter using credentials from environment vars.

    Returns:
        tweepy.Client: a Twitter API object.
    """

    consumer_key = os.environ["TW_CONSUMER_KEY"]
    consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"]
    access_token = os.environ["TW_ACCESS_TOKEN"]
    access_token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]

    try:
        client = tweepy.Client(
           consumer_key, consumer_secret, access_token, access_token_secret)

    except TooManyRequests as error:
        logging.error("Twitter error: too many requests: %s", str(error))
        raise

    except TwitterServerError as error:
        logging.error("An error happend on Twitter's server: %s", str(error))
        raise

    except Exception as error:
        logging.error("Authenticating to Twitter failed. Cause: %s", str(error))
        raise

    logging.info("Authenticated to Twitter successfully.")

    return client


def tweet(client, text, in_reply_to=None):
    """Tweets text, optionally replying to another tweet, and handles occuring errors.

    Arguments:
        client (tweepy.Client): a Twitter API object.
        text (str): text of the tweet.
        in_reply_to (int): id of the tweet to which this one will be replying. None by default.
    Returns:
        int: Posted tweet's id.
    """

    try:
        tweet = client.create_tweet(text=text, in_reply_to_tweet_id=in_reply_to)
        logging.info("Successfully tweeted the message!")
        return tweet.data["id"]

    except TooManyRequests as error:
        logging.error("Twitter error: too many requests: %s", str(error))
        raise

    except TwitterServerError as error:
        logging.error("An error happend on Twitter's server: %s", str(error))
        raise

    except Forbidden as error:
        logging.error("Posting a tweet failed because access is forbidden: %s", str(error))
        raise

    except Exception as error:
        logging.error("Posting a tweet failed with an error: %s", str(error))
        raise
