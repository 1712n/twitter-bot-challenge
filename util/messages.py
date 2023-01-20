# For logging
import logging
#
from pymongo.command_cursor import CommandCursor
from pprint import pformat

from core.config import settings
from core.config import APP_NAME
from models.message import Message
from db.pairs import PairsToolBox
from db.posts import PostsToolBox
from twitter.tweets import TwitterToolBox
from models.post import Post


logger = logging.getLogger(f"{APP_NAME}.{__name__}")


def compose_message(pair_to_post: str) -> str | None:
    pairs_tool = PairsToolBox()

    # Get venues shares for pair_to_post
    err, data = pairs_tool.get_venues_by_pair(
        pair=pair_to_post,
        limit=settings.VENUES_LIMIT,
    )
    if err:
        logger.critical(f"Failed to get venues shares: {err}")
        return None

    # Make message text
    try:
        msg = Message(pair_to_post=pair_to_post, data=data)
        if msg:
            logger.debug(f"Message text: {msg.text}")
            return msg.text
        else:
            logger.critical(f"Failed to make text for the message")
            return None
    except Exception as e:
        logger.critical(f"Failed to make text for the message: {e}")
        return None


def send_message(pair: str, text: str) -> str | None:
    posts_tool = PostsToolBox()
    # Check if exist pair_to_post in posts
    err, post_present = posts_tool.is_pair_in_posts(pair)
    if err:
        logger.critical(f"Failed to check pair_to_post in posts")
        return None
    logger.debug(f"is pair: {pair} in posts: {post_present}")

    # Find tweet_id in posts
    err, old_tweet_id = posts_tool.get_tweet_id_by_pair(pair=pair)
    if err:
        logger.warning(f"Failed to check if tweet exists in posts")
    else:
        try:
            int(old_tweet_id)
            logger.debug(f"tweet_id is OK: {old_tweet_id}")
        except:
            logger.debug(f"tweet_id is not numeric: {old_tweet_id}")
            old_tweet_id = None  # Just to be sure

    # Send message as s tweet
    twitter_tool = TwitterToolBox()
    if old_tweet_id:
        logger.debug(f"Going to send message as reply tweet in old thread")
    else:
        logger.debug(f"Going to send message as new tweet as a new thread")
    # Going to create tweet
    err, new_tweet_id = twitter_tool.create_tweet(
        text,
        old_tweet_id=old_tweet_id
    )
    # If we were trying to create reply tweet. Let's try new tweet
    if err and old_tweet_id:
        logger.debug(f"Failed to create reply tweet. Going to create new")
        err, new_tweet_id = twitter_tool.create_tweet(
            text,
            old_tweet_id=None
        )
    elif err:
        return None

    logger.debug(f"Created tweet with id: {new_tweet_id}")
    return new_tweet_id


def add_message(pair: str, tweet_id: str, text: str) -> str | None:
    """
    Add message to posts_db with pair tweet_id and text
    :param pair:
    :param tweet_id:
    :param text:
    :return: object_id
    """
    posts_tool = PostsToolBox()
    post = Post(pair=pair, tweet_id=tweet_id, text=text)
    err, object_id = posts_tool.insert_post(post)
    if err:
        logger.debug(f"Failed to insert post: {post}")
    else:
        logger.debug(f"Inserted post id: {object_id}")
        return object_id

