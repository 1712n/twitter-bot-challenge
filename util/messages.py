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
    logger.info(f"is pair: {pair} in posts: {post_present}")

    # Send message as s tweet
    ...


def add_message():
    ...

