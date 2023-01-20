# For logging
import logging
#
from pymongo.command_cursor import CommandCursor
from pprint import pformat

from core.config import settings
from core.config import APP_NAME
from models.message import Message
from db.pairs import PairsToolBox

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


def compose_message(pair_to_post: str):
    pairs_tool = PairsToolBox()

    # Get
    err, data = pairs_tool.get_venues_by_pair(
        pair=pair_to_post,
        limit=settings.VENUES_LIMIT,
    )
    try:
        msg = Message(pair_to_post=pair_to_post, data=data)
        logger.debug(f"Message text: {msg.text}")
    except Exception as e:
        logger.critical(f"Failed to make text for the message")
    ...


def send_message():
    ...


def add_message():
    ...

