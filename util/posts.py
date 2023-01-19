import logging

from core.config import APP_NAME
from db.posts import PostsToolBox

from pprint import pprint

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


def select_pair(top_pairs):
    posts_tool = PostsToolBox()
    err, oldest_post = posts_tool.get_oldest_pairs_post(top_pairs)
    logger.debug(f"We have oldest_post: {oldest_post}")
    ...


def add_message():
    ...


