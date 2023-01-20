import logging

from core.config import APP_NAME
from db.posts import PostsToolBox

from pprint import pprint

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


def select_pair(top_pairs):
    posts_tool = PostsToolBox()

    err, oldest_post = posts_tool.get_oldest_pairs_post(top_pairs)
    if not err and ('pair' in oldest_post):
        logger.debug(f"We have oldest post: {oldest_post}")
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # Sometimes pair is list. I don't know why. It's a workaround
        if type(oldest_post['pair']) == list:
            logger.warning(f"Sometimes pair is list: {oldest_post['pair']}")
            pair_to_post = oldest_post['pair'][0]
        else:
            pair_to_post = oldest_post['pair']
        logger.debug(f"pair_to_post: {pair_to_post}")
        return pair_to_post
    else:
        logger.critical("Failed to get oldest post")
        return None


