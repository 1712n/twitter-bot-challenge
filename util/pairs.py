import logging

from core.config import settings
from core.config import APP_NAME
from db.pairs import PairsToolBox

from pprint import pprint

logger = logging.getLogger(f"{APP_NAME}.{__name__}")

def get_top_pairs():
    logger.debug('get_top_pairs')
    top_limit = settings.TOP_LIMIT
    # instrument.get_top()
    # 1. get all granularities
    #   1h, 1m
    #   count all
    # 2. get top 100 for eache granularity
    # 3. choose biggest volumes
    pairs: PairsToolBox = PairsToolBox()

    # Get granularity of the largest pair
    logger.debug("Getting granularity with the most volume ...")
    err, gty = pairs.get_largest_pair_granularity()
    if not err:
        logger.debug(f"The most granularity: {gty}")
    else:
        logger.critical("Failed to get all granularities")
        return None

    # Getting top pairs with known granularity
    err, top = pairs.get_top_pairs(granularity=gty, limit=top_limit)
    if not err:
        logger.debug(f"We have: {len(top)} pairs with: {gty}")
        pprint(top)
        return top
    else:
        logger.critical(f"Failed to get top pairs with: {gty}")
        return None

