from core.log import log
from core.config import settings
from db.pairs import Pair

from pprint import pprint


def get_top_pairs():
    log.logger.debug('get_top_pairs')
    top_limit = settings.TOP_LIMIT
    # instrument.get_top()
    # 1. get all granularities
    #   1h, 1m
    #   count all
    # 2. get top 100 for eache granularity
    # 3. choose biggest volumes
    pairs: Pair = Pair()

    # Get granularity of the largest pair
    log.logger.debug("Getting granularity with the most volume ...")
    err, gty = pairs.get_largest_pair_granularity()
    if not err:
        log.logger.debug(f"The most granularity: {gty}")
    else:
        log.logger.critical("Failed to get all granularities")
        return None

    # Getting top 100 with known granularity
    err, top = pairs.get_top_pairs(granularity=gty, limit=top_limit)
    if not err and len(top) == top_limit:
        log.logger.debug(f"We have: {len(top)} pairs with: {gty}")
        pprint(top)
        return top
    else:
        log.logger.critical(f"Failed to get top pairs with: {gty}")
        return None

