import logging

from core.config import settings
from core.config import APP_NAME
from db.pairs import PairsToolBox

from pprint import pprint
from pprint import pformat

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


def get_top_pairs():
    logger.debug('get_top_pairs')
    top_limit = settings.TOP_LIMIT

    pairs_tool: PairsToolBox = PairsToolBox()

    # Get granularity of the largest pair
    logger.debug("Getting granularity with the most volume ...")
    err, gty = pairs_tool.get_largest_pair_granularity()
    if not err:
        logger.debug(f"The most granularity: {gty}")
    else:
        logger.critical("Failed to get all granularities")
        return None

    # Getting top pairs with known granularity
    err, top = pairs_tool.get_top_pairs(granularity=gty, limit=top_limit)
    if not err:
        logger.debug(f"We have: {len(top)} pairs with: {gty}")
        # pprint(top)
        logger.debug(pformat(top))

        return top
    else:
        logger.critical(f"Failed to get top pairs with: {gty}")
        return None

