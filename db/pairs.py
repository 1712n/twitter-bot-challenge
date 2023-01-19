import logging

from core.config import settings
from core.config import APP_NAME
from db.session import db_session as db_session

from pymongo.command_cursor import CommandCursor

# Temporary for dev test
from pprint import pprint

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class Pair:
    def __init__(self):
        self.__collection_name = settings.PAIRS_NAME

    def get_granularities(self) -> tuple[str | None, list | None]:
        """
        Get granularities from a mongodb collection
        :return:
        """
        # Preparing mongodb pipeline for db.collection.aggregate command
        stage_group = {
            "$group": {
                "_id": "$granularity",
            }
        }
        pipeline = [
            stage_group,
        ]
        # Executing mongodb db.collection.aggregate command
        err = None
        try:
            coll = db_session.db[settings.PAIRS_NAME]
            result = coll.aggregate(pipeline)
            return err, [elem['_id'] for elem in result if len(elem['_id'])]
        except Exception as e:
            err = f"Failed to get granularities: {e}"
            logger.critical(err)
            return err, None

    # Get granularity for largest pair by volume
    def get_largest_pair_granularity(self) -> tuple[str | None, str | None]:
        """
        Get granularity for largest pair by volume
        :return:
        """
        # Preparing mongodb pipeline for db.collection.aggregate command
        stage_project = {  # Make a pair field by $project $concat
            "$project": {
                "pair": {"$concat": ["$pair_symbol", "-", "$pair_base"]},
                "volume": "$volume",
                "granularity": "$granularity",
            }
        }
        stage_group = {  # Group data by $pair, $granularity, summary $volume
            "$group": {
                "_id": {
                    "pair": "$pair",
                    "granularity": "$granularity",
                },
                "volume": {"$sum": {"$toDouble": "$volume"}},
            }
        }
        stage_sort = {"$sort": {"volume": -1}}  # Sorty by $volume desc
        stage_limit = {"$limit": 1}  # Limit output to 1 row
        pipeline = [
            stage_project,
            stage_group,
            stage_sort,
            stage_limit,
        ]
        # pprint(pipeline)
        # Executing mongodb db.collection.aggregate command
        err = None
        try:
            coll = db_session.db[settings.PAIRS_NAME]
            logger.debug("Running aggregate ...")
            result: CommandCursor = coll.aggregate(pipeline)
        except Exception as e:
            err = f"Failed to get top granularity: {e}"
            logger.critical(err)
            return err, None
        # Parsing data
        try:
            elem = result.next()
            gty = elem['_id']['granularity']
            return err, gty
        except Exception as e:
            err = f"Failed to parse data: {e}"
            logger.debug(err)
            return err, None

    # Get top pairs
    def get_top_pairs(
            self,
            granularity: str,
            limit: int = 100,
            sort_order: int = -1  # -1  reverse order
    ) -> tuple[str | None, list | None]:
        """
        Get top 100 pair by volume with specified granularity
        :return:
        """
        # Preparing mongodb pipeline for db.collection.aggregate command
        stage_project = {  # Make a pair field by $project $concat
            "$project": {
                "pair": {
                    "$toUpper": {
                        "$concat": ["$pair_symbol", "-", "$pair_base"]
                    }
                },
                "volume": "$volume",
                "granularity": "$granularity",
            }
        }
        stage_group = {
            "$group": {
                "_id": {
                    "pair": "$pair",
                },
                "volume": {"$sum": {"$toDouble": "$volume"}},
            }
        }
        stage_match = {"$match": {"granularity": granularity}}
        stage_sort = {"$sort": {"volume": sort_order}}
        stage_limit = {"$limit": limit}
        pipeline = [
            stage_project,
            stage_match,
            stage_group,
            stage_sort,
            stage_limit,
        ]
        # Executing mongodb db.collection.aggregate command
        err = None
        try:
            coll = db_session.db[settings.PAIRS_NAME]
            result = coll.aggregate(pipeline)
        except Exception as e:
            err = f"Failed to get granularities: {e}"
            logger.critical(err)
            return err, None
        # Parsing data
        try:
            data = []
            for elem in result:
                market_pair = elem['_id'].copy()
                market_pair['volume'] = elem['volume']
                data.append(market_pair)
            if not len(data) and (len(data) > limit):
                err = f"No data or too more data in the results"
                logger.critical(err)
            return err, data
        except Exception as e:
            err = f"Failed to parse data: {e}"
            logger.critical(err)
            return err, None




