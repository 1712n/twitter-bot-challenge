# For logging
import logging
#
from pymongo.command_cursor import CommandCursor
# Temporary for dev test
from pprint import pprint


from core.config import settings
from db.session import db_session as db_session

logger = logging.getLogger()


class PostsToolBox:
    def __init__(self):
        self.__collection_name = settings.PAIRS_NAME

    def get_oldest_pairs_post(self, pairs: list) -> tuple[str | None, list | None]:
        """
        Get granularities from a mongodb collection
        :return:
        """
        # Preparing mongodb pipeline for db.collection.aggregate command
        # stage_match = {"$match": {"pair": {"$in": ["LINK-USDT", "LTC-USDT"]}}}
        stage_match = {"$match": {"pair": {"$in": pairs}}}
        stage_sort_1 = {"$sort": {"pair": 1, "time": -1}}
        stage_group = {
            "$group": {
                "_id": {
                    "pair": "$pair",
                },
                "latestTime": {"$first": "$time"},
            },
        },
        stage_sort_2 = {"$sort": {"time": 1}}
        stage_limit = {"$limit": 1}
        pipeline = [
            stage_match,
            stage_sort_1,
            stage_group,
            stage_sort_2,
            stage_limit,
        ]
        # Executing mongodb db.collection.aggregate command
        err = None
        try:
            coll = db_session.db[settings.PAIRS_NAME]
            result = coll.aggregate(pipeline)
            return err, [elem['_id'] for elem in result if len(elem['_id'])]
        except Exception as e:
            err = f"Failed to get granularities: {e}"
            log.logger.debug(err)
            return err, None
