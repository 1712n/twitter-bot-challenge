# For logging
import logging
#
from pymongo.command_cursor import CommandCursor
from pprint import pformat

from core.config import settings
from core.config import APP_NAME
from db.session import db_session as db_session

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class PostsToolBox:
    def __init__(self):
        self.collection_name = settings.POSTS_NAME

    def get_oldest_pairs_post(self, pairs: list) -> tuple[str | None, dict | None]:
        """
        Get granularities from a mongodb collection
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        !! May be it wrong
        !! I'll get back to check it later
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        :return:
        """
        # Preparing mongodb pipeline for db.collection.aggregate command
        stage_match: dict = {"$match": {"pair": {"$in": pairs}}}
        stage_sort_1: dict = {"$sort": {"pair": 1, "time": -1}}
        stage_group: dict = {
            "$group": {
                "_id": "$pair",
                "latestTime": {"$first": "$time"},
            },
        }
        stage_sort_2: dict = {"$sort": {"time": 1}}
        stage_limit: dict = {"$limit": 1}
        pipeline: list = [
            stage_match,
            stage_sort_1,
            stage_group,
            stage_sort_2,
            stage_limit,
        ]
        # Executing mongodb db.collection.aggregate command
        # logger.debug(f"Going to execute aggregate with pipeline: {pformat(pipeline)}")
        logger.debug(f"Going to execute aggregate with pipeline: {pformat(pipeline)}")
        err = None
        try:
            coll = db_session.db[self.collection_name]
            result: CommandCursor = coll.aggregate(
                pipeline=pipeline,
                maxTimeMS=10000
            )
            elem = result.next()
            logger.debug(f"aggregate result: {elem}")
            return err, {'pair': elem['_id'], 'time': elem['latestTime']}
        except Exception as e:
            err = f"Failed to get oldest post: {e}"
            logger.debug(err)
            return err, None

    def is_pair_in_posts(self, pair: str) -> tuple[str | None, bool | None]:
        # Executing mongodb command
        stage_filter = {"pair": pair}
        logger.debug(f"Going to execute find: {pformat(stage_filter)}")
        err = None
        try:
            coll = db_session.db[self.collection_name]
            post_count = coll.count_documents(
                filter=stage_filter,
                limit=1,
                maxTimeMS=10000
            )
            if int(post_count) == 0:
                logger.debug(f"post count: {post_count}")
                return err, False
            elif int(post_count) == 1:
                return err, True
            else:
                err = f"Failed"
                return err, None
        except Exception as e:
            err = f"Failed check presence"
            logger.debug(err)
            return err, None



