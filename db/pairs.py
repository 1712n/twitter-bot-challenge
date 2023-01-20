import logging
#
from pymongo.command_cursor import CommandCursor
#
from pprint import pformat

from core.config import settings
from core.config import APP_NAME
from db.session import db_session as db_session


logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class PairsToolBox:
    def __init__(self):
        self.collection_name = settings.PAIRS_NAME

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
            coll = db_session.db[self.collection_name]
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
            coll = db_session.db[self.collection_name]
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
        stage_project_2 = {"$project": {"pair": "$pair"}}
        pipeline = [
            stage_project,
            stage_match,
            stage_group,
            stage_sort,
            stage_limit,
            stage_project_2,
        ]
        # Executing mongodb db.collection.aggregate command
        err = None
        try:
            coll = db_session.db[self.collection_name]
            result = coll.aggregate(pipeline)
        except Exception as e:
            err = f"Failed to get granularities: {e}"
            logger.critical(err)
            return err, None
        # Parsing data
        try:
            data = []
            for elem in result:
                # market_pair = elem['_id'].copy()['pair']
                market_pair = elem['_id']['pair']
                data.append(market_pair)
            if not len(data) and (len(data) > limit):
                err = f"No data or too more data in the results"
                logger.critical(err)
            logger.debug(f"Going to return data: {data}")
            return err, data
        except Exception as e:
            err = f"Failed to parse data: {e}"
            logger.critical(err)
            return err, None

    # Get top pairs
    def get_venues_by_pair(self, pair: str, limit: int = 5) \
            -> tuple[str | None, list | None]:
        """
        Get venues market share for pair
        :return:
        """
        # Preparing mongodb pipeline for db.collection.aggregate command
        """
        db.getCollection("ohlcv_db").aggregate(
        [
            { "$project": {
              'pair': {
                '$toUpper':  
                    {'$concat': ['$pair_symbol', '-', '$pair_base']},
                },
              'marketVenue': '$marketVenue',
              'volume': '$volume',
              }
            },
          {'$match': {'pair': 'SHIB-USDT'}},
          {
            '$group': {
              '_id': '$marketVenue', 
              'venueVolume': {"$sum": {"$toDouble": "$volume"}},
            }
          }, {
            '$group': {
              '_id': null, 
              'total': {
                '$sum': '$venueVolume'
              }, 
              'data': {
                '$push': '$$ROOT'
              }
            }
          }, {
            '$unwind': {
              'path': '$data'
            }
          }, {
            '$project': {
              '_id': 0, 
              'marketVenue': '$data._id', 
              'share': '$data.venueVolume', 
              'percentage': {
                '$multiply': [
                  100, {
                    '$divide': [
                      '$data.venueVolume', '$total'
                    ]
                  }
                ]
              }
            }
          },
            {'$sort': {'percentage': -1}},
          { '$limit': 5 },
            {"$project": {
                "marketVenue": "$marketVenue",
                "percentage": {"$round": ["$percentage", 2]}
            }}
        ]
        )
        """
        stage_project_make_pair: dict = {
            "$project": {
                "pair": {
                    "$toUpper":
                        {"$concat": ["$pair_symbol", "-", "$pair_base"]}
                },
                "marketVenue": "$marketVenue",
                "volume": "$volume",
                }
            }
        stage_match_pair: dict = {"$match": {"pair": pair}}
        stage_group_venue_sum_venuevolume: dict = {
            "$group": {
                "_id": "$marketVenue",
                "venueVolume": {"$sum": {"$toDouble": "$volume"}},
            }
          }
        stage_group_total_venuevolume: dict = {
            "$group": {
                "_id": "null",
                "total": {"$sum": "$venueVolume"},
                "data": {"$push": "$$ROOT"}
            }
        }
        stage_unwind: dict = {
            "$unwind": {"path": "$data"}
        }
        stage_project_percentage: dict = {
            "$project": {
                "_id": 0,
                "marketVenue": "$data._id",
                "share": "$data.venueVolume",
                "percentage": {
                    "$multiply": [
                        100,
                        {"$divide": ["$data.venueVolume", "$total"]}
                    ]
                }
            }
        }
        stage_sort_percentage: dict = {"$sort": {"percentage": -1}}
        stage_limit: dict = {"$limit": limit}
        stage_project_round: dict = {
            "$project": {
                "marketVenue": "$marketVenue",
                "percentage": {"$round": ["$percentage", 2]}
            }
        }
        pipeline: list = [
            stage_project_make_pair,
            stage_match_pair,
            stage_group_venue_sum_venuevolume,
            stage_group_total_venuevolume,
            stage_unwind,
            stage_project_percentage,
            stage_sort_percentage,
            stage_limit,
            stage_project_round,
        ]
        # Executing mongodb db.collection.aggregate command
        logger.debug(f"Going to execute aggregate with pipeline: {pformat(pipeline)}")
        err = None
        try:
            coll = db_session.db[self.collection_name]
            result = coll.aggregate(pipeline)
        except Exception as e:
            err = f"Failed to get venues market share for pair: {e}"
            logger.critical(err)
            return err, None
        # Parsing data
        try:
            data = []
            for elem in result:
                logger.debug(f"row: {elem}")
                market_pair = elem
                data.append(market_pair)
            if not len(data) and (len(data) > limit):
                err = f"No data or too more data in the results"
                logger.critical(err)
            logger.debug(f"Going to return data: {data}")
            return err, data
        except Exception as e:
            err = f"Failed to parse data: {e}"
            logger.critical(err)
            return err, None



