# Project logging
import pymongo.database

from core.log import log
# For mongodb
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
# Temporary for printing
import pprint

# For mongodb
from db.session import get_db
from core.config import settings

def main():
    log.logger.debug('The app started')

    db = get_db()
    try:
        colls = [settings.PAIRS_NAME, settings.POSTS_NAME]
        for coll in colls:
            content = db[coll].find_one()
            log.logger.debug(f"collection: {coll} find_one: {content}")
    except Exception as e:
        log.logger.debug(f"list_databases... failed: {e}")


if __name__ == '__main__':
    main()
