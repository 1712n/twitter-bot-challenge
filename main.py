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


def main():
    log.logger.debug('The app started')

    client: MongoClient = get_db()
    try:
        log.logger.debug('list_databases...')
        for db_item in client.list_databases():
            #pprint.pprint(db_item)
            db_name = db_item['name']
            log.logger.debug(f"db: {db_name}")

            db: pymongo.database.Database = client[db_name]
            for coll in db.list_collection_names():
                log.logger.debug(f"collection: {coll}")

    except Exception as e:
        log.logger.debug(f"list_databases... failed: {e}")

    # db = client.sample_mflix
    # collection = db.movies

    # log.logger.debug('find_one ...')
    # pprint.pprint(collection.find_one())


if __name__ == '__main__':
    main()
