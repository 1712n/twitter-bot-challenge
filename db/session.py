# For mongodb
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Logging
from core.log import log
# Settings
from core.config import settings


def get_db():
    log.logger.debug('Connecting to mongodb ...')
    client = MongoClient(settings.MONGODB_URI)
    try:
        # The ping command is cheap and does not require auth.
        client.admin.command('ping')
        log.logger.debug("Mongodb successful ping")
        db = client.get_database(settings.MONGODB_DBNAME)
        log.logger.debug(f"Mongodb db name set: {settings.MONGODB_DBNAME}")
        return db
    except ConnectionFailure:
        log.logger.debug("Mongodb server isn't available")
        client.close()
        return None

