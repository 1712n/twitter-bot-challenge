# For mongodb
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Logging
from core.log import log
# Settings
from core.config import settings


def get_db() -> MongoClient | None:
    log.logger.debug('Connecting to mongodb ...')
    db = MongoClient(settings.MONGODB_URI)
    try:
        # db = SessionLocal()
        # yield db
        # The ping command is cheap and does not require auth.
        db.admin.command('ping')
        log.logger.debug("Mongodb successful ping")
        return db
    except ConnectionFailure:
        log.logger.debug("Mongodb server isn't available")
        db.close()
        return None

