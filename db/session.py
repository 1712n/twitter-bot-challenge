# Logging
import logging
# For mongodb
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Settings
from core.config import settings
from core.config import APP_NAME

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class DatabaseConnection:
    client: MongoClient = None
    db = None

    def __init__(self):
        logger.debug('Connecting to mongodb ...')
        self.client: MongoClient = MongoClient(
            settings.MONGODB_URI,
            serverSelectionTimeoutMS=settings.SERVERSELECTIONTIMEOUTMS
        )
        try:
            # The ping command is cheap and does not require auth.
            self.client.admin.command('ping')
            logger.debug("Mongodb successful ping")

            self.db = self.client.get_database(settings.MONGODB_DBNAME)
            logger.debug(f"Mongodb db name set: {settings.MONGODB_DBNAME}")
        except ConnectionFailure:
            err = "Mongodb server isn't available"
            logger.critical(err)
            self.client.close()
            # If there's no connection, we will through an Exception
            raise Exception(err)

    # Check database connection to database
    def check_db_connection(self) -> bool:
        if self.db:
            try:
                # The ping command is cheap and does not require auth.
                self.client.admin.command('ping')
                logger.debug("Mongodb successful ping")
                return True
            except ConnectionFailure:
                logger.debug("Mongodb server isn't available")
                return False
        else:
            logger.debug("There's no working connection. Nothing to check")
            return False


try:
    db_session = DatabaseConnection()
except Exception as e:
    logger.critical(f"Failed to connect to mongodb: {e}")
    db_session = None
