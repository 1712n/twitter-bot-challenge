# Logging
import logging
#from core.log import log

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
            text = "Mongodb server isn't available"
            logger.critical(text)
            self.client.close()
            # If there's no connection, we will through an Exception
            raise Exception(text)

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


db_session = DatabaseConnection()


# def get_db():
    # logger.debug('Connecting to mongodb ...')
    # client = MongoClient(
    #     settings.MONGODB_URI,
    #     serverSelectionTimeoutMS=settings.SERVERSELECTIONTIMEOUTMS
    # )
    # try:
    #     # The ping command is cheap and does not require auth.
    #     client.admin.command('ping')
    #     logger.debug("Mongodb successful ping")
    #     db = client.get_database(settings.MONGODB_DBNAME)
    #     logger.debug(f"Mongodb db name set: {settings.MONGODB_DBNAME}")
    #     return db
    # except ConnectionFailure:
    #     logger.critical("Mongodb server isn't available")
    #     client.close()
    #     return None
#
#
# def check_connection():
#     ...


