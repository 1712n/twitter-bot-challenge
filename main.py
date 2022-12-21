import os
import sys
from loguru import logger
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

logger.remove()
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SS}</green> | <level>{level: <8}</level> | {message}"
)

logger.info('CryptoCapBot has started!')


def get_mongodb_client():
    address = os.environ["MONGO_DB_ADDRESS"]
    username = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]

    uri = f"mongodb+srv://{username}:{password}@{address}"

    return MongoClient(uri)


mongodb_client = get_mongodb_client()

for db in mongodb_client.list_database_names():
    collections = mongodb_client[db].list_collection_names()

    if len(collections) == 0:
        continue

    logger.debug(
        "Database '{}' contains {} collection(s): {}",
        db, len(collections), ','.join(collections)
    )

logger.info('CryptoCapBot is exiting. Good-bye!')
