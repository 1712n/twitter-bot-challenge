import logging
import os
import sys

from dotenv import load_dotenv


load_dotenv()

TW_ACCESS_TOKEN = os.getenv('TW_ACCESS_TOKEN')
TW_ACCESS_TOKEN_SECRET = os.getenv('TW_ACCESS_TOKEN_SECRET')
TW_CONSUMER_KEY = os.getenv('TW_CONSUMER_KEY')
TW_CONSUMER_KEY_SECRET = os.getenv('TW_CONSUMER_KEY_SECRET')

MONGODB_USER = os.getenv('MONGODB_USER')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD')
MONGO_DB_ADDRESS = os.getenv('MONGO_DB_ADDRESS')


logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)



