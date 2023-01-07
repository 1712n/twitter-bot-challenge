import os
from dotenv import load_dotenv

load_dotenv()

DEBUG = int(os.environ["DEBUG"])

MONGODB_USER = os.environ["MONGODB_USER"]
MONGODB_PASSWORD = os.environ["MONGODB_PASSWORD"]
MONGO_DB_ADDRESS = os.environ["MONGO_DB_ADDRESS"]

TW_CONSUMER_KEY = os.environ["TW_CONSUMER_KEY"]
TW_CONSUMER_KEY_SECRET = os.environ["TW_CONSUMER_KEY_SECRET"]
TW_ACCESS_TOKEN = os.environ["TW_ACCESS_TOKEN"]
TW_ACCESS_TOKEN_SECRET = os.environ["TW_ACCESS_TOKEN_SECRET"]