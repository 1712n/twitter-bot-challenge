import os
from dotenv import load_dotenv


load_dotenv()

MONGODB_ADDRESS = os.environ["MONGO_DB_ADDRESS"]
MONGODB_USERNAME = os.environ["MONGODB_USER"]
MONGODB_PASSWORD = os.environ["MONGODB_PASSWORD"]

TW_ACCESS_TOKEN = os.environ["TW_ACCESS_TOKEN"]
TW_ACCESS_TOKEN_SECRET = os.environ["TW_ACCESS_TOKEN_SECRET"]
TW_CONSUMER_KEY = os.environ["TW_CONSUMER_KEY"]
TW_CONSUMER_KEY_SECRET = os.environ["TW_CONSUMER_KEY_SECRET"]
