import os

from dotenv import load_dotenv

load_dotenv()

# General settings
DEBUG = bool(int(os.getenv("DEBUG")))
LIVE = bool(int(os.getenv("LIVE")))

# Twitter API settings
if LIVE:
    TW_CONSUMER_KEY = os.getenv("TW_CONSUMER_KEY")
    TW_CONSUMER_KEY_SECRET = os.getenv("TW_CONSUMER_KEY_SECRET")
    TW_ACCESS_TOKEN = os.getenv("TW_ACCESS_TOKEN")
    TW_ACCESS_TOKEN_SECRET = os.getenv("TW_ACCESS_TOKEN_SECRET")
else:   
    TW_CONSUMER_KEY = os.getenv("M_TW_CONSUMER_KEY")
    TW_CONSUMER_KEY_SECRET = os.getenv("M_TW_CONSUMER_KEY_SECRET")
    TW_ACCESS_TOKEN = os.getenv("M_TW_ACCESS_TOKEN")
    TW_ACCESS_TOKEN_SECRET = os.getenv("M_TW_ACCESS_TOKEN_SECRET")

# MongoDB settings
if LIVE:
    MONGODB_USER = os.getenv("MONGODB_USER")
    MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")
    MONGO_DB_ADDRESS = os.getenv("MONGO_DB_ADDRESS")
else:
    MONGODB_USER = os.getenv("MG_USER")
    MONGODB_PASSWORD = os.getenv("MG_PASSWORD")
    MONGO_DB_ADDRESS = os.getenv("MG_CLUSTER")