import os

from dotenv import load_dotenv

load_dotenv()

# General settings
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
MG_USER = os.getenv("MG_USER")
MG_PASSWORD = os.getenv("MG_PASSWORD")
MG_CLUSTER = os.getenv("MG_CLUSTER")