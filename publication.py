import datetime
import pymongo
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

user = os.environ.get("user")
password = os.environ.get("password")
cluster_address = os.environ.get("cluster_address")

TW_ACCESS_TOKEN = os.environ.get("TW_ACCESS_TOKEN")
TW_ACCESS_TOKEN_SECRET = os.environ.get("TW_ACCESS_TOKEN_SECRET")
TW_CONSUMER_KEY = os.environ.get("TW_CONSUMER_KEY")
TW_CONSUMER_KEY_SECRET = os.environ.get("TW_CONSUMER_KEY_SECRET")

