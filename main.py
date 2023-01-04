import os
from pymongo import MongoClient
from twitter import Twitter

# Get environment variables
TW_ACCESS_TOKEN = os.environ['TW_ACCESS_TOKEN']
TW_ACCESS_TOKEN_SECRET = os.environ['TW_ACCESS_TOKEN_SECRET']
TW_CONSUMER_KEY = os.environ['TW_CONSUMER_KEY']
TW_CONSUMER_KEY_SECRET = os.environ['TW_CONSUMER_KEY_SECRET']
mongodb_user = os.environ['MONGODB_USER']
mongodb_password = os.environ['MONGODB_PASSWORD']
mongodb_cluster_address = os.environ['MONGODB_CLUSTER_ADDRESS']