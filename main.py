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

# Connect to MongoDB
client = MongoClient(f'mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_cluster_address}')
ohlcv_db = client['ohlcv_db']
posts_db = client['posts_db']

# Query ohlcv_db for the top 100 pairs by compound volume
top_pairs = ohlcv_db.pair.find().sort('volume', -1).limit(100)

# Query posts_db for the latest documents corresponding to those 100 pairs
latest_posts = posts_db.posts.find({'pair': {'$in': top_pairs}})

# Sort results by the oldest timestamp to find the pairs that haven't been posted for a while
sorted_posts = latest_posts.sort('timestamp', 1)