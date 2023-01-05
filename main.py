import os
from pymongo import MongoClient
import tweepy

# Authenticate with the Twitter API using OAuth 1.0a
auth = tweepy.OAuth1UserHandler(
    consumer_key=os.environ['TW_CONSUMER_KEY'],
    consumer_secret=os.environ['TW_CONSUMER_KEY_SECRET'],
    access_token=os.environ['TW_ACCESS_TOKEN'],
    access_token_secret=os.environ['TW_ACCESS_TOKEN_SECRET'])
api = tweepy.API(auth)

# Connect to MongoDB
user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]
uri = f"mongodb+srv://{user}:{password}@{address}"
client = MongoClient(uri)

#fuctionality
ohlcv_db = client['ohlcv_db']
posts_db = client['posts_db']

# Query ohlcv_db for the top 100 pairs by compound volume
top_pairs = ohlcv_db.pair.find().sort('volume', -1).limit(100)

# Query posts_db for the latest documents corresponding to those 100 pairs
latest_posts = posts_db.posts.find({'pair': {'$in': top_pairs}})

# Sort the results by the oldest timestamp to find the pairs that haven't been posted for a while,
# then sort by corresponding volume to find the biggest markets among them
pair_to_post = sorted(latest_posts, key=lambda x: x['timestamp'])[0]
pair_to_post = sorted(pair_to_post, key=lambda x: x['volume'], reverse=True)[0]
