

import os
user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

consumer_key = os.environ["TW_CONSUMER_KEY"]
consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"]
access_token = os.environ["TW_ACCESS_TOKEN"]
access_token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]

from pymongo import MongoClient
client = MongoClient(f"mongodb+srv://{user}:{password}@{address}/?retryWrites=true&w=majority")

global ohlcv_db
global posts_db
db = client["metrics"]
ohlcv_db = db["ohlcv_db"]
posts_db = db["posts_db"]


import tweepy
global twitter_client

twitter_client = tweepy.Client(consumer_key=consumer_key,
                           consumer_secret=consumer_secret,
                           access_token=access_token,
                           access_token_secret=access_token_secret)

def get_data():
    top_pairs = list(ohlcv_db.aggregate([
    {"$group": {"_id": {"pair_symbol": "$pair_symbol", "pair_base": "$pair_base"}, \
                "compound_volume": {"$sum":  {"$toDouble": "$volume"}}}},
    {"$sort": {"compound_volume": -1}},
    {"$limit": 100},
    {"$project": {"_id": 0, "pair_symbol": "$_id.pair_symbol", "pair_base": "$_id.pair_base", "volume": "$compound_volume"}}
    ]))
    pairs = [f"{p['pair_symbol'].upper()}-{p['pair_base'].upper()}" for p in top_pairs]
    
    posts = list(posts_db.find({"pair": {"$in": pairs}}).sort("time", 1))
    
    # Set the threshold for the time since last post in days
    time_threshold = 7

    # Get the current date and subtract the threshold to get the date cutoff
    date_cutoff = datetime.now() - timedelta(days=time_threshold)

    # Get all pairs that have been posted in the past threshold days
    recently_posted_pairs = [x['pair'] for x in posts_db.find({"time": {"$gte": date_cutoff}})]
    recently_posted_pairs = [i for i in recently_posted_pairs if type(i) is str]
    pairs_to_post = list(set(pairs) - set(recently_posted_pairs))
    
    if len(pairs_to_post) == 0 :
        pairs_to_post = pairs
        
    pair_to_post = None
    for post in pairs_to_post:
        
        pair = [d for d in top_pairs if d['pair_symbol'] == post.split('-')[0].lower() \
                and d['pair_base'] == post.split('-')[1].lower()][0]

        if not pair_to_post or pair["volume"] > pair_to_post["volume"]:
            pair_to_post = pair
        
    top_markets = list(ohlcv_db.aggregate([
        {"$match": {"pair_symbol": pair_to_post['pair_symbol'], "pair_base": pair_to_post['pair_base']}},
        {"$group": {"_id": "$marketVenue", "market_volume": {"$sum": {"$toDouble": "$volume"}}}},
        {"$sort": {"market_volume": -1}}
    ]))
    # Compose the message_to_post
    message_to_post = f"Top Market Venues for {pairs_to_post[0]}:\n"
    total_volume = sum([d['market_volume'] for d in top_markets])

    for market in top_markets:
        message_to_post += f"{market['_id']} {round((market['market_volume']/total_volume)*100,2)}\n"
        
    message =  {
                "pair": pair_to_post['pair_symbol'].upper()+ '-' + pair_to_post['pair_base'].upper(),
                "message": message_to_post,
                "time": datetime.now()
                }
            
    return message


def post_data(message):
    # Connect to Twitter
    
    pair = message['pair']
    existing_post = posts_db.find_one({"pair": pair})
#     try:
#         message['time'] = message['time'].strftime("%Y-%m-%d %H:%M:%S")
#     except:
#         pass
    if existing_post:
        try:
            thread_id = existing_post['tweet_id']
            twitter_client.create_tweet(text = message['message'], in_reply_to_status_id=thread_id)
        except:
            tweet_id = str(existing_post['_id'])
            message['tweet_id'] = tweet_id
            try:
                twitter_client.create_tweet(text = message['message'])
                print('tweet updated')
            except:
                print("HTTPSConnectionPool error with twitter")
                posts_db.insert_one(message)
                
    else:
        tweet_id = str(existing_post['_id'])
        message['tweet_id'] = tweet_id
        try:
            twitter_client.create_tweet(text = message['message'])
            posts_db.insert_one(message)
            print('new tweet created')
        except:
            print("HTTPSConnectionPool error with twitter")
        
from datetime import datetime
from datetime import timedelta
if __name__ == "__main__":
    message = get_data()
    post_data(message)