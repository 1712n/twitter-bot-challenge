import datetime
import tweepy
import pymongo
import os

def post_tweet(pair_to_post, message_to_post):
    try:
        # Authenticate with the Twitter API using OAuth 1.0a
        auth = tweepy.OAuth1UserHandler(
            consumer_key=os.environ['TW_CONSUMER_KEY'],
            consumer_secret=os.environ['TW_CONSUMER_SECRET'],
            access_token=os.environ['TW_ACCESS_TOKEN'],
            access_token_secret=os.environ['TW_ACCESS_TOKEN_SECRET'])
        api = tweepy.API(auth)
        tweet = api.update_status(status=message_to_post)
        tweet_id = tweet.id

        # add tweet to posts_db
        post = {
            "pair": pair_to_post,
            "time": datetime.datetime.utcnow(),
            "tweet_text": message_to_post,
            "tweet_id": tweet_id
        }
        posts_db.insert_one(post)
    except tweepy.TweepError as e:
        print(e)

def compose_message(pair_to_post, volume_data):
    message = f"Top Market Venues for {pair_to_post}:\n"
    for market in volume_data:
        message += f"{market['marketVenue']}: {market['volume']}%\n"
    return message

def main():
    try:
        # Connect to the MongoDB database
        user = os.environ["MONGODB_USER"]
        password = os.environ["MONGODB_PASSWORD"]
        address = os.environ["MONGO_DB_ADDRESS"]

        uri = f"mongodb+srv://{user}:{password}@{address}"
        client = pymongo.MongoClient(uri)
        ohlcv_db = client['ohlcv_db']
        posts_db = client['posts_db']

        # query for top 100 pairs by volume
        top_pairs = ohlcv_db.aggregate([
            {"$match": {"granularity": "1h"}},
            {"$sort": {"volume": -1}},
            {"$limit": 100}
        ])

        # query for pairs that haven't been posted recently
        not_recently_posted = posts_db.find({"time": {"$lt": datetime.datetime.utcnow() - datetime.timedelta(days=7)}})

        # find pair with highest volume among those that haven't been posted recently
        pair_to_post = None
        max_volume = 0
        for pair in not_recently_posted:
            volume = ohlcv_db.find_one({"pair_symbol": pair["pair"].split("-")[0], "pair_base": pair["pair"].split("-")[1], "granularity":"1h"})["volume"]
            if volume > max_volume:
                pair_to_post = pair["pair"]
                max_volume = volume

        # compose message to post
        volume_data = ohlcv_db.find({"pair_symbol": pair_to_post.split("-")[0], "pair_base": pair_to_post.split("-")[1], "granularity":"1h"})
        message_to_post = compose_message(pair_to_post, volume_data)
        print(message_to_post)

        post_tweet(pair_to_post, message_to_post)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
