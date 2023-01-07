import tweepy
import pymongo
import os

def main():
    try:
        # Authenticate with the Twitter API using OAuth 1.0a
        auth = tweepy.OAuth1UserHandler(
            consumer_key=os.environ['TW_CONSUMER_KEY'],
            consumer_secret=os.environ['TW_CONSUMER_SECRET'],
            access_token=os.environ['TW_ACCESS_TOKEN'],
            access_token_secret=os.environ['TW_ACCESS_TOKEN_SECRET'])
        api = tweepy.API(auth)

        # Connect to the MongoDB database
        user = os.environ["MONGODB_USER"]
        password = os.environ["MONGODB_PASSWORD"]
        address = os.environ["MONGO_DB_ADDRESS"]

        uri = f"mongodb+srv://{user}:{password}@{address}"
        client = pymongo.MongoClient(uri)
        ohlcv_db = client['ohlcv_db']
        posts_db = client['posts_db']

        # Use a tailable cursor to efficiently read the latest documents in the ohlcv_db capped collection
        cursor = ohlcv_db.find(cursor_type=pymongo.cursor.CursorType.TAILABLE).sort([('$natural', -1)]).limit(100)

        # Continuously process new documents as they are added to the collection
        while cursor.alive:
            for doc in cursor:
                # Check if the document corresponds to a pair that hasn't been tweeted about recently
                latest_post = posts_db.find_one({'pair': doc['pair']})
                if not latest_post or doc['timestamp'] > latest_post['timestamp']:
                    # Compose the message_to_post
                    message_to_post = f"Top Market Venues for {doc['pair']}:\n"
                    for market, volume in doc['market_volumes'].items():
                        message_to_post += f"{market}: {volume:.2f}%\n"

                    # Check if the message_to_post already exists in the posts collection
                    existing_post = posts_db.find_one({'message': message_to_post})
                    if existing_post:
                        # If it does, post the tweet to the corresponding Twitter thread
                        tweet = api.update_status(status=message_to_post, in_reply_to_status_id=existing_post['tweet_id'])
                    else:
                        # If it doesn't, post a new tweet
                        tweet = api.update_status(status=message_to_post)

                    # Add the message_to_post to the posts collection
                    posts_db.insert_one({'pair': doc['pair'], 'timestamp': doc['timestamp'], 'message': message_to_post, 'tweet_id': tweet.id})

    except Exception as e:
        # Print the error message if something goes wrong
        print(e)

if __name__ == '__main__':
    main()
