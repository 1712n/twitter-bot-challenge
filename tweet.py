import tweepy
import pymongo
import datetime


# Replace these values with your own Twitter API keys and tokens
consumer_key = "NnPL65juj8nstnd6x5t4tECun"
consumer_secret = "i0yD4lmam6mBDqTaLlCOSQ5DdP38yj8yeqZ2ezNy3GRHYw4Zku"
access_token = "1600087262909317120-WaeIU8aBbV0QkyML1U3xtzZP1NHeAO"
access_token_secret = "uNdcHPWMzR8uruFHoXEFKylue5VmOExki4PZn3omw9x3U"


user = "twitter-bot-challenge-user"
password = "1Dci5pk0UHGBUzpN"
cluster_address = "loadtests.mjmdg.mongodb.net"

# Replace this value with the connection string for your own MongoDB database
mongodb_uri = (
    "mongodb+srv://"
    + user
    + ":"
    + password
    + "@"
    + cluster_address
    + "/test?retryWrites=true&w=majority"
)


def post_tweet():
    # Connect to the database
    client = pymongo.MongoClient(mongodb_uri)
    ohlcv_db = client["ohlcv_db"]
    posts_db = client["posts_db"]

    # Query the ohlcv_db and posts_db collections to get the pair_to_post and market values
    pair_to_post, market_values = get_pair_to_post(ohlcv_db, posts_db)

    # Compose the message
    message = compose_message(pair_to_post, market_values)

    # Authenticate with the Twitter API
    auth = tweepy.OAuth1UserHandler(
        consumer_key, consumer_secret, access_token, access_token_secret
    )
    api = tweepy.API(auth)

    # Get the latest tweet from the posts_db collection
    latest_tweet = (
        posts_db.find({"pair": pair_to_post["pair"]}).sort("timestamp", -1).limit(1)
    )

    if latest_tweet:
        # If the latest tweet exists, get the tweet_id of the first tweet in the thread
        thread_id = latest_tweet[0]["tweet_id"]
    else:
        # If the latest tweet does not exist, set the thread_id to None
        thread_id = None

    # Post the tweet to the corresponding Twitter thread or as a new tweet
    tweet = api.update_status(status=message, in_reply_to_status_id=thread_id)

    # Add the tweet to the posts_db collection
    post = {
        "pair": pair_to_post["pair"],
        "timestamp": datetime.datetime.utcnow(),
        "message": message,
        "tweet_id": tweet.id,
    }
    posts_db.insert_one(post)


def get_pair_to_post(ohlcv_db, posts_db):
    # Query ohlcv_db for the top 100 pairs by compound volume
    top_pairs = ohlcv_db.find({"compound_volume": -1}).limit(100)

    # Create a tailable cursor for the latest documents corresponding to those 100 pairs
    latest_posts = (
        posts_db.data.locations.find({"pair": {"$in": top_pairs}})
        .sort("timestamp", 1)
        .add_option(pymongo.cursor.CursorType.TAILABLE)
    )

    # Convert the cursor to a list of documents
    sorted_pairs = list(map(lambda x: x, latest_posts))

    # Sort results by the oldest timestamp to find the pairs that haven't been posted for a while,
    # then corresponding volume to find the biggest markets among them
    sorted_pairs = sorted(sorted_pairs, key=lambda x: (x["timestamp"], x["volume"]))

    # Select the pair_to_post
    pair_to_post = sorted_pairs[0]

    # Get the corresponding latest volumes by market values from ohlcv_db
    market_values = ohlcv_db.find({"pair": pair_to_post["pair"]})

    return pair_to_post, market_values


def compose_message(pair_to_post, market_values):
    # Initialize the message string
    message = f"Top Market Venues for {pair_to_post['pair']}:\n"

    # Iterate over the market values and append each market and its volume to the message
    for market_value in market_values:
        message += f"{market_value['market']}: {market_value['volume']}%\n"

    # Return the composed message
    return message


if __name__ == "__main__":
    message = compose_message(
        {"pair": "BTC/USD"},
        [
            {"market": "Bitstamp", "volume": 0.5},
            {"market": "Coinbase", "volume": 0.3},
            {"market": "Kraken", "volume": 0.2},
        ],
    )
    print(message)
    # OUTPUT:
    # Top Market Venues for BTC/USD:
    # Bitstamp: 0.5%
    # Coinbase: 0.3%
    # Kraken: 0.2%

    ohlcv_db = (
        pymongo.MongoClient(mongodb_uri)
        .get_database("ohlcv_db")
        .get_collection("ohlcv")
    )
    posts_db = (
        pymongo.MongoClient(mongodb_uri)
        .get_database("posts_db")
        .get_collection("posts")
    )
    post = get_pair_to_post(
        ohlcv_db,
        posts_db,
    )
    print(post)

    post_tweet()
