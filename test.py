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


# def post_tweet():
#     # Connect to the database
#     client = pymongo.MongoClient(mongodb_uri)
#     ohlcv_db = client["ohlcv_db"]
#     posts_db = client["posts_db"]

#     # Query the ohlcv_db and posts_db collections to get the pair_to_post and market values
#     pair_to_post, market_values = get_pair_to_post(ohlcv_db, posts_db)

#     # Compose the message
#     message = compose_message(pair_to_post, market_values)

#     # Authenticate with the Twitter API
#     auth = tweepy.OAuth1UserHandler(
#         consumer_key, consumer_secret, access_token, access_token_secret
#     )
#     api = tweepy.API(auth)

#     # Get the latest tweet from the posts_db collection
#     latest_tweet = (
#         posts_db.find({"pair": pair_to_post["pair"]}).sort("timestamp", -1).limit(1)
#     )

#     if latest_tweet:
#         # If the latest tweet exists, get the tweet_id of the first tweet in the thread
#         thread_id = latest_tweet[0]["tweet_id"]
#     else:
#         # If the latest tweet does not exist, set the thread_id to None
#         thread_id = None

#     # Post the tweet to the corresponding Twitter thread or as a new tweet
#     tweet = api.update_status(status=message, in_reply_to_status_id=thread_id)

#     # Add the tweet to the posts_db collection
#     post = {
#         "pair": pair_to_post["pair"],
#         "timestamp": datetime.datetime.utcnow(),
#         "message": message,
#         "tweet_id": tweet.id,
#     }
#     posts_db.insert_one(post)



# Connect to the MongoDB database
client = pymongo.MongoClient(mongodb_uri)
db = client["metrics"]
ohlcv_collection = db["ohlcv_db"]
posts_collection = db["posts_db"]

# Query the ohlcv_db collection for the top 100 pairs by compound volume
top_pairs = list(ohlcv_collection.find().sort([("volume", pymongo.DESCENDING)]).limit(5))
# print(top_pairs)
# Query the posts_db collection for the latest documents corresponding to the top 100 pairs
top_posts = posts_collection.find().sort([("time", pymongo.DESCENDING)]).limit(5)
# posts = top_posts.find({"timestamp": {"$in": top_pairs}})
# print(top_pairs[0].get("timestamp"))
# top_timestamps = top_pairs[0].get("timestamp")
# top_time = top_posts[0].get("time")

# Get the corresponding pairs between top_timestamps and top_time
pairs = []
for pair in top_pairs:
    for post in top_posts:
        if pair.get("timestamp") == post.get("time"):
            pairs.append(pair)
print(pairs)


# # Sort the results by the oldest timestamp to find the pairs that haven't been posted for a while,
# # then corresponding volume to find the biggest markets among them and select the pair_to_post
# sorted_posts = posts.sort([("timestamp", pymongo.ASCENDING), ("volume", pymongo.DESCENDING)])
# pair_to_post = sorted_posts

# # Compose the message_to_post for the pair_to_post with corresponding latest volumes by market values
# # from the ohlcv_db collection
# markets = ohlcv_collection.data.locations.find({"pair": pair_to_post})
# message_to_post = "Top Market Venues for {}:".format(pair_to_post)
# total_volume = 0
# for market in markets:
#     total_volume += market["volume"]
# for market in markets:
#     message_to_post += "\n{} {:.2f}%".format(market["market"], market["volume"] / total_volume * 100)

# # Use the Tweepy library to connect to the Twitter API and post the tweet
# auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
# api = tweepy.API(auth)

# # Check if the pair_to_post tweets already exists in posts_db
# tweet = None
# for post in posts:
#     if post["pair"] == pair_to_post:
#         tweet = api.get_status(post["tweet_id"])
#         break

# # If the tweet exists, use the update_status method to post the tweet to the corresponding Twitter thread
# if tweet:
#     api.update_status(message_to_post, in_reply_to_status_id=tweet.id)

# # If the tweet does not exist, use the update_status method to post a new tweet
# else:
#     tweet = api.update_status(message_to_post)

# # Add the message_to_post to the posts_db collection
# posts_collection.insert_one({"pair": pair_to_post, "message": message_to_post, "tweet_id": tweet.id})
