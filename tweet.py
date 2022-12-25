# ## Required functionality

# A Python function will compose and post a tweet with the latest values from [MongoDB time series](https://www.mongodb.com/docs/manual/core/timeseries-collections/) collections using the logic described below.

# ## Algo

# 1. query `ohlcv_db` for the top 100 `pair`s by compound `volume`
# 1. query `posts_db` for the latest documents corresponding to those 100 `pair`s
# 1. sort results by the oldest `timestamp` to find the pairs that haven't been posted for a while, then corresponding `volume` to find the biggest markets among them and select the `pair_to_post`
# 1. compose `message_to_post` for the `pair_to_post` with corresponding latest `volume`s by `market` values from `ohlcv_db` using this example:
#    ```
#    Top Market Venues for BTC-USDC:
#    Binance 30.10%
#    Coinbase 20.20%
#    Kraken 10.30%
#    Bitstamp 5.40%
#    Huobi 2.50%
#    Others 31.5%
#    ```
# 1. keep similar tweets in one thread. if `pair_to_post` tweets already exists in `posts_db`, post tweet to the corresponding Twitter thread. else, post a new tweet.
# 1. add your `message_to_post` to `posts_db`


#  Python function will compose and post a tweet with the latest values from MongoDB time series collections using the logic described below.
# 1. query `ohlcv_db` for the top 100 `pair`s by compound `volume`
# 1. query `posts_db` for the latest documents corresponding to those 100 `pair`s
# 1. sort results by the oldest `timestamp` to find the pairs that haven't been posted for a while, then corresponding `volume` to find the biggest markets among them and select the `pair_to_post`
# 1. compose `message_to_post` for the `pair_to_post` with corresponding latest `volume`s by `market` values from `ohlcv_db` using this example:
#    ```
#    Top Market Venues for BTC-USDC:
#    Binance 30.10%
#    Coinbase 20.20%
#    Kraken 10.30%
#    Bitstamp 5.40%
#    Huobi 2.50%
#    Others 31.5%
#    ```

# 1. keep similar tweets in one thread. if `pair_to_post` tweets already exists in `posts_db`, post tweet to the corresponding Twitter thread. else, post a new tweet.
# 1. add your `message_to_post` to `posts_db`


# Solution

# Import libraries
import datetime
import pymongo
import tweepy
import pandas as pd
import numpy as np


# Set environment variables
CONSUMER_KEY = "NnPL65juj8nstnd6x5t4tECun"
CONSUMER_SECRET = "i0yD4lmam6mBDqTaLlCOSQ5DdP38yj8yeqZ2ezNy3GRHYw4Zku"
ACCESS_TOKEN = "1600087262909317120-WaeIU8aBbV0QkyML1U3xtzZP1NHeAO"
ACCESS_TOKEN_SECRET = "uNdcHPWMzR8uruFHoXEFKylue5VmOExki4PZn3omw9x3U"

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
# Connect to MongoDB
client = pymongo.MongoClient(mongodb_uri)
db = client["metrics"]
ohlcv_collection = db["ohlcv_db"]
posts_collection = db["posts_db"]
# ohlcv_collection = ohlcv_db["ohlcv_collection"]
# posts_collection = posts_db["posts_collection"]


# Connect to Twitter
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)


# Query `ohlcv_db` for the top 100 `pair`s by compound `volume`
def get_top_pairs():
    top_pairs = list(ohlcv_collection.find().sort("volume", -1).limit(5))
    return top_pairs


# Query `posts_db` for the latest documents corresponding to those 100 `pair`s
def get_latest_posts():
    latest_posts = list(posts_collection.find().sort("timestamp", -1).limit(5))
    return latest_posts


# Sort results by the oldest `timestamp` to find the pairs that haven't been posted for a while, then corresponding `volume` to find the biggest markets among them and select the `pair_to_post`
def get_pair_to_post():
    top_pairs = get_top_pairs()
    latest_posts = get_latest_posts()
    top_pairs_df = pd.DataFrame(top_pairs)
    latest_posts_df = pd.DataFrame(latest_posts)
    merged_df = pd.merge(top_pairs_df, latest_posts_df, on="_id", how="left")
    merged_df["timestamp"] = pd.to_datetime(merged_df["timestamp"])
    merged_df["timestamp"] = merged_df["timestamp"].fillna(datetime.datetime.now())
    merged_df["delta"] = datetime.datetime.now() - merged_df["timestamp"]
    merged_df["delta"] = merged_df["delta"].dt.days
    merged_df = merged_df.sort_values(by="delta", ascending=True)
    pair_to_post = merged_df["_id"].iloc[0]
    return pair_to_post


# Compose `message_to_post` for the `pair_to_post` with corresponding latest `volume`s by `market` values from `ohlcv_db` using this example:
#    ```
#    Top Market Venues for BTC-USDC:

#    Binance 30.10%

#    Coinbase 20.20%

#    Kraken 10.30%

#    Bitstamp 5.40%

#    Huobi 2.50%

#    Others 31.5%

#    ```


def get_message_to_post():
    pair_to_post = get_pair_to_post()
    top_pairs = get_top_pairs()
    top_pairs_df = pd.DataFrame(top_pairs)
    top_pairs_df = top_pairs_df[top_pairs_df["_id"] == pair_to_post]
    top_pairs_df = top_pairs_df.reset_index(drop=True)
    top_pairs_df = top_pairs_df.rename(columns={"_id": "pair"})
    top_pairs_df = top_pairs_df.rename(columns={"volume": "pair_volume"})
    # top_pairs_df = top_pairs_df.drop(columns=["_id"])
    # ohlcv_df = pd.DataFrame(list(ohlcv_collection.find()))
    ohlcv_df = ohlcv_df[ohlcv_df["pair"] == pair_to_post]
    ohlcv_df["timestamp"] = pd.to_datetime(ohlcv_df["timestamp"])
    ohlcv_df = ohlcv_df.sort_values(by="timestamp", ascending=False)
    ohlcv_df = ohlcv_df.drop_duplicates(subset="market")
    ohlcv_df = ohlcv_df.reset_index(drop=True)
    merged_df = pd.merge(top_pairs_df, ohlcv_df, on="pair", how="left")
    merged_df["market_share"] = merged_df["volume"] / merged_df["pair_volume"]
    merged_df["market_share"] = merged_df["market_share"].apply(
        lambda x: round(x * 100, 2)
    )
    merged_df = merged_df.sort_values(by="market_share", ascending=False)
    message_to_post = "Top Market Venues for " + pair_to_post + ":\n"
    for i in range(0, len(merged_df)):
        market = merged_df["market"].iloc[i]
        market_share = merged_df["market_share"].iloc[i]
        message_to_post = message_to_post + market + " " + str(market_share) + "%\n"
    return message_to_post


# Post `message_to_post` to Twitter
def post_to_twitter():
    message_to_post = get_message_to_post()
    api.update_status(message_to_post)


# Run the script
if __name__ == "__main__":
    post_to_twitter()
