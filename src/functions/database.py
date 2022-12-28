from datetime import datetime, timedelta
import logging
import pandas as pd
import pymongo
from .config import MONGO_DB_ADDRESS, MONGODB_USER, MONGODB_PASSWORD


def get_mongodb_client():
    """
    Connect to MongoDB.

    Returns:
    MongoClient -- MongoDB client
    """
    conn_str = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGO_DB_ADDRESS}"
    logging.info("Connecting to MongoDB.")
    mongo_client = pymongo.MongoClient(conn_str)
    logging.info("Connected to MongoDB.")
    return mongo_client


def get_pair_to_post(ohlcv_db, posts_db, timeout=5000, time_range=1):
    """
    Choose trading pair to post.

    Keyword arguments:
    ohlcv_db -- ohlcv database
    posts_db -- posts database
    timeout -- maximum time to allow the operation to run (default 5000)
    time_range -- query only for documents with timestamps in the last `time_range` hours (default 1)
    Returns:
    str -- trading pair
    str -- post id
    str -- pair symbol
    str -- pair base
    """

    logging.info("Starting get_pair_to_post function.")

    # 1. query ohlcv_db for the top 100 pairs by compound volume
    logging.info(
        "Querying ohlcv_db for the top 100 pairs by compound volume in the last %i hours...",
        time_range,
    )
    result = list(
        ohlcv_db.aggregate(
            [
                # filter for the last hour
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.now().astimezone()
                            - timedelta(hours=time_range)
                        },
                    }
                },
                # group by pair, calculate volume sum
                {
                    "$group": {
                        "_id": {"$concat": ["$pair_symbol", "/", "$pair_base"]},
                        "volume_sum": {"$sum": {"$toDouble": "$volume"}},
                    }
                },
                # sort by volume sum
                {"$sort": {"volume_sum": -1}},
                # get top 100 pairs
                {"$limit": 100},
            ],
            maxTimeMS=timeout,
        )
    )
    if len(result) == 0:
        raise ValueError("No pairs for the given time range.")

    logging.info("Found %i pairs.", len(result))

    pairs = ["-".join(map(str.upper, i["_id"].split("/"))) for i in result]
    result = pd.DataFrame(result, index=pairs)

    # 2. query posts_db for the latest documents corresponding to those 100 pairs

    logging.info(
        "Querying posts_db for the latest documents corresponding to found pairs..."
    )
    posts = list(
        posts_db.aggregate(
            [
                # group by pair, calculate latest timestamp and save its id
                {
                    "$group": {
                        "_id": "$pair",
                        "latest_time": {"$max": {"time": "$time", "post_id": "$_id"}},
                    }
                },
                # filter for top 100 pairs by volume sum
                {"$match": {"_id": {"$in": pairs}}},
            ],
            maxTimeMS=timeout,
        )
    )

    # 3. sort results by the oldest timestamp to find the pairs that haven't been posted for a while,
    # then corresponding volume to find the biggest markets among them and select the pair_to_post.
    # If all pairs have been posted at least once, post the one that hasn't been posted for the longest time.
    # Else post any that has never been posted.
    if len(posts) == 0:
        logging.info("No matching posts found, sorting only by volume.")
        result.sort_values(by="volume_sum", ascending=False, inplace=True)
    else:
        logging.info("Found %i matching posts.", len(posts))
        posts = pd.DataFrame(
            [i["latest_time"] for i in posts], index=[i["_id"] for i in posts]
        )

        result = result.join(posts, how="left")
        result["time"].fillna(datetime.fromisoformat("1970-01-01"), inplace=True)
        result.sort_values(
            by=["time", "volume_sum"], ascending=[True, False], inplace=True
        )

    pair_to_post = result.iloc[0]
    logging.info("Selected pair %s.", result.index.values[0])
    # fmt: off
    return [result.index.values[0], pair_to_post["post_id"]] + pair_to_post["_id"].split("/")
    # fmt: on


def get_message_to_post(
    pair, pair_symbol, pair_base, ohlcv_db, timeout=5000, time_range=1
):
    """
    Compose message to post.

    Keyword arguments:
    pair -- pair name
    pair_symbol -- pair symbol
    pair_base -- pair base
    ohlcv_db -- ohlcv database
    timeout -- maximum time to allow the operation to run (default 5000)
    time_range -- query only for documents with timestamps in the last `time_range` hours (default 1)
    Returns:
    str -- message to post
    """

    logging.info("Starting get_message_to_post function.")
    # 4. compose message_to_post for the pair_to_post with corresponding latest volumes by market values from ohlcv_db using this example:
    logging.info(
        "Querying ohlcv_db for the latest volumes by market values for pair %s in the last %i hours...",
        pair,
        time_range,
    )
    result = list(
        ohlcv_db.aggregate(
            [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.now().astimezone()
                            - timedelta(hours=time_range)
                        },
                        "pair_symbol": pair_symbol,
                        "pair_base": pair_base,
                    }
                },
                {
                    "$group": {
                        "_id": "$marketVenue",
                        "volume": {
                            "$max": {
                                "time": "$timestamp",
                                "value": {"$toDouble": "$volume"},
                            }
                        },
                    }
                },
            ],
            maxTimeMS=timeout,
        )
    )
    if len(result) == 0:
        raise ValueError("No documents for given pair found.")

    logging.info("Found %i markets.", len(result))
    result = (
        pd.DataFrame(
            [{"_id": i["_id"], "market_volume": i["volume"]["value"]} for i in result]
        )
        .sort_values(by="market_volume", ascending=False)
        .reset_index(drop=True)
    )

    message_to_post = f"Top Market Venues for {pair}:\n"

    if len(result) <= 6:
        # If there are 6 or less markets, post all of them
        volume_sum = result["market_volume"].sum()
        for _, i in result.iterrows():
            message_to_post += (
                f"{i['_id'].capitalize()} {(i['market_volume']/volume_sum)*100:.2f}%\n"
            )
    else:
        # Else post the top 5 markets and the rest as "Others"
        volume_sum = result["market_volume"].sum()
        for idx, i in result.iterrows():
            if idx == 5:
                break
            message_to_post += (
                f"{i['_id'].capitalize()} {(i['market_volume']/volume_sum)*100:.2f}%\n"
            )
        message_to_post += (
            f"Others {(1 - result['market_volume'][:5].sum()/volume_sum)*100:.2f}%\n"
        )

    logging.info("Message to post constructed.")
    return message_to_post


def add_post_to_db(pair, tweet_id, tweet_text, posts_db):
    """
    Add post to posts_db. Returns id of the new post.

    Keyword arguments:
    pair -- pair name
    tweet_id -- id of the tweet
    tweet_text -- text of the tweet
    posts_db -- posts database
    Returns:
    int -- id of the new post
    """
    logging.info("Adding tweet with id %s to posts_db...", tweet_id)
    new_post = posts_db.insert_one(
        {
            "pair": pair,
            "time": datetime.now().astimezone(),
            "tweet_text": tweet_text,
            "tweet_id": tweet_id,
        }
    )
    logging.info("Tweet added to posts_db with id %s.", new_post.inserted_id)
    return new_post.inserted_id
