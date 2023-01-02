import pymongo
import os
import datetime
import tweepy

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]
consumer_key = os.environ["TW_CONSUMER_KEY"]
consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"]
access_token = os.environ["TW_ACCESS_TOKEN"]
access_token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = pymongo.MongoClient(uri)

if __name__ == '__main__':
    ############### part 1 of the algorithm

    select_granularity = {"$match": {"granularity": "1h"}}
    limit_data = {
        "$group": {
            "_id": {
                "pair": {"$toUpper": {"$concat": ["$pair_symbol", "-", "$pair_base"]}},
            },
            "volume": {
                "$sum": {"$toDouble": "$volume"}
            }
        }
    }
    sort_data = {
        "$sort": {
            "volume": pymongo.DESCENDING
        }
    }
    top_100 = {"$limit": 100}

    res = client["metrics"]["ohlcv_db"].aggregate([
        select_granularity,
        limit_data,
        sort_data,
        top_100
    ])

    top_pairs = [(x["_id"]["pair"], x["volume"]) for x in res]

    ####################### part 2 of the algorithm

    match_pairs = {
        "$match": {
            "pair": {"$in": [x[0] for x in top_pairs]}
        }
    }
    find_latest = {
        "$group": {
            "_id": "$pair",
            "time": {
                "$max": "$time"
            }
        }
    }
    sort_latest = {
        "$sort": {
            "time": pymongo.DESCENDING
        }
    }

    res = client["metrics"]["posts_db"].aggregate([
        match_pairs,
        find_latest,
        sort_latest,
    ])

    latest_pairs = [(x["_id"], x["time"]) for x in res]

    #################### Selecting pair_to_post

    # The instructions are unclear, the exact algorithm to choose the pair to post is not given
    # So I'm just going to pick the biggest pair by volume that's in latest_pairs

    latest_pairs_ids = [x[0] for x in latest_pairs]
    pair_to_post = None
    for (pair, volume) in top_pairs:
        if pair in latest_pairs_ids:
            pair_to_post = (pair, volume)
            break

    # print(pair_to_post)

    ############### Composing message

    pair = pair_to_post[0]
    symbol = pair.split('-')[0].lower()
    base = pair.split('-')[1].lower()

    find_the_pair = {
        "$match": {
            "pair_symbol":symbol,
            "pair_base":base
        }
    }
    sort_by_timestamp = {
        "$sort": {
                "timestamp": pymongo.DESCENDING
        }

    }

    tickers = client["metrics"]["ohlcv_db"].aggregate([
        select_granularity,
        find_the_pair,
        sort_by_timestamp
    ])
    first_item = next(tickers)
    current_date = first_item["timestamp"]
    market_venues = {
        first_item["marketVenue"]: float(first_item["volume"])
    }
    for x in tickers:
        if x["timestamp"] == current_date:
            venue = x["marketVenue"]
            volume = x["volume"]
            if venue in market_venues.keys():
                market_venues[venue] += float(volume)
            else:
                market_venues[venue] = float(volume)
        else:
            break

    total_volume = sum(market_venues.values())
    items = sorted(market_venues.items(), key=lambda x: x[1], reverse=True)[:5]
    percentages = [(key, (value/total_volume)) for (key, value) in items]
    others = 1.0 - sum([x[1] for x in percentages])
    #print(percentages)
    #print(others)
    response = f"Top Market Venues for {pair}:\n"
    for (market, value) in percentages:
        percentage = str(round(value*100, 1))
        venue = market.capitalize()
        response += f"{venue}: {percentage}%\n"
    percentage = str(round(others*100, 1))
    response += f"Others: {percentage}%"
    print(response)

    ############ adding the tweet to posts

    post = {
        "pair": pair,
        "tweet_text": response,
        "time": datetime.datetime.fromisoformat(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
    }
    client["metrics"]["posts_db"].insert_one(post)

    ##### posting the tweet

    use_twitter = True

    if use_twitter:
        twitter = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )

        parent = client["metrics"]["posts_db"].find({
            'pair':pair,
            'tweet_id': {
                '$exists': True
            }
        })\
            .sort('time',pymongo.DESCENDING)\
            .limit(1)
        parent = list(parent)
        print(parent[0])
        #input("press to continue...") ## to work around a problem with Twitter being blocked in my country
        if parent is []:
            twitter.create_tweet(text=response)
        else:
            twitter.create_tweet(text=response, in_reply_to_tweet_id=parent[0]["tweet_id"])