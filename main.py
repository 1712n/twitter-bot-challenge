import sys

import pymongo
import tweepy
import datetime
import logging

from database import Database
from twitter import Twitter

use_twitter = True  # change to True if you want the tweet to be actually posted


def get_top_pairs(db):
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

    res = db.ohlcv().aggregate([
        select_granularity,
        limit_data,
        sort_data,
        top_100
    ])

    return [(x["_id"]["pair"], x["volume"]) for x in res]


def get_latest_posts(db, top_pairs):
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

    res = db.posts().aggregate([
        match_pairs,
        find_latest,
        sort_latest,
    ])

    return [(x["_id"], x["time"]) for x in res]


def select_pair_to_post(top_pairs, latest_pairs, db):
    # The instructions are unclear, the exact algorithm to choose the pair to post is not given
    # So I'm just going to pick the biggest pair by volume that's in latest_pairs

    # First, try to find the most voluminous pair that hasn't been posted lately
    latest_pairs_ids = [x[0] for x in latest_pairs]
    for (pair, volume) in top_pairs:
        if pair not in latest_pairs_ids:
            return pair, volume
    # If such pair does not exist, simply post the most voluminous pair
    return top_pairs[0]


def choose_message(db, top_pairs):
    message = None
    pair_to_post = None
    for pair in top_pairs:
        duplicate_message = db.posts().aggregate([{
            "$match": {
                "pair":  pair[0]
            }
        }])
        messages = list(duplicate_message)
        if len(messages) == 0:
            message = compose_message(db, pair)
            pair_to_post = pair
            break
    if message == None:
        logging.info("All recent pairs have already been posted; nothing else to post.")
        sys.exit(0)
    return(message, pair_to_post)


def compose_message(db, pair_to_post):
    pair = pair_to_post[0]
    symbol = pair.split('-')[0].lower()
    base = pair.split('-')[1].lower()

    select_granularity = {"$match": {"granularity": "1h"}}
    find_the_pair = {
        "$match": {
            "pair_symbol": symbol,
            "pair_base": base
        }
    }
    sort_by_timestamp = {
        "$sort": {
            "timestamp": pymongo.DESCENDING
        }

    }

    tickers = db.ohlcv().aggregate([
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
    percentages = [(key, (value / total_volume)) for (key, value) in items]
    others = 1.0 - sum([x[1] for x in percentages])
    response = f"Top Market Venues for  {pair}:\n"
    for (market, value) in percentages:
        percentage = str(round(value * 100, 1))
        venue = market.capitalize()
        response += f"{venue}: {percentage}%\n"
    percentage = str(round(others * 100, 1))
    response += f"Others: {percentage}%"
    return response


def post_message_to_db(db, pair, message):
    post = {
        "pair": pair,
        "tweet_text": message,
        "time": datetime.datetime.fromisoformat(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
    }
    db.posts().insert_one(post)

def post_message_to_twitter(db, pair, message):
    parent = db.posts().find({
        'pair': pair,
        'tweet_id': {
            '$exists': True
        }
    }) \
        .sort('time', pymongo.DESCENDING) \
        .limit(1)

    parent = list(parent)

    # input("press to continue...") # to work around a problem with Twitter being blocked in my country

    if len(parent) == 0:
        Twitter().post_new_tweet(message)
    else:
        Twitter().post_reply(message, parent[0]["tweet_id"])


if __name__ == '__main__':
    db = Database()

    top_pairs = None
    try:
        top_pairs = get_top_pairs(db)
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error reading ohlcv: {e}")
        sys.exit(1)

    latest_pairs = None
    try:
        latest_pairs = get_latest_posts(db, top_pairs)
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error reading posts: {e}")
        sys.exit(1)

    message = None
    pair_to_post = None
    try:
        message, pair_to_post = choose_message(db, top_pairs)
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error reading posts: {e}")
        sys.exit(1)

    #message = None
    #try:
    #    message = compose_message(db, pair_to_post)
    #except pymongo.errors.PyMongoError as e:
    #    logging.error(f"Error reading ohlcv: {e}")
    #    sys.exit(1)

    try:
        post_message_to_db(db, pair_to_post, message)
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error adding to posts: {e}")
        sys.exit(1)

    if use_twitter:
        try:
            post_message_to_twitter(db, pair_to_post, message)
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error reading posts: {e}")
            sys.exit(1)
        except tweepy.errors.TweepyException as e:
            logging.error(f"Error posting to Twitter: {e}")
            sys.exit(1)

