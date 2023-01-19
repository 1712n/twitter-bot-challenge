import os
import tweepy
import logging
from database import connect_db, choose_pair, get_markets, get_pair_thread, save_tweet
from twitter import connect_twitter, tweet


def compose_message(pair, compound_volume, markets):
    sorted_markets = sorted(markets, key=lambda market: markets[market], reverse=True)
    message = f"Top Market Venues for {pair}"
    others_percent = 0

    for i in range(len(markets)):
        percent = markets[sorted_markets[i]] / compound_volume * 100

        if i >= 5:
            others_percent += percent
        else:
            message += f"\n{sorted_markets[i].upper()}: {round(percent, 2)}%"

    if others_percent > 0:
        message += f"\nOthers: {round(others_percent, 2)}%"

    return message


def main():
    if os.environ.get("DEBUG"):
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]: %(message)s", level=level)

    db_client = connect_db()
    twitter_api = connect_twitter()

    pair, compound_volume = choose_pair(db_client)
    markets = get_markets(db_client, pair)

    message = compose_message(pair, compound_volume, markets)
    logging.info("Posting and saving this message: \n %s", message)
    thread_id = get_pair_thread(db_client, pair)
    tweet_id = tweet(twitter_api, message, in_reply_to=int(thread_id))
    save_tweet(db_client, message, tweet_id, pair)

    db_client.close()

    logging.info("Finished successfully!")


if __name__ == '__main__':
    main()

