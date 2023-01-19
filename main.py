# Project logging
from core.log import log
# For mongodb
# from pymongo import MongoClient
# from pymongo.errors import ConnectionFailure
# import pymongo.database
# Temporary for printing
# import pprint

# For mongodb
# from db.session import get_db
# from core.config import settings
# Logic
from util.pairs import get_top_pairs
from util.posts import get_latest_posts
from util.posts import select_pair
from util.posts import add_message
from util.twitter import compose_message
from util.twitter import send_message

import time

def main():
    log.logger.info('The app started')

    # db = get_db()
    steps = 0
    while steps < 6:
        # query ohlcv_db for the top 100 pairs by compound volume
        top_pairs = get_top_pairs()
        if len(top_pairs) == 0:
            sleep_time = 2
            log.logger.info(f"Query for top pairs failed. Sleeping for {sleep_time} ...")
            time.sleep(sleep_time)
            steps = 0
            continue
        steps += 1

        # query posts_db for the latest documents corresponding to those 100 pairs
        get_latest_posts(top_pairs)
        steps += 1
        # sort results by the oldest timestamp to find the pairs that
        # haven't been posted for a while, then corresponding volume to find
        # the biggest markets among them and select the pair_to_post
        select_pair()
        steps += 1
        # compose message_to_post for the pair_to_post with corresponding
        # latest volumes by market values from ohlcv_db
        compose_message()
        steps += 1
        # keep similar tweets in one thread. if pair_to_post tweets already exists in
        # posts_db, post tweet to the corresponding Twitter thread. else, post a new tweet.
        send_message()
        steps += 1
        # add your message_to_post to posts_db
        add_message()
        steps += 1


    # try:
    #     collections = [settings.PAIRS_NAME, settings.POSTS_NAME]
    #     for coll in collections:
    #         for content in db[coll].find().limit(4):
    #             log.logger.debug(f"collection: {coll} object: {content}")
    # except Exception as e:
    #     log.logger.debug(f"list_databases... failed: {e}")

    log.logger.info('The app finished')


if __name__ == '__main__':
    main()
