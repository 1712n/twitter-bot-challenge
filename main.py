# Project logging
from core.log import logger
# For sleep
import time

# Logic
from util.pairs import get_top_pairs
from util.posts import select_pair
from util.messages import add_message
from util.messages import compose_message
from util.messages import send_message


def main():
    logger.info('The app started')

    # db = get_db()
    steps = 0
    sleep_time = 2
    while steps < 5:
        # query ohlcv_db for the top 100 pairs by compound volume
        top_pairs = get_top_pairs()
        if len(top_pairs) == 0:
            logger.info(f"Query for top pairs failed. Sleeping for {sleep_time} ...")
            time.sleep(sleep_time)
            steps = 0
            continue
        steps += 1
        logger.info(f"Got top pairs, qty: {len(top_pairs)}")
        logger.debug(f"top_pairs: {top_pairs}")

        # query posts_db for the latest documents corresponding to those 100 pairs
        # I don't need it??????
        # get_latest_posts(top_pairs)
        # steps += 1
        # sort results by the oldest timestamp to find the pairs that
        # haven't been posted for a while, then corresponding volume to find
        # the biggest markets among them and select the pair_to_post
        pair_to_post = select_pair(top_pairs)
        if not pair_to_post:
            logger.critical(f"Query and selection for pair_to_post failed")
            steps = 0
            continue
        steps += 1
        logger.info(f"Selected pair_to_post: {pair_to_post}")
        # compose message_to_post for the pair_to_post with corresponding
        # latest volumes by market values from ohlcv_db
        msg_text = compose_message(pair_to_post=pair_to_post)
        if not msg_text:
            logger.critical(f"Failed to compose a message text")
            continue
        steps += 1
        logger.info(f"Text message composed")
        # keep similar tweets in one thread. if pair_to_post tweets already exists in
        # posts_db, post tweet to the corresponding Twitter thread. else, post a new tweet.
        send_message(pair=pair_to_post, text=msg_text)
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

    logger.info('The app finished')


if __name__ == '__main__':
    main()
