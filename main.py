# Project logging
from core.log import logger
# For sleep
import time
# Formatting
from pprint import pformat

# Logic
from util.pairs import get_top_pairs
from util.posts import select_pair_to_post
from util.messages import add_message_to_posts
from util.messages import compose_message_to_post
from util.messages import send_message


def main():
    logger.info('The app started')

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
        logger.debug(f"Top pairs: {top_pairs}")

        # query posts_db for the latest documents corresponding to those 100 pairs
        # sort results by the oldest timestamp to find the pairs that
        # haven't been posted for a while, then corresponding volume to find
        # the biggest markets among them and select the pair_to_post
        pair_to_post = select_pair_to_post(top_pairs)
        if not pair_to_post:
            logger.critical(f"Query and selection for pair_to_post failed")
            steps = 0
            continue
        steps += 1
        logger.info(f"Selected pair_to_post: {pair_to_post}")

        # compose message_to_post for the pair_to_post with corresponding
        # latest volumes by market values from ohlcv_db
        message_to_post = compose_message_to_post(pair_to_post=pair_to_post)
        if not message_to_post:
            logger.critical(f"Failed to compose a message text")
            continue
        steps += 1
        logger.info(f"Text message_to_post composed: {pformat(message_to_post)}")

        # keep similar tweets in one thread. if pair_to_post tweets already exists in
        # posts_db, post tweet to the corresponding Twitter thread. else, post a new tweet.
        tweet_id = send_message(pair=pair_to_post, text=message_to_post)
        if not tweet_id:
            logger.critical(f"Failed to send tweet")
        steps += 1
        logger.info(f"Tweet created, id: {tweet_id}")

        # add your message_to_post to posts_db
        post_id = add_message_to_posts(
            pair=pair_to_post,
            tweet_id=tweet_id,
            text=message_to_post,
        )
        steps += 1
        logger.info(f"Post added, id: {post_id}")

    if steps < 5:
        logger.info('The app finished successfully')
    else:
        logger.critical('The app finished with failure')


if __name__ == '__main__':
    main()
