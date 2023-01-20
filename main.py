# Project logging
from core.log import logger

# For sleep
import time
# Formatting
from pprint import pformat

# Project settings
from core.config import settings
# Logic
from util.pairs import get_top_pairs
from util.posts import select_pair_to_post
from util.messages import add_message_to_posts
from util.messages import compose_message_to_post
from util.messages import send_message


# 1. Gather all data
def get_data() -> tuple[str | None, str | None, str | None]:
    POINTS = 3
    err = None
    # 1.1. Top pairs by compound volume
    top_pairs = get_top_pairs()
    if not top_pairs or not len(top_pairs):
        err = f"Failed to get top pairs"
        logger.info(err)
        return err, None, None
    logger.info(f"[1/{POINTS}] Got top pairs, qty: {len(top_pairs)}")
    logger.debug(f"Top pairs: {top_pairs}")

    # 1.2. pair_to_post
    pair_to_post = select_pair_to_post(top_pairs)
    if not pair_to_post or not len(pair_to_post):
        err = f"Query and selection for pair_to_post failed"
        logger.critical(err)
        return err, None, None
    logger.info(f"[2/{POINTS}] Selected pair_to_post: {pair_to_post}")

    # 1.3. compose message_to_post for the pair_to_post
    message_to_post = compose_message_to_post(pair_to_post=pair_to_post)
    if not message_to_post or not len(message_to_post):
        err = f"Failed to compose a message_to_post"
        logger.critical(err)
        return err, None, None
    logger.info(f"[3/{POINTS}] Text message_to_post composed: {pformat(message_to_post)}")

    return None, pair_to_post, message_to_post

# 2. Publish data
def publish_data(
        pair_to_post: str,
        message_to_post: str,
        retry: int = 2,
        interval: int = 5,
) -> tuple[str | None, str | None, str | None]:
    POINTS = 2
    err = None

    # 2.1. Send message_to_post to Twitter
    tweet_id = send_message(pair=pair_to_post, text=message_to_post)
    if not tweet_id or not len(str(tweet_id)):
        err = f"Failed to send tweet"
        logger.critical(err)
        return err, None, None
    logger.info(f"[1/{POINTS}] Tweet created, id: {tweet_id}")

    # 2.2. Put message_to_post to posts_db
    # retry = float('Inf') if not retry or (retry < 0) else retry
    i: int = 1
    while i <= retry:
        logger.info(f"Trying to add message to posts. Attempt: {i} of {retry}")
        post_id = add_message_to_posts(
            pair=pair_to_post,
            tweet_id=tweet_id,
            text=message_to_post,
        )
        if not post_id or not len(str(post_id)):
            err = f"Failed to add post"
            logger.critical(err)
        else:
            logger.info(f"[2/{POINTS}] Post added, id: {post_id}")
            return None, tweet_id, post_id

        if interval > 0:
            logger.info(f"Sleeping for {interval} sec")
            time.sleep(interval)
        if retry > 0:
            i += 1

    return err, None, None


def main():
    logger.info('The app started')

    # global cycle params
    global_timeout: int = settings.GLOBAL_TIMEOUT
    global_retry: int = settings.GLOBAL_RETRY
    global_interval: int = settings.GLOBAL_INTERVAL

    # inner cycle params
    i: int
    inner_retry: int = settings.INNER_RETRY
    inner_interval: int = settings.INNER_INTERVAL

    global_cycle_counter: int = 1

    # global cycle
    while global_cycle_counter < global_retry:
        # 1. Gather data
        logger.info(f"[1__] Trying to get data. Attempt "
                    f"{global_cycle_counter} of {global_retry}")
        err, pair_to_post, message_to_post = get_data()
        # 2. Data is OK. Publish data
        if not err:
            i = 1
            while i <= inner_retry:
                logger.info(f"[2__] Trying to publish data. Attempt "
                            f"{i} of {inner_retry}")
                err, tweet_id, post_id = publish_data(
                    pair_to_post=pair_to_post,
                    message_to_post=message_to_post,
                    retry=inner_retry,
                    interval=inner_interval,
                )
                # Failed to publish
                if err:
                    logger.critical(f"Failed to publish data: {err}")
                else:
                    break
                if inner_retry > 0:
                    i += 1
                    logger.info(f"Sleeping for {inner_interval} sec")
                    time.sleep(inner_interval)
        else:   # Failed to get data
            logger.critical(f"Failed to get data: {err}")

        if not err:
            break

        if global_retry > 0:
            global_cycle_counter += 1
            logger.info(f"Sleeping for {global_interval} sec")
            time.sleep(global_interval)

    if not err:
        logger.info('The app finished successfully')
    else:
        logger.critical('The app finished with failure')


if __name__ == '__main__':
    main()
