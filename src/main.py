import logging
from db import (
    get_top_pairs_by_volume,
    get_pair_to_post,
    get_message_to_post,
    get_origin_tweet_id,
    add_new_post_to_db
)
from twitter_bot import new_tweet

logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s %(name)5s: %(lineno)3s: %(levelname)s >> %(message)s",
    level=logging.INFO,
)


def main():
    logging.info("Starting script..")
    top_pairs = get_top_pairs_by_volume()
    pair_to_post = get_pair_to_post(pairs=top_pairs)
    message_to_post = get_message_to_post(pair=pair_to_post)
    origin_tweet_id = get_origin_tweet_id(pair=pair_to_post)
    new_tweet_id = new_tweet(
        pair=pair_to_post,
        text=message_to_post,
        origin_tweet_id=origin_tweet_id
    )
    logging.info("New tweet with id=%s was created!", new_tweet_id)
    new_post_id = add_new_post_to_db(
        pair=pair_to_post,
        tweet_id=new_tweet_id,
        text=message_to_post
    )
    logging.info("New post with id=%s was added to database!", new_post_id)


if __name__ == "__main__":
    main()
