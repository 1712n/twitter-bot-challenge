from db import (OhlcvDao,
                PostDao)
from service_post import (get_oldest_post,
                          create_post)
from twitter_api import TwitterApi

if __name__ == '__main__':
    ohlcv_dao = OhlcvDao()
    post_dao = PostDao()
    twitter_client = TwitterApi()

    pairs = ohlcv_dao.get_top_pairs()
    posts = post_dao.get_last_posts(pairs)
    last_post = get_oldest_post(posts)
    if last_post is not None:
        agg = ohlcv_dao.get_aggregated_venues(last_post)
        post = create_post(agg, last_post)
        twitter_id = twitter_client.create_tweet(post)
        if post.tweet_id is None:
            post.tweet_id = twitter_id
        post_dao.save_post(post)


print('DONE')