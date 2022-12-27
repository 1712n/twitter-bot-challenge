import logging

import twitter
from bot import MarketCapBot
from db import ohlcv_db, posts_db


def main():

    logging.info("Starting bot...")
    bot = MarketCapBot(ohlcv_db,posts_db,twitter.client)
    pair_to_post = bot.get_pair_to_post()
    message = bot.compose_message(pair=pair_to_post)
    bot.post_message(pair=pair_to_post,message=message)

if __name__ == "__main__":
    main()
