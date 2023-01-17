from dotenv import load_dotenv
load_dotenv()
import os
import PairToPost
import MarketCapBot

if __name__ == "__main__":

    pair_to_post = PairToPost(os.environ["MONGODB_USER"], os.environ["MONGODB_PASSWORD"], os.environ["MONGO_DB_ADDRESS"])
    bot = MarketCapBot(os.environ["TW_CONSUMER_KEY"], os.environ["TW_CONSUMER_KEY_SECRET"], os.environ["TW_ACCESS_TOKEN"], os.environ["TW_ACCESS_TOKEN_SECRET"])

    pair_to_post.get_pair_to_post()
    bot.set_pair_to_post(pair_to_post)
    bot.compose_message_to_post()
    new_post_data = bot.create_tweet()
    pair_to_post.add_post_to_collection(new_post_data)
