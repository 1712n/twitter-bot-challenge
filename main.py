import os
from PairToPost import PairToPost
from MarketCapBot import MarketCapBot
from WorkFlowRecorder import WorkFlowRecorder

if __name__ == "__main__":

    recorder = WorkFlowRecorder()
    recorder.get_logged("Start main workflow")

    pair_to_post = PairToPost(user=os.environ["MONGODB_USER"],
                                password=os.environ["MONGODB_PASSWORD"],
                                address=os.environ["MONGO_DB_ADDRESS"],
                                recorder=recorder)

    bot = MarketCapBot(consumer_key=os.environ["TW_CONSUMER_KEY"],
                        consumer_secret=os.environ["TW_CONSUMER_KEY_SECRET"],
                        access_token=os.environ["TW_ACCESS_TOKEN"],
                        access_token_secret=os.environ["TW_ACCESS_TOKEN_SECRET"],
                        recorder=recorder)

    pair_to_post.get_pair_to_post()

    bot.set_pair_to_post(pair_to_post)

    bot.compose_message_to_post()

    new_post_data = bot.create_tweet()

    pair_to_post.add_post_to_collection(new_post_data)

    recorder.get_logged("Stop execution")