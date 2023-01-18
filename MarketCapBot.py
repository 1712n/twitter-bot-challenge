import tweepy
from PairToPost import PairToPost
import copy
from WorkFlowRecorder import WorkFlowRecorder

class MarketCapBot():
    """Performs all required actions with Twitter API"""
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str, recorder: WorkFlowRecorder) -> None:
        """"""
        recorder.get_logged("Initialize connection to Twitter API")

        self.client = tweepy.Client(consumer_key=consumer_key, consumer_secret=consumer_secret,
            access_token=access_token, access_token_secret=access_token_secret)
        self.pair_document = None
        self.message_body = ""
        self.response = None
        self.published_post = None
        self.pair_to_post_id  = None
        self.recorder = recorder

    def set_pair_to_post(self, pair_to_post: PairToPost) -> None:
        """Makes its own copy of pair_document for internal usage """
        self.recorder.get_logged("method set_pair_to_post has started")

        if isinstance(pair_to_post, PairToPost):
            self.pair_document = copy.deepcopy(pair_to_post.pair_document)
            self.pair_to_post_id = id(pair_to_post)
        else:
            self.recorder.get_logged(error_flag=True, message="passed argument is not an instance of PairToPost")

        self.recorder.get_logged("method set_pair_to_post has finished")

    def compose_message_to_post(self) -> None:
        """Composes a message body for a tweet"""
        self.recorder.get_logged("method compose_message_to_post has started")

        title = f"Top Market Venues for {self.pair_document['pair_symbol'].upper()}-{self.pair_document['pair_base'].upper()}:\n"
        top_5_markets =""
        other_markets_percent = 0

        if len(self.pair_document["markets"]) >=5:
            for i in range(5):
                top_5_markets += f"{self.pair_document['markets'][i]['marketVenue'].capitalize()} {self.pair_document['markets'][i]['market_comp_vol_percent']}%\n"
            for i in range(5, len(self.pair_document["markets"])):
                other_markets_percent += self.pair_document["markets"][i]["market_comp_vol_percent"]
        else:
            for market in self.pair_document['markets']:
                top_5_markets += f"{market['marketVenue']} {market['market_comp_percent']}%\n"

        self.message_body = title + top_5_markets + f"Others {other_markets_percent:.2f}%\n"

        self.recorder.get_logged("message_to_post content:\n" + f"{self.message_body}")
        self.recorder.get_logged("method compose_message_to_post has finished")

    def create_tweet(self) -> dict:
        """
        Creates new tweet for pair.\n
        Returns dict consists of new post's related information.
        """
        self.recorder.get_logged("method create_tweet has started")
        # if no posts were found during aggregaion then we create the first tweet for the pair
        # backlog: add error handling
        if len(self.pair_document["latest_post"]) == 0:
            self.recorder.get_logged("pair doesn't have latest_post. submitting new tweet ...")
            self.response = self.client.create_tweet(text=self.message_body, user_auth=True)

        # if the latest related post has tweet_id field then post a reply in the same thread
        # backlog: add error handling
        elif "tweet_id" in self.pair_document["latest_post"] and self.pair_document["latest_post"]["tweet_id"] != None:
            self.recorder.get_logged(f"latest_post includes tweet_id. submitting new tweet in thread {self.pair_document['latest_post']['tweet_id']} ...")
            self.response = self.client.create_tweet(text=self.message_body,
                in_reply_to_tweet_id=self.pair_document["latest_post"]["tweet_id"], user_auth=True)

        # in case of there is no tweet_id field in the latest_post, then we find the one and post a reply in the same thread
        # backlog: add error handling
        else:
            self.recorder.get_logged("latest_post doesn't contain tweet_id. starting to search for the latest tweet for the pair to reply to ...")

            tweet_to_reply = None
            next_token = None
            bot_id = self.client.get_me(user_auth=True).data.id
            proceed = True

            # check every tweet(in order from the newest to oldest) until either find the last published tweet for the pair or next_token runs out
            while proceed:
                search_result = self.client.get_users_tweets(bot_id, pagination_token=next_token, max_results=100, user_auth=True, tweet_fields=["created_at"])
                for tweet in search_result.data:
                    if self.pair_document["latest_post"]["pair"] in tweet.text:
                        tweet_to_reply = str(tweet.id)
                        proceed = False
                        break
                if "next_token" in search_result.meta:
                    next_token = search_result.meta["next_token"]
                else:
                    proceed = False

            # if no related tweet was found, then just create the new one
            if tweet_to_reply == None:
                self.recorder.get_logged("no tweet_to_reply was found. submitting new tweet ...")
                self.response = self.client.create_tweet(text=self.message_body, user_auth=True)
            else:
                self.recorder.get_logged(f"found tweet_to_reply. submitting new tweet to thread {tweet_to_reply} ...")
                self.response = self.client.create_tweet(text=self.message_body, in_reply_to_tweet_id=tweet_to_reply, user_auth=True)

        self.recorder.get_logged("New tweet has beeb published.\n" + f"tweet_id: {self.response.data['id']}")

        # retreive new post related data to put the ones in a returned dict
        self.published_post = self.client.get_tweet(self.response.data["id"], user_auth=True,
            tweet_fields=["created_at"])

        new_tweet_data = {"tweet_id": str(self.published_post.data.id),
                        "text": self.published_post.data.text,
                        "timestamp": self.published_post.data.created_at,
                        "pair": f"{self.pair_document['pair_symbol'].upper()}-{self.pair_document['pair_base'].upper()}",
                        "pair_to_post_id": self.pair_to_post_id}

        self.recorder.get_logged(new_tweet_data.__str__().replace(",", "\n"))
        self.recorder.get_logged("method create_tweet has finished")
        return new_tweet_data