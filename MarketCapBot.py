import tweepy
import PairToPost
import copy

class MarketCapBot():
    """Performs all required actions with Twitter API"""
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str) -> None:
        self.client = tweepy.Client(consumer_key=consumer_key, consumer_secret=consumer_secret,
            access_token=access_token, access_token_secret=access_token_secret)
        self.pair_document = None
        self.message_body = ""
        self.response = None
        self.published_post = None
        self.pair_to_post_id  = None

    def set_pair_to_post(self, pair_to_post: PairToPost) -> None:
        """Makes its own copy of pair_document for internal usage """
        if isinstance(pair_to_post, PairToPost):
            self.pair_document = copy.deepcopy(pair_to_post.pair_document)
            self.pair_to_post_id = id(pair_to_post)

    def compose_message_to_post(self) -> None:
        """Composes a message body for a tweet"""
        title = f"Top Market Venues for {self.pair_document['pair_symbol'].upper()}-{self.pair_document['pair_base'].upper()}:\n"
        top_5_markets =""
        other_markets_percent = 0
        if len(self.pair_document["markets"]) >=5:
            for i in range(5):
                top_5_markets += f"{self.pair_document['markets'][i]['marketVenue']} {self.pair_document['markets'][i]['market_comp_vol_percent']}%\n"
            for i in range(5, len(self.pair_document["markets"])):
                other_markets_percent += self.pair_document["markets"][i]["market_comp_vol_percent"]
        else:
            for market in self.pair_document['markets']:
                top_5_markets += f"{market['marketVenue']} {market['market_comp_percent']}%\n"
        self.message_body = title + top_5_markets + f"Others {other_markets_percent:.2f}%\n"

    def create_tweet(self) -> dict:
        """
        Creates new tweet for pair.
        Returns dict consists of new post's related information.
        """
        # if no posts were found during aggregaion then we create the first tweet for the pair
        # backlog: add error handling
        if len(self.pair_document["latest_post"]) == 0:
            self.response = self.client.create_tweet(text=self.message_body, user_auth=True)
            if "id" not in self.response.data or len(self.response.data["id"]) == 0:
                print("Something went wrong. Response returned without tweet_id.\n" \
                    f"Returned errors:\n{self.response.errors}")
        # if the latest related post has tweet_id field then post a reply in the same thread
        # backlog: add error handling
        elif "tweet_id" in self.pair_document["latest_post"]:
            self.response = self.client.create_tweet(text=self.message_body,
                in_reply_to_tweet_id=self.pair_document["latest_post"]["tweet_id"], user_auth=True)
        # in case of there is no tweet_id field we find the one (using pair and timestamp from posts_db) and post a reply in the same thread
        # backlog: add error handling
        else:
            tweet_to_reply = ""
            bot_id = self.client.get_me(user_auth=True).data.id
            search_result = self.client.get_users_tweets(bot_id, user_auth=True,
                start_time=self.pair_document.latest_post["timestamp"],
                end_time=self.pair_document.latest_post["timestamp"].replace(minute=self.pair_document.latest_post["timestamp"].minute + 1),
                tweet_fields=["created_at"])
            for tweet in search_result.data:
                if self.pair_document.latest_post["pair"] in tweet.text:
                    tweet_to_reply = str(tweet.id)
                    break
            self.response = self.client.create_tweet(text=self.message_body,
                 in_reply_to_tweet_id=tweet_to_reply, user_auth=True)
        # retreive new post related data to put the ones in a returned dict
        self.published_post = self.client.get_tweet(self.response.data["id"], user_auth=True,
            tweet_fields=["created_at"])
        return {"tweet_id": self.published_post.data.id,
                "text": self.published_post.data.text,
                "timestamp": self.published_post.data.created_at,
                "pair": f"{self.pair_document['pair_symbol'].upper()}-{self.pair_document['pair_base'].upper()}",
                "pair_to_post_id": self.pair_to_post_id}