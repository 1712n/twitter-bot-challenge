import tweepy
import PairToPost

class MarketCapBot():
    """"""
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str) -> None:
        self.client = tweepy.Client(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token, access_token_secret=access_token_secret)
        self.pair_to_post = None
        self.message_body = ""

    def set_pair_to_post(self, pair_to_post: PairToPost) -> None:
        """"""
        self.pair_to_post = pair_to_post

    def compose_message_to_post(self) -> None:
        """Composes a message body for a tweet"""
        pair_document = self.pair_to_post.pair_document
        title = f"Top Market Venues for {pair_document['pair_symbol'].upper()}-{pair_document['pair_base'].upper()}:\n"
        top_5_markets =""
        other_markets_percent = 0
        if len(pair_document["markets"]) >=5:
            for i in range(5):
                top_5_markets += f"{pair_document['markets'][i]['marketVenue']} {pair_document['markets'][i]['market_comp_vol_percent']}%\n"
            for i in range(5, len(pair_document["markets"])):
                other_markets_percent += pair_document["markets"][i]["market_comp_vol_percent"]
        else:
            for market in pair_document['markets']:
                top_5_markets += f"{market['marketVenue']} {market['market_comp_percent']}%\n"
        self.message_body = title + top_5_markets + f"Others {other_markets_percent:.2f}%\n"

    def post_tweet(self) -> bool:
        """"""
        if len(self.pair_to_post["latest_post"]) == 0:
            # post the first tweet for the pair
            self.client.create_tweet(text=self.message_body, user_auth=True)
        elif "tweet_id" in self.pair_to_post["latest_post"]:
            # post a tweet in a thread by tweet_id
            self.client.create_tweet(text=self.message_body, in_reply_to_tweet_id=self.pair_to_post["latest_post"]["tweet_id"], user_auth=True)
        else:
            # in case of there is no tweet_id field
