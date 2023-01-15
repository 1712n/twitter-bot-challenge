class TwitterClient:
    def __init__(self,
                 access_token: str,
                 access_token_secret: str,
                 consumer_key: str,
                 consumer_key_secret: str):
        self._access_token = access_token
        self._access_token_secret = access_token_secret
        self._consumer_key = consumer_key
        self._consumer_key_secret = consumer_key_secret

    def origin_tweet_id(self, pair: str) -> str | None:
        pass

    def post(self, tweet: str, origin_tweet_id: str | None) -> str:
        pass