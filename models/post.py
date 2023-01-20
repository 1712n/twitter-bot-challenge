import logging

from core.config import APP_NAME


logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class Post:
    pair: str
    tweet_id: str
    text: str

    def __init__(self, pair: str, tweet_id: str, text: str):
        self.pair = pair
        self.tweet_id = tweet_id
        self.text = text

    def __str__(self):
        return f"{{pair: {self.pair}, tweet_id: {self.tweet_id}, text: {self.text}}}"

    def to_dict(self) -> dict:
        return {"pair": self.pair, "tweet_id": self.tweet_id, "text": self.text}

