# Logging
import logging
# For twitter
from requests_oauthlib import OAuth1Session

# Settings
from core.config import settings
from core.config import APP_NAME

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class TwitterConnection:
    def __init__(self):
        self.consumer_key = settings.TW_CONSUMER_KEY
        self.consumer_secret = settings.TW_CONSUMER_KEY_SECRET
        self.access_token = settings.TW_ACCESS_TOKEN
        self.access_token_secret = settings.TW_ACCESS_TOKEN_SECRET
        self.oauth = OAuth1Session(
            self.consumer_key,
            client_secret=self.consumer_secret,
            resource_owner_key=self.access_token,
            resource_owner_secret=self.access_token_secret,
        )


tw_session = TwitterConnection()
