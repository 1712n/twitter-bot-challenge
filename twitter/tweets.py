#
import logging
#
from requests_oauthlib import OAuth1Session
import os
import json

# Settings
from core.config import settings
from core.config import APP_NAME
from twitter.session import tw_session

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class TwitterToolBox:
    def create_tweet(self, text: str):
        # Making the request
        payload = {"text": text}
        response = tw_session.oauth.post(
            "https://api.twitter.com/2/tweets",
            json=payload,
        )

        if response.status_code != 201:
            raise Exception(
                f"Request returned an error: {response.status_code} {response.text}"
            )
        logger.debug(f"Response code: {response.status_code}")

        # Saving the response as JSON
        json_response = response.json()
        logger.debug(json.dumps(json_response, indent=2, sort_keys=True))

