# For logging
import logging
# For parsing response
import json

# Settings
from core.config import settings
from core.config import APP_NAME
from twitter.session import tw_session

logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class TwitterToolBox:
    def __init__(self):
        self.tweets_url = settings.TWEETS_URL

    def create_tweet(self, text: str, old_tweet_id: str = None) \
            -> tuple[str | None, list | None]:
        """
        Sent POST request to create a tweet with text
        :param: text
        :return: tweet_id
        """

        # We will be replying to existing thread
        if old_tweet_id:
            payload = {
                "text": text,
                "reply": {"in_reply_to_tweet_id": old_tweet_id}
            }
        else:
            payload = {"text": text}
        err = None
        try:
            response = tw_session.oauth.post(url=self.tweets_url, json=payload)
        except Exception as e:
            err = f"Failed to make a POST request: {url}: error: {e}"
            logger.critical(err)
            return err, None

        logger.debug(f"Response code: {response.status_code}")
        if response.status_code != 201:
            err = f"Request returned an error: {response.status_code} {response.text}"
            return err, None

        try:
            json_response = response.json()
            logger.debug(f"Received: {json.dumps(json_response, indent=2, sort_keys=True)}")
            new_tweet_id = json_response['data']['id']
            logger.debug(f"Created tweet with id: {new_tweet_id}")
        except Exception as e:
            err = "Failed to parse response after POST request"
            logger.debug(err)
            return err, None

        return None, new_tweet_id

