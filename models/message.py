import logging

from core.config import APP_NAME


logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class Message:
    template: str = "Top Market Venues for "
    text: str

    def __init__(self, pair_to_post: str, data: list):
        body: str = "\n".join(
            [" ".join(item.values()) for item in data]
        )
        self.text = self.template + pair_to_post + ":\n" + body


