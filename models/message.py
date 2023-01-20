import logging

from core.config import APP_NAME
from core.config import settings


logger = logging.getLogger(f"{APP_NAME}.{__name__}")


class Message:
    template: str = settings.MESSAGE_TEMPLATE
    text: str

    @staticmethod
    def calculate_others(data: list) -> float | None:
        try:
            others: float = 100
            for item in data:
                others -= float(item['percentage'])
            return others
        except Exception as e:
            logger.warning("Failed to calculate Others")
            return None

    def __init__(self, pair_to_post: str, data: list):
        others = self.calculate_others(data=data)
        body = ""
        for item in data:
            body += f"{item['marketVenue']} {item['percentage']}%\n"
        if others:
            body += f"Others {others:.2f}%"
        self.text = self.template + pair_to_post + ":\n" + body

