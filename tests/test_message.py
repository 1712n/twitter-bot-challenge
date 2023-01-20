import pytest

from core.config import settings
from models.message import Message


def test_create_message():
    data = [
        {'venue': 'Binance', 'share': '30%'},
        {'venue': 'OKX', 'share': '20%'},
        {'venue': 'Others', 'share': '50%'}
    ]
    pair_to_post = "BTC-USDT"
    msg = Message(pair_to_post=pair_to_post, data=data)
    print(msg.text)

    correct_text = f"{settings.MESSAGE_TEMPLATE}{pair_to_post}:\n" \
                   f"Binance 30%\n" \
                   f"OKX 20%\n" \
                   f"Others 50%"

    assert msg.text == correct_text

