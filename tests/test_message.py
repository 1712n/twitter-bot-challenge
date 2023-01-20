import pytest

from core.config import settings
from models.message import Message


def test_create_message():
    data = [
        {'marketVenue': 'Binance', 'percentage': '30.0'},
        {'marketVenue': 'OKX', 'percentage': '20.0'},
        {'marketVenue': 'Others', 'percentage': '50.0'}
    ]
    pair_to_post = "BTC-USDT"
    msg = Message(pair_to_post=pair_to_post, data=data)
    print(msg.text)

    correct_text = f"{settings.MESSAGE_TEMPLATE}{pair_to_post}:\n" \
                   f"Binance 30.0%\n" \
                   f"Okx 20.0%\n" \
                   f"Others 50.0%\n"

    assert msg.text == correct_text

