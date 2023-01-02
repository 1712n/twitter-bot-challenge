import logging
from db import (
    get_top_pairs_by_volume,
    get_pair_to_post,
    get_message_to_post
)

logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s %(name)5s: %(lineno)3s: %(levelname)s >> %(message)s",
    level=logging.INFO,
)


def main():
    top_pairs = get_top_pairs_by_volume()
    pair_to_post = get_pair_to_post(top_pairs)
    message = get_message_to_post(pair_to_post)
    print(message)


if __name__ == "__main__":
    main()
