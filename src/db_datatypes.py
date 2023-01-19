import logging
from collections import OrderedDict


logger = logging.getLogger(__name__)


'''Each of these classes inherits from OrderedDict class which 
is very convenient for storing pre-sorted data which we receive 
from mongoDB. Also classes helps me in orginising code a lot.'''


class TopPairsByVolume(OrderedDict):
    def diff(self, external_keys_set: set) -> set:
        return set(self.keys()).difference(external_keys_set)

    def find_largest_among(self, pairs_set: set) -> tuple[str, float]:
        for pair in self.keys():
            if pair in pairs_set:
                return pair, self[pair]
        raise Exception(
            "Pair supposed to be in the dictionary!")


class OldestLastPostsForPairs(OrderedDict):
    # returns None if there's no pairs
    def first(self):
        return next(iter(self), None)


class PairMarketStats(OrderedDict):
    def compose_message(self, pair: str, total_vol: float):
        message = f"Top Market Venues for {pair}:\n"

        percent_sum = 0
        for market in self.keys():
            market_vol_percens = self[market] / total_vol * 100
            percent_sum += int(market_vol_percens * 100)

            if market_vol_percens >= 0.01:
                # forming a message
                message += f"{market.title()} {market_vol_percens:.2f}%\n"

        remains = 10000 - percent_sum
        remains /= 100
        if remains >= 0.01:
            message += f"Others {remains:.2f}%"

        return message
