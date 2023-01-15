from typing import Dict


def volume_to_rate(volume_by_market: Dict[str, float], limit=5) -> Dict[str, float]:
    total = sum(volume_by_market.values())
    others = 0
    res = {}
    for k, v in volume_by_market.items():
        if len(res) < limit:
            res[k] = v * 100 / total
        else:
            others += v
    if others > 0:
        others = others * 100 / total
        res["others"] = others
    return res


def message_format(pair: str, volume_by_market: Dict[str, float]) -> str:
    rates = volume_to_rate(volume_by_market)
    rates_formatted = '\n'.join([f"{venue.capitalize()}: {format(rate, '.2f')}%" for venue, rate in rates.items()])
    return f"Top Market Venues for {pair.upper()}:\n{rates_formatted}"
