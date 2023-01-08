from datetime import datetime

from dto import PairPost, AggregatePairVenue, Post

def get_oldest_post(posts: list[PairPost]) -> PairPost | None:
    if len(posts) > 1:
        return posts[-1]
    return None


def create_post(aggregated_venues: list[AggregatePairVenue], pair_post: PairPost) -> Post:
    tweet_text = 'Top Market Venues for {}: \n'
    other_text = 'Other'
    percentage_template = '{:.2f}'
    pair_markets_dict: dict[str, list[AggregatePairVenue]] = {}
    complete_text = ''
    template = '{}: {}%'

    for agg in aggregated_venues:
        if agg.pair in pair_markets_dict:
            pair_markets_dict[agg.pair].append(agg)
        else:
            pair_markets_dict[agg.pair] = [agg]

    for key, values in pair_markets_dict.items():
        total = sum(map(lambda val: val.count, values))
        other_total = 0
        sorted_values = sorted(values, key=lambda val: val.count, reverse=True)
        complete_text = tweet_text.format(key.upper())
        for i, market in enumerate(sorted_values):
            if i < 5:
                percentage = market.count / total
                venue_format = market.venue.capitalize()
                market_format = template.format(venue_format, percentage_template.format(percentage))
                complete_text += market_format + '\n'
            else:
                other_total += market.count
        if other_total != 0:
            other_percentage = other_total / total
            other_format = template.format(other_text, percentage_template.format(other_percentage))
            complete_text += other_format

    return Post(datetime.utcnow(), pair_post.tweet_id, complete_text, pair_post.pair)
