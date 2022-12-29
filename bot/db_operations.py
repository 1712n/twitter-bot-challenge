from bot.config import client

# get database and collections
dbname = client['metrics']
ohlcv = dbname["ohlcv_db"]
posts = dbname["posts_db"]


def get_top_pairs():
    # a function to find exactly top-100 pairs
    top_pairs = ohlcv.find().sort('volume', -1)
    pairs = []
    # collecting pairs
    for i in top_pairs:
        if not i['market_id'] in pairs:
            pairs.append(str(i['market_id'].upper()).replace(str(i['marketVenue'].upper() + '-'), '').replace('-F', ''))
        # set will not allow duplicates among our pairs, so we will have exactly 100 unique pairs
        if len(set(pairs)) >= 100:
            break
    return pairs


def get_top_posts():
    # function to extract pairs from posts corresponding to top 100 pairs
    corresponding_posts = []
    for pair in get_top_pairs():
        top_posts = posts.find({"pair": pair}).sort('timestamp', -1)
        for x in top_posts:
            if x['pair'] not in corresponding_posts:
                corresponding_posts.append(x['pair'])
    return corresponding_posts


def get_message_to_post(pair):
    pair_symbol = pair.split('-')[0].lower()
    pair_base = pair.split('-')[1].lower()

    pair_instances = ohlcv.find({'pair_symbol': pair_symbol, 'pair_base': pair_base}).sort('volume', -1)

    message = {}
    other = {'others': 0}

    for i in pair_instances:
        if i['marketVenue'] not in message:
            if len(message) <= 5:
                message[i['marketVenue']] = float(i['volume'])
                continue
            else:
                other['others'] += float(i['volume'])
                continue
        message[i['marketVenue']] += float(i['volume'])

    message = {k: round((v / sum(message.values())) * 100, 2) for k, v in message.items()}

    message_to_post = f'Top Market Venues for {pair}:\n'
    for market, volume in reversed(message.items()):
        message_to_post += f'{market.capitalize()} {volume}%\n'
    message_to_post += f'Others {other["others"]}%'

    return message_to_post


def get_composed_posts():
    composed_posts = []
    for pair in get_top_posts():
        composed_posts.append(get_message_to_post(pair))
    return composed_posts

