def get_percentage(num_a: float, num_b: float) -> float:
    return (num_a / num_b) * 100


def sort_dict(dictionary, by_key=True, reverse=False):
    index = 0 if by_key else 1
    sorted_list = sorted(dictionary.items(),
                         key=lambda x: x[index], reverse=reverse)
    return dict(sorted_list)
