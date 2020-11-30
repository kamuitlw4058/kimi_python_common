def dict_merge(down_dict, up_dict):
    """
    merge up_dict dict to down_dict dict
    :param down_dict: dict
    :param up_dict: dict
    :return: new dict
    """
    keys = {i for i in down_dict.keys()} | {i for i in up_dict.keys()}
    res = {}
    for k in keys:
        if k in down_dict:
            v = down_dict[k]
            if isinstance(v, dict) and k in up_dict:
                res[k] = dict_merge(v, up_dict[k])
            else:
                res[k] = up_dict[k] if k in up_dict else v
        else:
            res[k] = up_dict[k]

    return res