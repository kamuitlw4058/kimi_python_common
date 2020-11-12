
def dict2item(dic,item,keys_mapper=None,apply_key=[]):
    if keys_mapper is None:
        for k,v in dic.items():
            if len(apply_key) == 0 or  k  in apply_key:
                item[k] = v
    else:
        for k,v in dic.items():
            k = keys_mapper.get(k,None)
            if k is not None and (len(apply_key) == 0 or k  in apply_key):
                item[k] = v
    return item
