



def to_bool(v):
    if isinstance(v,str):
        v = v.lower()
        v =  v != 'false'
        return v
    else:
        raise NotImplementedError('input type not imp '+ type(v))