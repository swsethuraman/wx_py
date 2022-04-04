def sanitize_none(d):
    for key in d.keys():
        if d[key] == "None":
            d[key] = None
    return d


def sanitize_float(l):
    r = []
    for l_temp in l:
        if isinstance(l_temp, float):
            r.append(l_temp)
    return r
