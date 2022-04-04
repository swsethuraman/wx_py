from pyxll import xl_func
from wx.utils import utils
from wx.utils import common
import numpy as np

@xl_func("float x, float y: float")
def utils_test(x, y):
    return x + y


@xl_func(":object")
def return_none():
    return None


@xl_func("float[] x, str[] dist_list: object")
def fit_dist(x, dist_list):
    return utils.fit_dist(x, dist_list)


@xl_func("str dist_name, var[] dist_params, int nsims: numpy_array<float>", auto_resize=True)
def dist_sim(dist_name, dist_params, nsims):
    nsims = int(nsims)
    print(dist_params)
    dist_params = common.sanitize_float(dist_params)
    print("dist params")
    print(dist_params)
    x = utils.dist_sim(dist_name, dist_params, nsims)
    return x


@xl_func("str sp_str, str sp_char, int i: str")
def split_by(sp_str, sp_char, i):
    return sp_str.split(sp_char)[i]


@xl_func("object fn, numpy_array<ndim=1> x, str[] stats: float[]")
def fn_stats(fn, x, stats):
    y = fn(x)
    res = []

    for stat in stats:
        if stat == 'mean':
            res.append(np.mean(y, axis=0))
        elif stat == 'stdev':
            res.append(np.std(y, axis=0))
        elif stat == 'sf':
            th_ = np.percentile(y, 99, axis=0)
            y_ = y[y >= th_]
            res.append(np.mean(y_, axis=0))
        else:
            pass
    return res




