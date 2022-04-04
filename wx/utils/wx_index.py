import numpy as np


wx_index = {
    'HDD'	: lambda x, y, z, w: hdd(x, y, daily_index_max=z, daily_index_min=w),
    'CDD'	: lambda x, y, z, w: cdd(x, y, daily_index_max=z, daily_index_min=w),
    'TAvg'	: lambda x, y, z, w: x,
    'TMax'	: lambda x, y, z, w: x,
    'TMin'	: lambda x, y, z, w: x,
    'Digital_Low': lambda x, y, z, w: digital_low(x, y),
    'Digital_High': lambda x, y, z, w: digital_high(x, y)
}


def hdd(x, y, daily_index_max=None, daily_index_min=None):
    hdd_ = np.maximum(y -x, 0.0)
    if daily_index_max is not None:
        hdd_ = np.minimum(hdd_, daily_index_max)
    if daily_index_min is not None:
        hdd_ = np.maximum(hdd_, daily_index_min)
    return hdd_


def cdd(x, y, daily_index_max=None, daily_index_min=None):
    cdd_ = np.maximum(x - y, 0.0)
    if daily_index_max is not None:
        cdd_ = np.minimum(cdd_, daily_index_max)
    if daily_index_min is not None:
        cdd_ = np.maximum(cdd_, daily_index_min)
    return cdd_


def digital_low(x, y, daily_index_max=None, daily_index_min=None):
    low_ = np.float64(np.less(x, y))
    return low_


def digital_high(x, y, daily_index_max=None, daily_index_min=None):
    high_ = np.float64(np.greater(x, y))
    return high_

