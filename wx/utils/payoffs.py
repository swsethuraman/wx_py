import numpy as np

def call(x, k, notional, limit=None):
    payoff = np.maximum(x-k, 0)*notional
    if limit is not None:
        payoff = np.minimum(limit, payoff)
    return payoff


def put(x, k, notional, limit=None):
    payoff = np.maximum(k-x, 0)*notional
    if limit is not None:
        payoff = np.minimum(limit, payoff)
    return payoff


def swap(x, k, notional, limit_plus=None, limit_minus=None):
    payoff = (x-k)*notional
    if limit_plus is not None:
        payoff = np.minimum(limit_plus, payoff)
    if limit_minus is not None:
        payoff = np.maximum(-limit_minus, payoff)
    return payoff
