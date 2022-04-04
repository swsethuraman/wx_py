from scipy.stats import norm
from scipy import optimize


def bachelier(opt_type, mean, stdev, notional, strike, limit, result='px'):
    if result == 'px':
        if opt_type == 'call':
            px_ = bachelier_call(mean, stdev, notional, strike, limit, result=result)
        elif opt_type == 'put':
            px_ = bachelier_put(mean, stdev, notional, strike, limit, result=result)
        else:
            px_ = -10000000000.00
        return px_
    elif result == 'delta':
        delta = bachelier_implied_vol(opt_type, mean, notional, )
    pass


def bachelier_put_nolimit(mean, stdev, notional, strike):
    x = strike - mean
    put_ = x*norm.cdf(x/stdev) + stdev*norm.pdf(x/stdev)
    return put_*notional


def bachelier_put(mean, stdev, notional, strike, limit=None, result='px'):
    if limit is None:
        return bachelier_put_nolimit(mean, stdev, notional, strike)
    else:
        put_nolimit = bachelier_put_nolimit(mean, stdev, 1, strike)
        limit_strike = strike - limit/notional
        put_limitstrike = bachelier_put_nolimit(mean, stdev, 1, limit_strike)
        put_ = put_nolimit - put_limitstrike
    return put_*notional


def bachelier_call_nolimit(mean, stdev, notional, strike):
    x = mean - strike
    call_ = x*norm.cdf(x/stdev) + stdev*norm.pdf(x/stdev)
    return notional*call_


def bachelier_call(mean, stdev, notional, strike, limit=None, result='px'):
    if limit is None:
        return bachelier_call_nolimit(mean, stdev, notional, strike)
    else:
        call_nolimit = bachelier_call_nolimit(mean, stdev, 1, strike)
        limit_strike = strike + limit/notional
        call_limitstrike = bachelier_call_nolimit(mean, stdev, 1, limit_strike)
        call_ = call_nolimit - call_limitstrike
    return call_*notional


def bachelier_implied_vol(opt_type, mean, notional, strike, px, limit=None):
    fn = lambda x: bachelier(opt_type, mean, x, notional, strike, limit=limit, result='px') - px
    sol = optimize.root(fn, [50.0], method='broyden1', tol=1e-6)
    return sol.x[0]
