import pandas as pd
import numpy as np
from scipy.stats import norm, expon, gumbel_r, gamma, laplace, gumbel_l, beta
from scipy.stats import kstest
from scipy.optimize import fmin
import statsmodels.api as sm

def season_convertor(date_vector, season, season_start, risk_start, risk_end):
    risk_time_delta = risk_end - risk_start
    year_diff = risk_start.year - season_start.year
    risk_start_season = pd_datetime_from_ymd(year=season + year_diff, month=risk_start.month, day=risk_start.day)
    risk_end_season = risk_start_season + risk_time_delta
    return (date_vector >= risk_start_season) & (date_vector <= risk_end_season)


def pd_datetime_from_ymd(year=None, month=None, day=None):
    date_str = ''.join([str(year), '-', str(month), '-', str(day)])
    return pd.to_datetime(date_str, format='%Y-%m-%d')


dists = {
    'norm': norm,
    'expon': expon,
    'gumbel_r': gumbel_r,
    'gumbel_l': gumbel_l,
    'gamma': gamma,
    'laplace': laplace,
    'beta': beta
}


def fit_dist(x, dist_list, zero_mean=False):
    llh = []
    bic = []
    aic = []
    zm_llh = []
    zm_bic = []
    zm_aic = []
    n_samples = len(x)
    fit_results = []

    for d in dist_list:
        dist = dists[d]
        print('Fitting {0} distribution'.format(dist.name))

        # Fit unconstrained distribution
        params = dist.fit(x)
        n_params = len(params)

        dnew = dist(*params)
        llh.append(dnew.logpdf(x).sum())
        bic.append(np.log(n_samples) * n_params - 2 * llh[-1])
        aic.append(2 * n_params - 2 * llh[-1])

        # Fit  mean=0 distribution
        shape_params = None
        if n_params > 2:
            shape_params = params[:-2]
        scale_param = params[-1]
        if shape_params is None:
            fit_params = [scale_param]
        else:
            fit_params = list(shape_params) + [scale_param]

        # Minimize negative log-likelihood ratio while keeping mean=0
        fit_nllh = lambda y: -1 * dist(*tuple(y[:-1]), loc=-1 * y[-1] * dist(*tuple(y[:-1]), loc=0, scale=1).mean(),
                                       scale=y[-1]).logpdf(x).sum()
        res_params = fmin(fit_nllh, fit_params)
        fit_shape_params = res_params[:-1]
        fit_loc_param = [-1 * res_params[-1] * dist(*tuple(res_params[:-1]), loc=0, scale=1).mean()]
        fit_scale_param = [res_params[-1]]

        try:
            fit_param_bool = not fit_shape_params
        except Exception as e:
            fit_param_bool = False

        if fit_shape_params is None or fit_param_bool:
            fit_res_params = fit_loc_param + fit_scale_param
        else:
            fit_res_params = list(fit_shape_params) + fit_loc_param + fit_scale_param

        zm_llh_temp = dist(*tuple(fit_res_params)).logpdf(x).sum()
        zm_llh.append(zm_llh_temp)
        zm_bic.append(np.log(n_samples) * n_params - 2 * zm_llh[-1])
        zm_aic.append(2 * n_params - 2 * zm_llh[-1])

        # print('Distribution Fitting Results')
        # print('Checking Goodness of Fit')
        # print(kstest(x, dnew.cdf))
        # print("Log Likelihood Ratio: {0}".format(llh[-1]))
        # print("Mean: {0} \t Stdev: {1}".format(dnew.mean(), dnew.std()))
        results = {
            'dist_name': dist.name,
            'params': params,
            'zm_params': fit_res_params,
            'n_params': n_params,
            'n_samples': n_samples,
            'llh': llh[-1],
            'AIC': 2 * n_params - 2 * llh[-1],
            'BIC': np.log(n_samples) * n_params - 2 * llh[-1],
            'zm_llh': zm_llh[-1],
            'zm_AIC': zm_aic[-1],
            'zm_BIC': zm_bic[-1]
        }
        fit_results.append(results)

    # Return top 3 dists by zm_BIC
    ranked = np.array(zm_bic).argsort()
    return_results = []
    for i in ranked[0:3]:
        return_results.append(fit_results[i])

    return return_results


def dist_sim(dist_name, dist_params, nsims):
    dist_ = dists[dist_name](*tuple(dist_params))
    sims = dist_.rvs(nsims)
    return sims

