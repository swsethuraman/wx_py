import pandas as pd
import numpy as np
from scipy.stats import norm, expon, gumbel_r, gamma, laplace, gumbel_l, beta
from scipy.stats import kstest
import statsmodels.api as sm
from wx.utils import utils


class VanillaBaseModel:
    def __init__(self, vanilla, data=None, season_start=None):
        self.data = data
        self.vanilla = vanilla
        self.model = dict()
        self.model['burn'] = pd.DataFrame()
        self.season_start = season_start

    def get_results(self):
        self.model['burn_stats'] = dict()
        return self.model

    def evaluate_payoffs(self):
        self.model['burn']['Vanilla_Payoff'] = 0
        for leg in self.vanilla.legs:
            self.model['burn'][leg.name + '_Payoff'] = leg.payoff.payoff(self.model['burn'][leg.name])
            self.model['burn']['Vanilla_Payoff'] = self.model['burn']['Vanilla_Payoff'] + self.model['burn'][leg.name + '_Payoff']

        self.model['burn']['Vanilla_Payoff_Final'] = self.model['burn']['Vanilla_Payoff']
        deductible = self.vanilla.aggregate_deductible
        if deductible is not None:
            self.model['burn']['Vanilla_Payoff_Final'] = self.model['burn']['Vanilla_Payoff_Final'] - \
                                                 np.sign(self.model['burn']['Vanilla_Payoff_Final']) \
                                                 * np.minimum(deductible, np.abs(self.model['burn']['Vanilla_Payoff_Final']))
        limit_plus = self.vanilla.aggregate_limit_lc
        limit_minus = self.vanilla.aggregate_limit_cpty
        if limit_plus is not None:
            self.model['burn']['Vanilla_Payoff_Final'] = np.minimum(self.model['burn']['Vanilla_Payoff_Final'], limit_plus)
        if limit_minus is not None:
            self.model['burn']['Vanilla_Payoff_Final'] = np.maximum(self.model['burn']['Vanilla_Payoff_Final'], -limit_minus)
        pass


class VanillaBurnModel(VanillaBaseModel):
    def __init__(self, vanilla, data=None, season_start=None):
        super().__init__(vanilla, data, season_start=season_start)
        for leg in self.vanilla.legs:
            self.add_index(leg)
        self.evaluate_payoffs()

    def add_sub_index(self, sub_index):
        risk_start = None
        risk_end = None
        if not sub_index.name in self.model['burn'].columns:
            risk_start = sub_index.risk_start
            risk_end = sub_index.risk_end

        if self.season_start is None:
            self.season_start = risk_start

        st_season = self.data['Date'][0].year
        end_season = self.season_start.year

        df_subindex = pd.DataFrame(data={'Season': np.arange(st_season, end_season)})
        df_subindex[sub_index.name] = df_subindex['Season'].transform(
            lambda x: self.data[sub_index.name][
                utils.season_convertor(self.data['Date'], x, self.season_start, risk_start, risk_end)].sum())
        if self.model['burn'].empty:
            self.model['burn'] = df_subindex
        else:
            self.model['burn'] = pd.merge(self.model['burn'], df_subindex, how='left', on=['Season'])

    def add_index(self, leg):
        for sub_index in leg.vanilla_index:
            if not sub_index.name in self.model['burn'].columns:
                self.add_sub_index(sub_index)
                if not leg.name in self.model['burn'].columns:
                    self.model['burn'][leg.name] = pd.Series(self.model['burn'][sub_index.name] * sub_index.weight)
                else:
                    self.model['burn'][leg.name] += pd.Series(self.model['burn'][sub_index.name] * sub_index.weight)

    def calc_ar1factor(temp_df, window_yrs):
        yearmonth_TAvg_df = temp_df.groupby('Year-Month').mean()
        # yearmonth_TAvg_df['LTAvg'] = yearmonth_TAvg_df.groupby('Month')['TAvg'].transform(lambda x : yearmonth_TAvg_df = yearmonth_TAvg_df).dropna()
        # yearmonth_TAvg_dfrDeviationi = yearmonth_TAvg_dfUTAvgi - yearmonth_TAvg_df['LTAvg']
        # ar1_factor = yearmonth_TAvg_dfrDeviation'11:-4].corr(yearmonth_TAvg_df['Deviation'][5:])
        # return arl_factor, yearmonth_TAvg_dfrDeviation
        pass


class VanillaSingleLegModel(VanillaBurnModel):
    def __init__(self, vanilla, data=None, season_start=None):
        super().__init__(vanilla, data, season_start=season_start)

        self.leg = self.vanilla.legs[0]
        self.index_name = self.leg.name
        df = self.model['burn'][["Season", self.index_name]]

        # Fit a linear trend to the historical index and test for statistical significance
        self.model['regression'] = dict()
        self.model['regression']['offset'] = 1990
        x = sm.add_constant(df['Season'] - 1990)
        mod = sm.OLS(df[self.index_name], x)
        res = mod.fit()
        res.summary()

        # Check if regression is significant
        reg_significant = True
        for t in res.tvalues:
            if np.abs(t) < 2.5:
                reg_significant = False
                break

        self.model['regression']['reg_significant'] = reg_significant
        self.model['regression']['slope'] = res.params['Season']
        self.model['regression']['const'] = res.params['const']
        self.model['regression']['se_slope'] = res.HC0_se['Season']
        self.model['regression']['se_const'] = res.HC0_se['const']

        # DeTrend and calculate residuals
        if reg_significant:
            df['Trend'] = res.params['const'] + \
                          res.params['Season'] * (df['Season'] - self.model['regression']['offset'])
        else:
            df['Trend'] = df[self.index_name].mean()
        df['Resid'] = df[self.index_name] - df['Trend']
        self.model['burn']['Trend'] = df['Trend']
        self.model['burn']['Resid'] = df['Resid']
        # df.plot(x='Season', y=[self.index_name, 'Trend'])

        fit_dists = ['norm', 'expon', 'gumbel_r', 'gamma', 'laplace', 'gumbel_l', 'beta']
        self.model['fit_results'] = utils.fit_dist(df['Resid'], fit_dists)

        n_sim = 10000
        sim = pd.DataFrame()
        sim['Season'] = np.arange(n_sim)
        sim_projection = 0
        for result in self.model['fit_results']:
            dist_name = result['dist_name']
            dist_params = result['zm_params']
            dist_sim = utils.dists[dist_name](*tuple(dist_params))
            sim[dist_name] = dist_sim.rvs(n_sim) + sim_projection
            sim[dist_name + '_PayOff'] = self.leg.payoff.payoff(sim[dist_name])

        self.model['sim'] = sim
        pass
