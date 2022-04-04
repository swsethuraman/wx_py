from wx.utils import payoffs
from wx.utils.wx_index import wx_index
from wx.utils import utils
import wx.providers.trade.schema as schema
import json
from datetime import datetime
import pandas as pd
import numpy as np

class VanillaPayoff:
    def __init__(self, payoff_spec):

        self.type = payoff_spec['type']
        self.strike = payoff_spec['strike']
        self.notional = payoff_spec['notional']
        self.buysell = payoff_spec['buysell']
        self.limit_lc = payoff_spec['limit_lc']
        self.limit_cpty = payoff_spec['limit_cpty']
        self.schema = schema.VanillaPayOffSchema()
        self.payoff = None

        if self.type == 'call':
            if self.buysell == 'sell':
                self.payoff = lambda x: payoffs.call(x, self.strike, self.notional,
                                                     limit=self.limit_lc)
            else:
                self.payoff = lambda x: -1 * payoffs.call(x, self.strike, self.notional,
                                                          limit=self.limit_cpty)

        if self.type == 'put':
            if self.buysell == 'sell':
                self.payoff = lambda x: payoffs.put(x, self.strike, self.notional,
                                                    limit=self.limit_lc)
            else:
                self.payoff = lambda x: -1 * payoffs.put(x, self.strike, self.notional,
                                                         limit=self.limit_cpty)

        if self.type == 'swap':
            if self.buysell == 'sell':
                self.payoff = lambda x: payoffs.swap(x, self.strike, self.notional,
                                                     limit_plus=self.limit_lc,
                                                     limit_minus=self.limit_cpty)
            else:
                self.payoff = lambda x: payoffs.swap(self.strike, x, self.notional,
                                                     limit_plus=self.limit_lc,
                                                     limit_minus=self.limit_cpty)

    def desc(self):
        return self.schema.dumps(self)


class VanillaSubIndex:
    def __init__(self, sub_index_spec):
        self.location = sub_index_spec['location']
        self.index = sub_index_spec['index']
        self.index_threshold = sub_index_spec['index_threshold']
        if 'index_daily_max' in sub_index_spec.keys():
            self.index_daily_max = sub_index_spec['index_daily_max']
        else:
            self.index_daily_max = None
        if 'index_daily_min' in sub_index_spec.keys():
            self.index_daily_min = sub_index_spec['index_daily_min']
        else:
            self.index_daily_min = None

        self.index_aggregation = sub_index_spec['index_aggregation']
        self.underlying = sub_index_spec['underlying']
        self.underlying_unit = sub_index_spec['underlying_unit']
        self.risk_start = datetime.strptime(sub_index_spec['risk_start'], '%Y-%m-%d')
        self.risk_end = datetime.strptime(sub_index_spec['risk_end'], '%Y-%m-%d')
        self.weight = sub_index_spec['weight']
        self.name = self.location + '_' + str(self.risk_start.strftime('%Y-%m-%d')) + \
                    '_' + str(self.risk_end.strftime('%Y-%m-%d')) + '_' + self.underlying + \
                    '_' + self.index + str(self.index_threshold)
        self.transform = lambda x: wx_index[self.index](x, self.index_threshold, self.index_daily_max, self.index_daily_min)
        self.schema = schema.VanillaSubIndexSchema()

    def desc(self):
        return self.schema.dumps(self)

    def calc_sub_index(self, data, season_start=None):
        risk_start = self.risk_start
        risk_end = self.risk_end

        data[self.name] = self.transform(data[self.underlying])

        if season_start is None:
            self.season_start = risk_start

        st_season = data['Date'][0].year
        end_season = self.season_start.year

        df_subindex = pd.DataFrame(data={'Season': np.arange(st_season, end_season)})
        df_subindex[self.name] = df_subindex['Season'].transform(
            lambda x: data[self.name][
                utils.season_convertor(data['Date'], x, self.season_start, risk_start, risk_end)].sum())
        return df_subindex


class Leg:
    def __init__(self, leg_spec):
        self.vanilla_index = leg_spec['vanilla_index']
        self.payoff = leg_spec['payoff']
        self.name = leg_spec['name']
        self.schema = schema.LegSchema()

    def desc(self):
        return self.schema.dumps(self)


class Vanilla:
    def __init__(self, vanilla_spec):
        self.legs = vanilla_spec['legs']
        self.aggregate_limit_lc = vanilla_spec['aggregate_limit_lc']
        self.aggregate_limit_cpty = vanilla_spec['aggregate_limit_cpty']
        self.aggregate_deductible = vanilla_spec['aggregate_deductible']
        self.counterparty = vanilla_spec['counterparty']
        self.risk_region = vanilla_spec['risk_region']
        self.risk_sub_region = vanilla_spec['risk_sub_region']
        self.create_date_time = vanilla_spec['create_date_time']
        self.quoted_date_time = vanilla_spec['quoted_date_time']
        self.quoted_y_n = vanilla_spec['quoted_y_n']
        self.traded_y_n = vanilla_spec['traded_y_n']
        self.traded_date_time = vanilla_spec['traded_date_time']
        self.deal_number = vanilla_spec['deal_number']
        self.fair_value = vanilla_spec['fair_value']
        self.premium = vanilla_spec['premium']
        self.schema = schema.VanillaSchema()

    def desc(self):
        return self.schema.dumps(self)

    def desc(self):
        return self.schema.dumps(self)

    def calc_sub_index(self, data, season_start=None):
        risk_start = self.risk_start
        risk_end = self.risk_end

        data[self.name] = self.transform(data[self.underlying])

        if season_start is None:
            self.season_start = risk_start

        st_season = data['Date'][0].year
        end_season = self.season_start.year

        df_subindex = pd.DataFrame(data={'Season': np.arange(st_season, end_season)})
        df_subindex[self.name] = df_subindex['Season'].transform(
            lambda x: data[self.name][
                utils.season_convertor(data['Date'], x, self.season_start, risk_start, risk_end)].sum())
        return df_subindex


class QuantoLeg:
    def __init__(self, leg_spec):
        self.wx_index = leg_spec['wx_index']
        self.wx_payoff = leg_spec['wx_payoff']
        self.cmdty_index = leg_spec['cmdty_index']
        self.cmdty_payoff = leg_spec['cmdty_payoff']
        self.name = leg_spec['name']

class Quanto:
    def __init__(self, quanto_spec):
        self.legs = quanto_spec['legs']
        self.aggregate_limit_lc = quanto_spec['aggregate_limit_lc']
        self.aggregate_limit_cpty = quanto_spec['aggregate_limit_cpty']
        self.aggregate_deductible = quanto_spec['aggregate_deductible']

    def desc(self):
        return json.dumps(self.attrib, default=str)

class PricePayoff:
    def __init__(self, payoff_spec):

        self.type = payoff_spec['type']
        self.strike = payoff_spec['strike']
        self.notional = payoff_spec['notional']
        self.buysell = payoff_spec['buysell']
        self.limit_lc = payoff_spec['limit_lc']
        self.limit_cpty = payoff_spec['limit_cpty']
        self.schema = schema.PricePayOffSchema()
        self.payoff = None

        if self.type == 'call':
            if self.buysell == 'sell':
                self.payoff = lambda x: payoffs.call(x, self.strike, self.notional,
                                                     limit=self.limit_lc)
            else:
                self.payoff = lambda x: -1 * payoffs.call(x, self.strike, self.notional,
                                                          limit=self.limit_cpty)

        if self.type == 'put':
            if self.buysell == 'sell':
                self.payoff = lambda x: payoffs.put(x, self.strike, self.notional,
                                                    limit=self.limit_lc)
            else:
                self.payoff = lambda x: -1 * payoffs.put(x, self.strike, self.notional,
                                                         limit=self.limit_cpty)

        if self.type == 'swap':
            if self.buysell == 'sell':
                self.payoff = lambda x: payoffs.swap(x, self.strike, self.notional,
                                                     limit_plus=self.limit_lc,
                                                     limit_minus=self.limit_cpty)
            else:
                self.payoff = lambda x: payoffs.swap(self.strike, x, self.notional,
                                                     limit_plus=self.limit_lc,
                                                     limit_minus=self.limit_cpty)

    def desc(self):
        return self.schema.dumps(self)

class PriceSubIndex:
    def __init__(self, sub_index_spec):
        self.location = sub_index_spec['location']
        self.type = sub_index_spec['type']
        self.hours = sub_index_spec['hours']
        if 'index_daily_max' in sub_index_spec.keys():
            self.index_daily_max = sub_index_spec['index_daily_max']
        else:
            self.index_daily_max = None
        if 'index_daily_min' in sub_index_spec.keys():
            self.index_daily_min = sub_index_spec['index_daily_min']
        else:
            self.index_daily_min = None

        self.index_aggregation = sub_index_spec['index_aggregation']
        self.forward_source = sub_index_spec['forward_source']
        self.forward_id = sub_index_spec['forward_id']
        self.risk_start = datetime.strptime(sub_index_spec['risk_start'], '%Y-%m-%d')
        self.risk_end = datetime.strptime(sub_index_spec['risk_end'], '%Y-%m-%d')
        self.weight = sub_index_spec['weight']
        self.name = self.location + '_' + str(self.risk_start.strftime('%Y-%m-%d')) + \
                    '_' + str(self.risk_end.strftime('%Y-%m-%d')) + '_' + self.location + \
                    '_' + self.type + str(self.hours)
        #self.transform = lambda x: wx_index[self.index](x, self.index_threshold, self.index_daily_max, self.index_daily_min)
        self.schema = schema.PriceSubIndexSchema()