from wx.models import wx_helpers
from wx.models import wx_pricers
from wx.providers.trade.schema import VanillaPayOffSchema, VanillaSubIndexSchema, LegSchema, VanillaSchema
import time

vs = {
    'location': 'Minneapolis_WBAN_14922',
    'index': 'HDD',
    'index_threshold': 65.0,
    'index_aggregation': 'sum',
    'underlying': 'TAvg',
    'underlying_unit': 'F',
    'risk_start': '2020-11-01',
    'risk_end': '2020-12-31',
    'weight': 1.0
}

vs2 = {
    'location': 'Dallas_WMO_72259',
    'index': 'CDD',
    'index_threshold': 65.0,
    'index_aggregation': 'sum',
    'underlying': 'TAvg',
    'underlying_unit': 'F',
    'risk_start': '2019-05-01',
    'risk_end': '2019-10-31',
    'weight': 1.0
}

vs3 = {
    'location': 'McAllenMiller_WMO_722506',
    'index': 'CDD',
    'index_threshold': 65.0,
    'index_aggregation': 'sum',
    'underlying': 'TAvg',
    'underlying_unit': 'F',
    'risk_start': '2019-05-01',
    'risk_end': '2019-10-31',
    'weight': 1.0
}

vs4 = {
    'location': 'MidlandInternational_WMO_72265',
    'index': 'CDD',
    'index_threshold': 65.0,
    'index_aggregation': 'sum',
    'underlying': 'TAvg',
    'underlying_unit': 'F',
    'risk_start': '2019-05-01',
    'risk_end': '2019-10-31',
    'weight': 1.0
}

vs5 = {
    'location': 'MidlandInternational_WMO_72265',
    'index': 'CDD',
    'index_threshold': 65.0,
    'index_aggregation': 'sum',
    'underlying': 'TAvg',
    'underlying_unit': 'F',
    'risk_start': '2020-05-01',
    'risk_end': '2020-10-31',
    'weight': 1.0
}

v_s = wx_helpers.VanillaSubIndex(vs)
v_s2 = wx_helpers.VanillaSubIndex(vs2)
v_s3 = wx_helpers.VanillaSubIndex(vs3)
v_s4 = wx_helpers.VanillaSubIndex(vs4)
v_s5 = wx_helpers.VanillaSubIndex(vs5)
schema1 = VanillaSubIndexSchema()
pf1 = schema1.dumps(v_s)
pff1 = schema1.loads(pf1)


pay = {
    'type': 'swap',
    'strike': 2200.0,
    'notional': 46000.0,
    'buysell': 'buy',
    'limit_lc': None,
    'limit_cpty': None
}

pay2 = {
    'type': 'swap',
    'strike': 2892.0,
    'notional': 5175.0,
    'buysell': 'buy',
    'limit_lc': None,
    'limit_cpty': None
}

pay3 = {
    'type': 'swap',
    'strike': 3759.0,
    'notional': 2882.0,
    'buysell': 'buy',
    'limit_lc': None,
    'limit_cpty': None
}

pay4 = {
    'type': 'swap',
    'strike': 2551.0,
    'notional': 1092.0,
    'buysell': 'buy',
    'limit_lc': None,
    'limit_cpty': None
}

p = wx_helpers.VanillaPayoff(pay)
schema = VanillaPayOffSchema()
pf1 = schema.dumps(p)
pff1 = schema.loads(pf1)

p2 = wx_helpers.VanillaPayoff(pay2)
p3 = wx_helpers.VanillaPayoff(pay3)
p4 = wx_helpers.VanillaPayoff(pay4)


l = {
    'vanilla_index': [v_s],
    'name': 'MSP',
    'payoff': p
}

l2 = {
    'vanilla_index': [v_s2],
    'name': 'Dallas',
    'payoff': p2
}

l3 = {
    'vanilla_index': [v_s3],
    'name': 'McAllen',
    'payoff': p3
}

l4 = {
    'vanilla_index': [v_s4],
    'name': 'MidLand',
    'payoff': p4
}

l5 = {
    'vanilla_index': [v_s5],
    'name': 'Midland1',
    'payoff': p4
}

leg = wx_helpers.Leg(l)
leg_schema = LegSchema()
ls1 = leg_schema.dumps(leg)
leg2 = wx_helpers.Leg(l2)
leg3 = wx_helpers.Leg(l3)
leg4 = wx_helpers.Leg(l4)
leg5 = wx_helpers.Leg(l5)

v = {
    'legs': [leg],
    'aggregate_limit_lc': 50000000.0,
    'aggregate_limit_cpty': 50000000.0,
    'aggregate_deductible': 0.0,
    'counterparty': 'CentralHudson',
    'risk_region' : "USA",
    'risk_sub_region': "MidWest",
    'create_date_time': "None",
    'quoted_date_time': "None",
    'quoted_y_n': "No",
    'traded_y_n': "No",
    'traded_date_time': "No",
    'deal_number': "DL-55555"
}

vanilla = wx_helpers.Vanilla(v)
vanilla_schema = VanillaSchema()
v_json = vanilla_schema.dumps(vanilla)
# vp = wx_pricers.VanillaPricer(vanilla, 'VanillaBurn')
t1 = time.time()
vp = wx_pricers.VanillaPricer(vanilla, 'VanillaSingleLeg')
t2 = time.time()
print(t2-t1)
print(vp.model)
