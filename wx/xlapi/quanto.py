from pyxll import xl_func, xl_macro, xl_app, xlcAlert, XLCell, DataFrameFormatter, async_call, get_type_converter
from wx.models.wx_helpers import VanillaSubIndex, VanillaPayoff, Leg, Vanilla, PriceSubIndex, PricePayoff
from wx.providers.trade.schema import QuantoSchema
from wx.models.wx_pricers import VanillaPricer
from wx.providers.market_data.WX1_provider import Wx1Provider
from wx.providers.market_data.WX2_provider import Wx2Provider
import wx.utils.common as common
from wx.utils.utils import season_convertor
import wx.utils.option_px as option_px
import json
import os
import numpy as np
import pandas as pd

wp = Wx1Provider()
wp2 = Wx2Provider()

@xl_func("dict <str, var> sub_index_spec: object")
def create_price_subindex(sub_index_spec):
    sub_index_spec = common.sanitize_none(sub_index_spec)
    ps = PriceSubIndex(sub_index_spec)
    return ps

@xl_func("dict <str, var> payoff_spec: object")
def create_price_payoff(payoff_spec):
    payoff_spec = common.sanitize_none(payoff_spec)
    pp = PricePayoff(payoff_spec)
    return pp

@xl_func("str name, object payoff, object[] si_list, object *args: object")
def create_price_leg(name, payoff, si_list=None, *args):
    if si_list is None:
        pi = args
    else:
        if isinstance(si_list[0], list):
            si_list = si_list[0]
        pi = [si for si in si_list if si is not None]

    leg_spec = {
        'price_index': pi,
        'name': name,
        'payoff': payoff
    }
    pl = Leg(leg_spec)
    return pl

def save_quanto_trade(vanilla, filepath):
    schema = QuantoSchema()
    # vanilla_json = vanilla.desc()
    quanto_json = schema.dump(vanilla)
    vanilla_filename = vanilla.deal_number + '.json'
    full_filename = os.path.join(filepath, vanilla_filename)

    if not os.path.isfile(full_filename):
        try:
            print(vanilla_json)
            # with open(full_filename, 'w') as outfile:
            #     outfile.write(vanilla_json)
            with open(full_filename, 'w') as outfile:
                json.dump(vanilla_json, outfile, indent=3)
            msg = "Successfully saved vanilla trade!"
        except Exception as e:
            msg = "Failed to save vanilla."
    else:
        msg = "DL exists already. Did not save file."
    return msg