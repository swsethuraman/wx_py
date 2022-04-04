from pyxll import xl_func, xl_macro, xl_app, xlcAlert, XLCell, DataFrameFormatter, async_call, get_type_converter
from wx.models.wx_helpers import VanillaSubIndex, VanillaPayoff, Leg, Vanilla, PriceSubIndex, PricePayoff
from wx.providers.trade.schema import VanillaSchema
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
def create_vanilla_subindex(sub_index_spec):
    sub_index_spec = common.sanitize_none(sub_index_spec)
    vs = VanillaSubIndex(sub_index_spec)
    return vs


@xl_func("dict <str, var> payoff_spec: object")
def create_vanilla_payoff(payoff_spec):
    payoff_spec = common.sanitize_none(payoff_spec)
    vp = VanillaPayoff(payoff_spec)
    return vp


@xl_func("str name, object payoff, object[] si_list, object *args: object")
def create_vanilla_leg(name, payoff, si_list=None, *args):
    if si_list is None:
        vi = args
    else:
        if isinstance(si_list[0], list):
            si_list = si_list[0]
        vi = [si for si in si_list if si is not None]

    leg_spec = {
        'vanilla_index': vi,
        'name': name,
        'payoff': payoff
    }
    vl = Leg(leg_spec)
    return vl


@xl_func("dict <str, var> vanilla_spec: object")
def create_vanilla_trade(vanilla_spec):
    v = Vanilla(vanilla_spec)
    return v


@xl_func("object vanilla, str model: object")
def px_vanilla(vanilla, model="VanillaBurn"):
    return VanillaPricer(vanilla, model)


@xl_func("str fields, object pricer: dataframe<index=True>", auto_resize=True)
def get_df(field, pricer):
    return getattr(pricer, field)


@xl_func("str fields, dict d: dataframe<index=True>", auto_resize=True)
def get_df_from_dict(field, d):
    return d[field]

@xl_func("str key, dict d: var")
def get_value_from_dict(key, d):
    return d[key]


@xl_func("str key, dict d: object")
def get_obj_from_dict(key, d):
    if isinstance(d[key], tuple):
        return list(d[key])
    return d[key]


@xl_func("int index, object lst: object")
def get_list_element(index, lst):
    return lst[index]


@xl_func("object lst: int")
def get_list_size(lst):
    return len(lst)


@xl_macro("object vanilla, str filepath: str")
def save_vanilla_trade(vanilla, filepath):
    schema = VanillaSchema()
    # vanilla_json = vanilla.desc()
    vanilla_json = schema.dump(vanilla)
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


@xl_func("dict <str, str> constraints: object", auto_resize=False)
@xl_macro("dict <str, str>: object")
def load_wx_daily(constraints):
    #data_columns = ['RecordDate', 'TMax', 'TMin']
    data_columns = ['OPR_DATE', 'TMAX', 'TMIN']
    df = wp.get_wx_daily(constraints, data_columns)
    return df


@xl_func("str[] city_list, date start_date: str", macro=True)
@xl_macro("str[] city_list, date start_date: str")
def load_wx_daily_array(city_list, start_date):
    sd = pd.to_datetime(start_date)
    df = pd.DataFrame()
    count = 1
    for city in city_list:
        if not city == 'x':
            city_s = city.split("_")
            constraints = {city_s[1]: city_s[2]}
            print(constraints)
            #data_columns = ['RecordDate', 'TMax', 'TMin']
            data_columns = ['OPR_DATE', 'TMAX', 'TMIN']
            df_temp = wp.get_wx_daily(constraints, data_columns)
            #df_temp = df_temp.rename(columns={'TMax': city + '_TMax', 'TMin': city + '_TMin', 'TAvg': city + '_TAvg'})
            df_temp = df_temp.rename(columns={'TMax': city + '_TMax', 'TMin': city + '_TMin', 'TAvg': city + '_TAvg'})
            if count == 1:
                df = df_temp
            else:
                df = df.merge(df_temp, on='Date')
        count = count + 1

    df = df[df['Date'] >= sd]
    xl = xl_app()
    xl_range = xl.Range("hist_dump")
    cell = XLCell.from_range(xl_range)
    # formatter = DataFrameFormatter()
    cell.options(type="dataframe<index=False>", auto_resize=True).value = df
    return "Success"


#@xl_func("dict <str, str> constraints: str", macro=True)
@xl_func("dict <str, str> constraints, str position: str", macro=True)
def set_wx_daily(constraints,position):

    def update_func():
        df = load_wx_daily(constraints)
        xl = xl_app()
        #xl_range = xl.Range("wx_1")
        xl_range = xl.Range(position)
        cell = XLCell.from_range(xl_range)
        cell.options(type="object").value = df
        pass

    async_call(update_func())
    return "Success"


@xl_func("object sub_index, object data: dataframe<index=False>", auto_resize=True)
def calc_sub_index(sub_index, data):
    df = sub_index.calc_sub_index(data)
    return df


@xl_func("str filepath, str filename: object")
def load_vanilla_trade(filepath, filename):
    full_filename = os.path.join(filepath, filename)
    print(full_filename)
    with open(full_filename) as infile:
        vanilla_json = json.load(infile)
    print(vanilla_json)
    schema = VanillaSchema()
    vanilla = schema.loads(json.dumps(vanilla_json))
    print(vanilla)
    return vanilla


@xl_func("object[] si_list: object")
def create_list(si_list):
    vi = [si for si in si_list if si is not None]
    return vi


@xl_func("object function, float[] x: float[]")
def eval_fn(f, x):
    x = np.array(x).reshape((-1,))
    fx = f(x)
    return fx.tolist()


@xl_func("str fld, object obj: object")
def get_obj_field(fld, obj):
    return getattr(obj, fld)


@xl_func("date[] date_vector, int season, date season_start, date risk_start, date risk_end: int")
def utils_season_convertor(date_vector, season, season_start, risk_start, risk_end):
    print("Hello")
    print(risk_end)
    if len(date_vector) == 1:
        date_vector = pd.to_datetime(date_vector[0])
    risk_start = pd.to_datetime(risk_start)
    risk_end = pd.to_datetime(risk_end)
    season_start = pd.to_datetime(season_start)
    print(date_vector)
    print(season)
    print(season_start)
    print(risk_start)
    print(risk_end)

    return season_convertor(date_vector, season, season_start, risk_start, risk_end)


@xl_func("object x, str cast: var")
def cast_into(x, cast="str"):
    if cast == 'str':
        return str(x)
    elif cast == 'date':
        return pd.to_datetime(x)
    elif cast == 'float':
        return float(x)
    else:
        return x


@xl_func("str wban: dataframe<index=True>", auto_resize=True)
def get_homr_locations(wban):
    query_sql = "Select * from WX1.HOMR_STATION_LOCATION_HIST where WBAN={0}".format(wban)
    df = wp.run_query_df(query_sql)
    return df.reset_index(drop=True)


@xl_func("str opt_type, float mean, float stdev, float notional, float strike, float limit, str result: float")
def bachelier(opt_type, mean, stdev, notional, strike, limit=1e9, result='px'):
    return option_px.bachelier(opt_type, mean, stdev, notional, strike, limit=limit, result=result)


@xl_func("str opt_type, float mean, float notional, float strike, float px, float limit: float")
def bachelier_implied_vol(opt_type, mean, notional, strike, px, limit=None):
    return option_px.bachelier_implied_vol(opt_type, mean, notional, strike, px, limit=limit)


@xl_func("str x: str", macro=True)
def get_quotes(x):
    df = wp2.run_query_df(query_sql="SELECT * FROM WX2.QUOTES_DESK_PRICER_W_TRADES_V WHERE OPTION_TYPE = '" + str(x) + "'")
    xl = xl_app()
    xl_range = xl.Range("wx_"+str(x)+"_quotes")
    cell = XLCell.from_range(xl_range)
    cell.options(type="dataframe<index=False>", auto_resize=True).value = df
    return "Success"

@xl_func("str id_type, str id: str", macro=True)
def get_waterfall(id_type, id):
    df = wp2.run_query_df(query_sql="SELECT " + str(id_type) + ", OPR_DATE, TMIN, TMAX FROM WX1.WX_WEATHER_DAILY_CLEANED WHERE " + str(id_type) + " = '" + str(id) + "'")
    xl = xl_app()
    xl_range = xl.Range("paste_spot")
    cell = XLCell.from_range(xl_range)
    cell.options(type="dataframe<index=False>", auto_resize=True).value = df
    #return "SELECT " + str(id_type) + ", OPR_DATE, TMIN, TMAX FROM WX1.WX_WEATHER_DAILY_CLEANED WHERE " + str(id_type) + " = '" + str(id) + "'"

@xl_func("float[][] arr: float[][]","str table: str", auto_resize=True, macro=True)
def excel_to_db(arr, table, cols=None, cond=None):
    #x = np.array(arr)
    df = pd.DataFrame(arr)
    #xl = xl_app()
    #xl_range = xl.Range("puthere")
    #cell = XLCell.from_range(xl_range)
    #cell.options(type="dataframe<index=False>", auto_resize=True).value = df
    df = get_type_converter("dataframe","var")
    # = pd.DataFrame(array)
    #if cond == None:
    #    pass
    #else:
    #    df = df.loc[np.where(df['IN DB'] != 'Y')]
    #df = df[cols]
    #wp2.insert_to_db(df,str(table))
    #df = get_type_converter("dataframe","var")
    return df


