import pandas as pd
import numpy as np
import json
import os
from xlib import xdb

def load_pricing_jsons():
    db = 'WX2-GC'
    directory = '/shared/wx/Models/PROD/pricing/'
    df_main = pd.DataFrame(columns = ['filename','counterparty','risk_region','deal_number','aggregate_limit_cpty','aggregate_limit_lc','traded_date_time','create_date_time','risk_sub_region','quoted_y_n','aggregate_deductible','quoted_date_time','traded_y_n'])
    df_vi = pd.DataFrame(columns = ['filename','deal_number','name','vi_name','index_threshold','index_type','risk_start','risk_end','index_daily_max','index_daily_min','weight','index_aggregation','underlying','location','underlying_unit'])
    df_po = pd.DataFrame(columns = ['filename','deal_number','name','limit_lc','strike','limit_cpty','type','buysell','notional'])
    for filename in os.listdir(directory):
        if filename!='Inserted':
            with open(directory+filename) as file:
                data = json.load(file)
            counterparty = data['counterparty']
            risk_region = data['risk_region']
            deal_number = data['deal_number']
            aggregate_limit_cpty = data['aggregate_limit_cpty']
            aggregate_limit_lc = data['aggregate_limit_lc']
            traded_date_time = data['traded_date_time']
            create_date_time = data['create_date_time']
            risk_sub_region = data['risk_sub_region']
            quoted_y_n = data['quoted_y_n']
            aggregate_deductible = data['aggregate_deductible']
            quoted_date_time = data['quoted_date_time']
            traded_y_n = data['traded_y_n']
            new_row = {'filename':filename,'counterparty':counterparty, 'risk_region':risk_region, 'deal_number':deal_number, 'aggregate_limit_cpty':aggregate_limit_cpty, 'aggregate_limit_lc':aggregate_limit_lc, 'traded_date_time':traded_date_time, 'create_date_time':create_date_time, 'risk_sub_region':risk_sub_region, 'quoted_y_n':quoted_y_n, 'aggregate_deductible':aggregate_deductible, 'quoted_date_time':quoted_date_time, 'traded_y_n':traded_y_n}
            df_main = df_main.append(new_row, ignore_index=True)

            legs = data['legs']
            for i in range(len(legs)):
                try:
                    name=legs[i]['name']
                except:
                    name='Unnamed'
                for j in range(len(legs[i]['vanilla_index'])):
                    vanilla_index = legs[i]['vanilla_index'][j]
                    vi_name = vanilla_index['name']
                    index_threshold = vanilla_index['index_threshold']
                    index = vanilla_index['index']
                    risk_start = vanilla_index['risk_start']
                    risk_end = vanilla_index['risk_end']
                    index_daily_max = vanilla_index['index_daily_max']
                    index_daily_min = vanilla_index['index_daily_min']
                    weight = vanilla_index['weight']
                    index_aggregation = vanilla_index['index_aggregation']
                    underlying = vanilla_index['underlying']
                    location = vanilla_index['location']
                    underlying_unit = vanilla_index['underlying_unit']
                    new_row = {'filename':filename,'deal_number':deal_number, 'name':name, 'vi_name':vi_name, 'index_threshold':index_threshold, 'index_type':index, 'risk_start':risk_start, 'risk_end':risk_end, 'index_daily_max':index_daily_max, 'index_daily_min':index_daily_min, 'weight':weight, 'index_aggregation':index_aggregation, 'underlying':underlying, 'location':location, 'underlying_unit':underlying_unit}
                    df_vi = df_vi.append(new_row, ignore_index=True)

            for k in range(len(legs)):
                name=legs[k]['name']
                payoff = legs[k]['payoff']
                limit_lc = payoff['limit_lc']
                strike = payoff['strike']
                limit_cpty = payoff['limit_cpty']
                type = payoff['type']
                buysell = payoff['buysell']
                notional = payoff['notional']
                new_row = {'filename':filename,'deal_number':deal_number, 'name':name, 'limit_lc':limit_lc,'strike':strike,'limit_cpty':limit_cpty, 'type':type, 'buysell':buysell, 'notional':notional }
                df_po = df_po.append(new_row, ignore_index=True)

    conn = xdb.make_conn(db, stay_open=True)
    conn.bulkInsertDf(df_main, 'Deal_Pricing_Summary', local=True)
    conn.bulkInsertDf(df_vi, 'Deal_Pricing_Legs', local=True)
    conn.bulkInsertDf(df_po, 'Deal_Pricing_Payouts', local=True)
    conn.commit()
    conn.close()
