from wx.models.gas_hedging import gas_hedging_functions as ghf
from xlib import xdb
import pandas as pd
import numpy as np
db='WX1-GC'
###CONVERT BELOW INTO A FUNCTION
###VARIABLES TO SPECIFY MANUALLY
current_season = 2020
hedge_contracts = [['ALQ'], ['DGD'], ['NMC'], ['NTO'], ['NWR'], ['TPB'], ['TMT'], ['KTB'], ['DKR'], ['TZS'], ['TRZ'],
                   ['H'], ['M'], ['TFM']]
ind_months = [11, 12, 1, 2, 3]  # list individual months in risk period
hedge_in_advances = [1, 2, 3]  # how many months before risk period will hedge be put on
risks = ['strip', 'monthly']
###END OF VARIABLES TO SPECIFY MANUALLY
for i in hedge_contracts:
    # CREATE STRING OF HEDGE CONTRACTS (IN CASE OF MULTIPLE)
    hedge_contract = ''

    for j in i:
        hedge_contract = hedge_contract + "'" + j + "',"
    hedge_contract = hedge_contract[:-1]
    for m in risks:
        risk = m
        for k in hedge_in_advances:
            hedge_in_advance = k
            if ind_months[0] - hedge_in_advance < 0:
                hedge_initial = ind_months[0] - hedge_in_advance + 12
            else:
                hedge_initial = ind_months[0] - hedge_in_advance
            # GET PRICES AT TIME OF HEDGING
            df_hedge_price = ghf.get_hedge_prices_no_options(db, ind_months, hedge_contract, hedge_in_advance)
            df_henry_price = ghf.get_henry_prices_no_options(db, ind_months, hedge_in_advance)
            # GET EXPIRATION PRICES
            df_end_price = ghf.get_prices_at_expiry(db, hedge_contract)
            df_henry_end_price = ghf.get_henry_prices_at_expiry(db)
            df_hedge_price_fin = ghf.format_hedge_price(ind_months, df_hedge_price, df_henry_price, hedge_in_advance,
                                                    hedge_initial)
            df_price_final = ghf.format_prices(df_hedge_price_fin, df_end_price, df_henry_end_price, ind_months,
                                           current_season, risk, hedge_initial)
            result_db = 'WX2-GC'
            table = 'GAS_HEDGE_WINTER_SIMS_HEDGE_PRICES'
            df_price_final['hedge_in_advance'] = hedge_in_advance
            df_price_final['fc'] = str(ind_months[0]) + '-' + str(ind_months[-1])
            conn = xdb.make_conn(result_db, stay_open=True)
            conn.bulkInsertDf(df_price_final, table, local=True)
            conn.commit()
            conn.close()
    print('Finished ' + hedge_contract)