from wx.models.gas_hedging import gas_hedging_functions as ghf
from xlib import xdb
import pandas as pd
import numpy as np
db='WX1-GC'
###CONVERT BELOW INTO A FUNCTION
###VARIABLES TO SPECIFY MANUALLY
current_season=2020
min_winter=2012
exposure='warm' #warm or cold
region='NA' #NA or EU
hedge_locations=[['KORD'],['KBOS'],['KIAH'],['KDFW'],['KCVG'],['KMSP'],['KDCA'],['KEWR'],['KLGA'],['KATL'],['KLAS'],['KLAX'],['KSAC']]
risks=['monthly','strip'] #strip or monthly
ind_months = [11,12,1,2,3]#list individual months in risk period
hedge_in_advance_list = [1,2,3] #how many months before risk period will hedge be put on

curve_weighting_matrix = [
    [[1,1,1,1,1],[1,1,1,1],[1,1,1],[1,1],[1]],
    [[0.75,1,1,1,1.25], [0.75,1,1,1.25],[0.75,1,1.25],[0.75,1.25],[1]],
    [[0.5,0.75,1,1.25,1.5],[0.5,0.75,1.25,1.5],[0.5,1,1.5],[0.5,1.5],[1]],
    [[0.33,0.5,1.3,1.4,1.5],[0.5,0.75,1.25,1.5],[0.75,1,1.25],[0.75,1.25],[1]],
    [[0.25,0.5,1.25,1.5,1.5],[0.25,0.75,1.5,1.5],[0.25,1.25,1.75],[0.25,1.75],[1]],
    [[0,0.25,1.5,1.5,1.75],[0,0.5,1.5,1.75],[0,1.5,1.75],[0,2],[1]]
]
###END OF VARIABLES TO SPECIFY MANUALLY

#DONT CHANGE THESE (FIXED PARAMETERS)
fc = str(ind_months[0]) + '-' + str(ind_months[-1])
risk_months = []
strip = []
strip_str=''
for im in ind_months:
    risk_months.append(str(im))
    strip_str+=str(im)+'_'
strip.append(strip_str[:-1])
winter_exposure = 1000
if exposure=='warm':
    lau_side = 1
else:
    lau_side= -1

for i in hedge_locations:
    print(i)
    hedge_location = i[0]
    df_weather = ghf.get_weather_data(db, risk_months, hedge_location)
    if hedge_location=='KATL':
        bias = [2,6,6,5,1] #market bias for each month in risk period
    if hedge_location=='KDFW':
        bias = [2,6,6,5,1] #market bias for each month in risk period
    else:
        bias = [0,0,0,0,0] #market bias for each month in risk period
    for k in risks:
        risk = k
        df_weather_temp = ghf.format_weather(df_weather, hedge_location, risk)
        sql_hedge_contracts ="""
            select contract, 
                min(winter) as start, 
                max(winter) as end 
            from WX2.GAS_HEDGE_WINTER_SIMS_HEDGE_PRICES
            where contract in (
                select contract 
                from WX2.GAS_HEDGE_CONTRACTS
                where region='"""+region+"""'
            )
            group by contract
        """
        conn = xdb.make_conn(db, stay_open=True)
        hedge_contracts=conn.query(sql_hedge_contracts)
        conn.close()
        hedge_contracts = hedge_contracts.sort_values(['start']).sort_values(['end'],ascending=False).reset_index(drop=True)
        for j in hedge_in_advance_list:
            hedge_in_advance=j
            a=0
            for m in hedge_contracts['contract']:
                df_price_final = ghf.get_gas_hist(db, m, hedge_in_advance, fc)
                df_combined = ghf.combine_price_weather(df_weather_temp, df_price_final, risk, hedge_in_advance,strip)
                if a==0:
                    df_full=df_combined.copy()
                else:
                    df_full = df_full.append(df_combined)
                a+=1
            #SELECT REQUIRED PARTS OF DF_FULL FOR FURTHER USE
            df_full_final = df_full[['winter','strip','risk_months','hedge_month','hedge_in_advance','fc','station','coldness','prev_month_coldness']].drop_duplicates()
            df_full_final = df_full_final.reset_index()
            #ADD WEIGHTED PAYOUTS
            for n in ['basis','fixed']:
                df_hedge_final = ghf.get_hedge_points(df_full,min_winter,hedge_contracts,n)
                df_full_final = pd.merge(df_full_final,df_hedge_final,how='inner',on=['winter','strip','risk_months'])
            #!!!VALIDATE DF_FULL_FINAL AND BEYOND
            hedge_size_per_month = -100
            for p in range(50):
                hedge_size_per_month+=400
                delta_hedge_multiplier = 0
                for q in range(20):
                    delta_hedge_multiplier+=1
                    for c in curve_weighting_matrix:
                        curve_weighting=c
                        df_agg = ghf.generate_summary(df_full_final,hedge_size_per_month,delta_hedge_multiplier,ind_months,curve_weighting,risk,bias,winter_exposure,lau_side)
                        df_agg = df_agg.iloc[np.where(df_agg['winter']>=min_winter)]
                        result_db = 'WX2-GC'
                        table='GAS_HEDGE_WINTER_SIMS_PAYOUTS'
                        conn = xdb.make_conn(result_db, stay_open=True)
                        conn.bulkInsertDf(df_agg, table, local=True)
                        conn.commit()
                        conn.close()