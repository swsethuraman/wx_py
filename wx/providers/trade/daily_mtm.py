import pandas as pd
import numpy as np
from xlib import xdb
import json
import os
from datetime import datetime, timedelta
from wx.providers.common import Common_Functions as CF

def mtm_csv_update(run_date, env):
    db = 'WX1-GC'
    sql = """    
        select WX_ID,
            AVG(MTM_VALUE) as MTM_VALUE,
            SUM(DELTA) as DELTA,
            SUM(GAMMA) as GAMMA,
            SUM(VEGA) as VEGA,
            SUM(POWER_DELTA) as POWER_DELTA,
            SUM(POWER_GAMMA) as POWER_GAMMA,
            SUM(POWER_VEGA) as POWER_VEGA,
            SUM(GAS_DELTA) as GAS_DELTA,
            SUM(GAS_GAMMA) as GAS_GAMMA,
            SUM(GAS_VEGA) as GAS_VEGA
        from WX2.MTM_DAILY_V
        group by WX_ID
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_wx = conn.query(sql)
    conn.close()
    filepath='/data/wx/Imagine_Position_Marks/'
    file = 'Marks.EOD.' + run_date.strftime("%A")[:3]
    df_csv = pd.read_csv(filepath+file+'.csv')
    df_trade = pd.DataFrame().reindex_like(df_csv)
    for i in df_wx.itertuples():
        df_row = df_csv.loc[np.where(df_csv['LongName'].str.contains(i.WX_ID)==True)]
        df_row['Price'] = i.MTM_VALUE
        df_row['WeatherDelta'] = i.DELTA
        df_row['WeatherGamma'] = i.GAMMA
        df_row['WeatherVega'] = i.VEGA
        df_row['PowerDelta'] = i.POWER_DELTA
        df_row['PowerGamma'] = i.POWER_GAMMA
        df_row['PowerVega'] = i.POWER_VEGA
        df_row['GasDelta'] = i.GAS_DELTA
        df_row['GasGamma'] = i.GAS_GAMMA
        df_row['GasVega'] = i.GAS_VEGA
        df_trade = df_trade.append(df_row)
    df_trade = df_trade.dropna(thresh=5)
    if env=='Prod':
        df_trade.to_csv(filepath+file+'.csv', index=False)
        print ('Updated' + file)
    else:
        df_trade.to_csv(filepath+file+'_Test.csv', index=False)
        print ('Updated' + file+'_Test.csv')

###OTC FUNCTIONS (CURRENTLY ONLY SUPPORTS VANILLA)

def load_pricing_jsons():
    db = 'WX2-GC'
    directory = '/shared/wx/Models/PROD/pricing/'
    df_main = pd.DataFrame(columns = ['filename','counterparty','risk_region','deal_number','aggregate_limit_cpty','aggregate_limit_lc','traded_date_time','create_date_time','risk_sub_region','quoted_y_n','aggregate_deductible','quoted_date_time','traded_y_n'])
    df_vi = pd.DataFrame(columns = ['filename','deal_number','name','vi_name','index_threshold','index_type','risk_start','risk_end','index_daily_max','index_daily_min','weight','index_aggregation','underlying','location','underlying_unit'])
    df_po = pd.DataFrame(columns = ['filename','deal_number','name','limit_lc','strike','limit_cpty','type','buysell','notional'])
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            try:
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
            except:
                print('Failed loading json ' + filename)
    conn = xdb.make_conn(db, stay_open=True)
    conn.deleteAllRows('Deal_Pricing_Summary')
    conn.deleteAllRows('Deal_Pricing_Legs')
    conn.deleteAllRows('Deal_Pricing_Payouts')
    conn.commit()
    conn.close()
    CF.insert_update(db,'Deal_Pricing_Summary',df_main,'N')
    CF.insert_update(db,'Deal_Pricing_Legs',df_vi,'N')
    CF.insert_update(db,'Deal_Pricing_Payouts',df_po,'N')

def get_json_data():
    sql = """
    select a.FILENAME,
        a.COUNTERPARTY,
        a.RISK_REGION,
        a.DEAL_NUMBER,
        a.AGGREGATE_LIMIT_CPTY,
        a.AGGREGATE_LIMIT_LC,
        a.RISK_SUB_REGION,
        a.AGGREGATE_DEDUCTIBLE,
        c.NAME,
        c.VI_NAME,
        c.INDEX_THRESHOLD,
        c.INDEX_TYPE,
        c.RISK_START,
        c.RISK_END,
        c.INDEX_DAILY_MAX,
        c.INDEX_DAILY_MIN,
        c.WEIGHT,
        c.INDEX_AGGREGATION,
        c.UNDERLYING,
        c.LOCATION,
        c.UNDERLYING_UNIT,
        d.LIMIT_LC,
        d.STRIKE,
        d.LIMIT_CPTY,
        d.TYPE,
        d.BUYSELL,
        d.NOTIONAL
    from WX2.Deal_Pricing_Summary a
    inner join WX2.MTM_DEAL_SUMMARY b
        on a.DEAL_NUMBER=b.WX_ID
    inner join WX2.Deal_Pricing_Legs c
        on a.FILENAME=c.FILENAME
    inner join WX2.Deal_Pricing_Payouts d
        on a.FILENAME=d.FILENAME
        and c.NAME=d.NAME
    where a.TRADED_Y_N = 'Yes'
    and c.RISK_END>=date_add(curdate(), interval 1 day)
    """
    db='WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df = conn.query(sql)
    conn.close()
    return df

def get_deal_metadata(df,deal_number):
    #SELECT JSON DATA FOR SPECIFIC DEAL AND EXTRACT INFO FROM LOCATION FIELD
    df_locs = pd.DataFrame(df.iloc[np.where(df['DEAL_NUMBER']==deal_number)].reset_index(drop=True))
    split = df_locs['LOCATION'].str.split('_',expand=True)
    df_locs['NAME'] = split[0]
    df_locs['ID_TYPE'] = split[1]
    df_locs['ID'] = split[2]
    return df_locs

#NEED TO ADD LOGIC TO CONVERT RAW TEMP TO UOM FROM JSON
#NEED TO ADD LOGIC TO CONVERT RAW TEMP TO UOM FROM JSON
def get_weather_data(row,as_of_date):
    #CREATE LIST OF RISK MONTHS
    risk_months=[]
    for m in range(12):
        mon=m+1
        if row.SEASON=='WINTER':
            if mon<=row.RISK_END.month or mon>=row.RISK_START.month:
                risk_months.append(mon)
        else:
            if mon<=row.RISK_END.month and mon>=row.RISK_START.month:
                risk_months.append(mon)
    #REMOVE LEADING 0 FROM WMO/WBAN
    if row.ID[:1]=='0':
        ID = row.ID[1:]
    elif row.ID=='71123':
        ID = '71155'
    else:
        ID = row.ID
    #print(row.ID_TYPE, ID)
    #GET ACTUAL TEMPRS
    sql_actuals = """
        select OPR_DATE,
            extract(year from OPR_DATE) as YEAR,
            extract(year from date_sub(OPR_DATE, interval """+str(row.DATE_SHIFT)+""" day)) as SEASON,
            extract(month from OPR_DATE) as MONTH,
            extract(day from OPR_DATE) as DAY,
            WMO,
            WBAN,
            ICAO,
            TMIN,
            TMAX,
            TAVG,
            greatest(0,"""+row.INDEX_THRESHOLD+"""-TAVG) as HDD,
            greatest(0,TAVG-"""+row.INDEX_THRESHOLD+""") as CDD,
            UOM
        from (
            select OPR_DATE,
                WMO,
                WBAN,
                ICAO,
                round(TMIN,0) as TMIN,
                round(TMAX,0) as TMAX,
                (round(TMIN,0) + round(TMAX,0))/2 as TAVG,
                UOM
            from WX1.WX_WEATHER_DAILY_CLEANED
            where OPR_DATE<date'""" + as_of_date + """'
            and """ + row.ID_TYPE + """ = '""" + ID + """'
        ) a
    """
    db='WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_tempr = conn.query(sql_actuals)
    conn.close()
    icao = df_tempr['ICAO'].unique()[0]
    wmo = df_tempr['WMO'].unique()[0]
    wban = df_tempr['WBAN'].unique()[0]
    #GET MOST RECENT FORECAST
    sql_fcst = """
        select OPR_DATE,
            extract(year from OPR_DATE) as YEAR,
            extract(year from date_sub(OPR_DATE, interval """+str(row.DATE_SHIFT)+""" day)) as SEASON,
            extract(month from OPR_DATE) as MONTH,
            extract(day from OPR_DATE) as DAY,
            '"""+ str(wmo) +"""' as WMO,
            '"""+ str(wban) +"""' as WBAN,
            STATION as ICAO,
            round(FCST_MN,0) as TMIN,
            round(FCST_MX,0) as TMAX,
            FCST_AVG as TAVG,
            greatest(0,"""+row.INDEX_THRESHOLD+"""-FCST_AVG) as HDD,
            greatest(0,FCST_AVG-"""+row.INDEX_THRESHOLD+""") as CDD,
            'F' as UOM
        from (
            select date_add(a.OPR_DATE, interval cast(replace(a.FORECAST_DAY,'D','') as signed) day) as OPR_DATE,
                a.STATION,
                a.FCST_MN,
                a.FCST_MX,
                a.FCST_AVG
            from WX1.MS_CWG_FCST_NA a
            inner join (
                select STATION, 
                    max(OPR_DATE) as MOST_RECENT 
                from WX1.MS_CWG_FCST_NA
                where OPR_DATE>date_sub(curdate(),interval 7 day)
                and OPR_DATE<=date'""" + as_of_date + """'
                and STATION = '"""+ str(icao) +"""'
                group by STATION
            ) b
            on a.STATION=b.STATION
            and a.OPR_DATE=b.MOST_RECENT
        ) c
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_fcst = conn.query(sql_fcst)
    conn.close()
    #EXCLUDE OVERLAPPING DAYS
    df_fcst = df_fcst.iloc[np.where(df_fcst['OPR_DATE']>df_tempr['OPR_DATE'].max())].reset_index(drop=True)
    #COMBINE ACTUALS AND FORECASTS
    df_tempr = df_tempr.append(df_fcst)
    #GET 10YR
    df_merged = pd.merge(df_tempr,df_tempr,how='inner',on=['MONTH','DAY'],suffixes=('', '_HIST'))
    df_merged = df_merged.iloc[np.where((df_merged['YEAR']>df_merged['YEAR_HIST']) & (df_merged['YEAR']<df_merged['YEAR_HIST']+11))].reset_index(drop=True)
    df_merged = df_merged[['OPR_DATE','SEASON','YEAR','MONTH','DAY','WMO','WBAN','ICAO','TMIN','TMAX','TAVG','HDD','CDD','UOM','TMIN_HIST','TMAX_HIST','TAVG_HIST','HDD_HIST','CDD_HIST']]
    df_merged = df_merged.groupby(['OPR_DATE','SEASON','YEAR','MONTH','DAY','WMO','WBAN','ICAO','TMIN','TMAX','TAVG','HDD','CDD','UOM']).mean().reset_index()
    df_merged = df_merged.iloc[np.where(df_merged['MONTH'].isin(risk_months))].reset_index(drop=True)
    return df_merged

def create_index(row,df_merged):
    #CREATE LIST OF DATES IN RISK PERIOD
    sql_dates = """
        select OPR_DATE
        from WX1.Util_DateList
        where OPR_DATE between date'"""+str(row.RISK_START.year)+"""-"""+str(row.RISK_START.month)+"""-"""+str(row.RISK_START.day)+"""' 
        and date'"""+str(row.RISK_END.year)+"""-"""+str(row.RISK_END.month)+"""-"""+str(row.RISK_END.day)+"""'
    """
    db='WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_dates = conn.query(sql_dates)
    conn.close()
    df_dates['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_dates['OPR_DATE'])]
    df_mtm = pd.merge(df_dates,df_merged,how='left',on=['OPR_DATE'])
    df_mtm = df_mtm[['OPR_DATE','SEASON','YEAR','MONTH','DAY','WMO','WBAN','ICAO','TMIN','TMAX','TAVG','HDD','CDD','UOM']]
    df_mtm['YEAR'] = pd.DatetimeIndex(df_mtm['OPR_DATE']).year
    df_mtm['MONTH'] = pd.DatetimeIndex(df_mtm['OPR_DATE']).month
    df_mtm['DAY'] = pd.DatetimeIndex(df_mtm['OPR_DATE']).day
    df_mtm = pd.merge(df_merged,df_mtm,how='left',on=['MONTH','DAY'],suffixes=('', '_OBS'))
    df_mtm['TMIN'] = np.where(df_mtm['TMIN_OBS'].isna(),df_mtm['TMIN'],df_mtm['TMIN_OBS'])
    df_mtm['TMAX'] = np.where(df_mtm['TMAX_OBS'].isna(),df_mtm['TMAX'],df_mtm['TMAX_OBS'])
    df_mtm['TAVG'] = np.where(df_mtm['TAVG_OBS'].isna(),df_mtm['TAVG'],df_mtm['TAVG_OBS'])
    df_mtm['HDD'] = np.where(df_mtm['HDD_OBS'].isna(),df_mtm['HDD'],df_mtm['HDD_OBS'])
    df_mtm['CDD'] = np.where(df_mtm['CDD_OBS'].isna(),df_mtm['CDD'],df_mtm['CDD_OBS'])
    return df_mtm


def wx_payoff(pay_type, notional, strike, limit_lc, limit_cpty, buy_sell, index):
    if pay_type == 'call':
        wx_payoff = max(index - strike, 0)
    elif pay_type == 'put':
        wx_payoff = max(strike - index, 0)
    elif pay_type == 'swap':
        wx_payoff = index - strike
    wx_payoff = notional * wx_payoff
    if buy_sell == 'sell':
        wx_payoff = -1 * wx_payoff
    if limit_lc != 'None':
        limit_lc = float(limit_lc)
        wx_payoff = max(-1 * limit_lc, wx_payoff)
    if limit_cpty != 'None':
        limit_cpty = float(limit_cpty)
        wx_payoff = max(limit_cpty, wx_payoff)
    return wx_payoff

def run_burns(row,df_mtm,bump,pay_type,notional,strike,limit_lc,limit_cpty,buy_sell,index):
    df_burns = df_mtm[['SEASON','UOM','TMIN','TMAX','TAVG','HDD','CDD']].groupby(['SEASON']).sum().reset_index()
    df_burns = df_burns.iloc[np.where((df_burns['SEASON']<row.RISK_START.year) & (df_burns['SEASON']>row.RISK_START.year-11))]
    mean = df_burns[index].mean()
    std = df_burns[index].std()
    df_burns['WX_PAYOFF'] = df_burns.apply(lambda x: wx_payoff(pay_type,notional,strike,limit_lc,limit_cpty,buy_sell,x[index]),axis=1)
    if std==0:
        plus_factor=0
        minus_factor=0
    else:
        plus_factor = (std + bump)/std
        minus_factor = (std - bump)/std
    df_burns['WX_PAYOFF_+BUMP'] = df_burns.apply(lambda x: wx_payoff(pay_type,notional,strike,limit_lc,limit_cpty,buy_sell,x[index]+bump),axis=1)
    df_burns['WX_PAYOFF_-BUMP'] = df_burns.apply(lambda x: wx_payoff(pay_type,notional,strike,limit_lc,limit_cpty,buy_sell,x[index]-bump),axis=1)
    df_burns['WX_PAYOFF_+FACTOR'] = df_burns.apply(lambda x: wx_payoff(pay_type,notional,strike,limit_lc,limit_cpty,buy_sell,mean+((x[index]-mean)*plus_factor)),axis=1)
    df_burns['WX_PAYOFF_-FACTOR'] = df_burns.apply(lambda x: wx_payoff(pay_type,notional,strike,limit_lc,limit_cpty,buy_sell,mean+((x[index]-mean)*minus_factor)),axis=1)
    return df_burns, mean, std

def wx_limits_deducs(agg_limit_lc,agg_limit_cpty,agg_deduc,buy_sell,final_fv):
    if agg_deduc!='None':
        agg_deduc=float(agg_deduc)
        if buy_sell=='buy':
            final_fv = max(0,final_fv-agg_deduc)
        if buy_sell=='sell':
            final_fv = min(0,final_fv-agg_deduc)
    if agg_limit_lc!='None':
        agg_limit_lc=float(agg_limit_lc)
        final_fv = min(final_fv,agg_limit_lc)
    if agg_limit_cpty!='None':
        agg_limit_cpty=float(agg_limit_cpty)
        final_fv = min(final_fv,agg_limit_cpty)
    return final_fv

#WRAPPER FUNCTION THAT RUNS ALL ABOVE FUNCTIONS
def run_otc_mtm(mtm_date):
    mtm_date = str(mtm_date.year)+'-'+str(mtm_date.month)+'-'+str(mtm_date.day)
    print(mtm_date)
    as_of_date = datetime.today() - timedelta(1)
    as_of_date = str(as_of_date.year)+'-'+str(as_of_date.month)+'-'+str(as_of_date.day)
    #UPDATE JSONS IN DB
    load_pricing_jsons()
    #GET JSON DATA FOR DB
    df = get_json_data()
    #FORMAT DATES AND GET ADJUSTMENTS FOR WINTER SEASON
    df['RISK_START'] = [datetime.date(d) for d in pd.to_datetime(df['RISK_START'])]
    df['RISK_END'] = [datetime.date(d) for d in pd.to_datetime(df['RISK_END'])]
    df['SEASON'] = np.where(pd.DatetimeIndex(df['RISK_START']).month > pd.DatetimeIndex(df['RISK_END']).month,'WINTER','STANDARD')
    df['DATE_SHIFT'] = np.where(pd.DatetimeIndex(df['RISK_START']).month > pd.DatetimeIndex(df['RISK_END']).month,180,0)
    #GET WEIGHTS (TOTAL NUMBER OF LEGS IN EACH DEAL)
    df['WEIGHT'] = df['WEIGHT'].astype(float)
    df_weights = df[['FILENAME','WEIGHT']].groupby('FILENAME').sum().reset_index()
    df = pd.merge(df,df_weights,how='inner',on='FILENAME',suffixes=('', '_TOTAL'))
    #GET LIST OF DEALS TO LOOP THROUGH
    deals = df['DEAL_NUMBER'].drop_duplicates()
    final_mtm = pd.DataFrame(columns=['WX_ID','MTM_DATE','FV','DELTA','GAMMA','VEGA','INDEX_VALUE','GAS_DELTA','GAS_GAMMA','GAS_VEGA','POWER_DELTA','POWER_GAMMA','POWER_VEGA','WX_CNHG','WXVOL_CHNG','PX_CHNG','PXVOL_CNHG','WX_MEAN','WX_VOL','PX_MEAN','PX_VOL'])
    #LOOP THROUGH DEALS
    for deal_number in deals:
        print(deal_number)
        df_locs = get_deal_metadata(df,deal_number)
        #LOOP THROUGH EACH LOCATION IN DEAL
        final_fv = 0
        final_fv_plus = 0
        final_fv_minus = 0
        final_fv_vegaplus = 0
        final_fv_vegaminus = 0
        final_mean = 0
        final_std = 0
        for row in df_locs.itertuples():
            df_merged = get_weather_data(row,as_of_date)
            df_mtm = create_index(row,df_merged)
            #SET VARIABLES NEEDED FOR CALCULATING FV & GREEKS
            pay_type=df_locs['TYPE'][0]
            notional=float(df_locs['NOTIONAL'][0])
            strike=float(df_locs['STRIKE'][0])
            limit_lc=df_locs['LIMIT_LC'][0]
            limit_cpty=df_locs['LIMIT_CPTY'][0]
            buy_sell=df_locs['BUYSELL'][0]
            index = df_locs['INDEX_TYPE'][0]
            weighting = df_locs['WEIGHT'][0] / df_locs['WEIGHT_TOTAL'][0]
            bump = 50
            #RUN 10YR BURNS
            df_burns, mean, std = run_burns(row,df_mtm,bump,pay_type,notional,strike,limit_lc,limit_cpty,buy_sell,index)
            #CALCULATE FV & FV COMPONENTS FOR GREEKS
            fv = df_burns['WX_PAYOFF'].mean()
            fv_plus = df_burns['WX_PAYOFF_+BUMP'].mean()
            fv_minus = df_burns['WX_PAYOFF_-BUMP'].mean()
            fv_vegaplus = df_burns['WX_PAYOFF_+FACTOR'].mean()
            fv_vegaminus = df_burns['WX_PAYOFF_-FACTOR'].mean()
            mean = mean*weighting
            std = std*weighting
            fv = fv*weighting
            fv_plus = fv_plus*weighting
            fv_minus = fv_minus*weighting
            fv_vegaplus = fv_vegaplus*weighting
            fv_vegaminus = fv_vegaminus*weighting
            final_mean+=mean
            final_std+=std
            final_fv+=fv
            final_fv_plus+=fv_plus
            final_fv_minus+=fv_minus
            final_fv_vegaplus+=fv_vegaplus
            final_fv_vegaminus+=fv_vegaminus
        #APPLY DEAL LEVELS LIMIT AND DEDUC
        agg_limit_lc = df_locs['AGGREGATE_LIMIT_LC'][0]
        agg_limit_cpty = df_locs['AGGREGATE_LIMIT_CPTY'][0]
        agg_deduc = df_locs['AGGREGATE_DEDUCTIBLE'][0]
        #NEED TO MAKE SURE THIS WORKS FOR BUYS AND SELLS
        final_fv = wx_limits_deducs(agg_limit_lc,agg_limit_cpty,agg_deduc,buy_sell,final_fv)
        final_fv_plus = wx_limits_deducs(agg_limit_lc,agg_limit_cpty,agg_deduc,buy_sell,final_fv_plus)
        final_fv_minus = wx_limits_deducs(agg_limit_lc,agg_limit_cpty,agg_deduc,buy_sell,final_fv_minus)
        final_fv_vegaplus = wx_limits_deducs(agg_limit_lc,agg_limit_cpty,agg_deduc,buy_sell,final_fv_vegaplus)
        final_fv_vegaminus = wx_limits_deducs(agg_limit_lc,agg_limit_cpty,agg_deduc,buy_sell,final_fv_vegaminus)
        delta = (final_fv_plus - final_fv_minus)/(2*bump)
        gamma = (final_fv_plus + final_fv_minus - 2*final_fv)/(bump**2)
        vega = (final_fv_vegaplus - final_fv_vegaminus)/(2*bump)
        final_fv=round(final_fv,2)
        delta=round(delta,2)
        gamma=round(gamma,2)
        vega=round(vega,2)
        new_row = {'WX_ID': deal_number, 'MTM_DATE': mtm_date, 'FV': final_fv, 'DELTA': delta, 'GAMMA': gamma,
                   'VEGA': vega, 'INDEX_VALUE': np.nan, 'GAS_DELTA': np.nan, 'GAS_GAMMA': np.nan, 'GAS_VEGA': np.nan,
                   'POWER_DELTA': np.nan, 'POWER_GAMMA': np.nan, 'POWER_VEGA': np.nan, 'WX_CNHG': np.nan,
                   'WXVOL_CHNG': np.nan, 'PX_CHNG': np.nan, 'PXVOL_CNHG': np.nan, 'WX_MEAN': final_mean,
                   'WX_VOL': final_std, 'PX_MEAN': np.nan, 'PX_VOL': np.nan}
        final_mtm = final_mtm.append(new_row, ignore_index=True)
    return final_mtm

###END OF OTC FUNCTIONS