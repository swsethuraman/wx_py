import requests
import pandas as pd
import numpy as np
from xlib import xdb
from datetime import date, datetime, timedelta
import time
import io
from requests.auth import HTTPBasicAuth
import json
import pytz
from wx.providers.common import Common_Functions as CF


def ICE_Options_Loader(start_date, end_date, ICE_Data):
    # Define DB Parameters
    db = 'WX1-GC'
    table = 'ICE_' + ICE_Data + '_Loader'
    # Define ICE credentials
    user = 'laurioncap_API'
    pwd = 'WxLaurion2020'
    key = 'ICEDOWNLOADS'
    # Get ICE cookies info
    r1 = requests.post('https://sso.theice.com/api/authenticateTfa')
    response = requests.post('https://sso.theice.com/api/authenticateTfa', cookies=r1.cookies,
                             data={'userId': user, 'password': pwd, 'appKey': key})
    # Generate range of dates to be included in run
    date_list = []
    startDate = start_date
    endDate = end_date
    for i in range((endDate - startDate).days + 1):
        date_list.append(startDate + timedelta(days=i))
    # Get data for each date from
    for i in date_list:
        date_str = str(i).replace('-', '_')
        if ICE_Data == 'Gas_Options':
            url = 'https://downloads2.theice.com/Settlement_Reports_CSV/Gas/icecleared_' + ICE_Data.lower() + '_' + date_str + '.dat'
        if ICE_Data == 'Power_Options':
            url = 'https://downloads2.theice.com/Settlement_Reports_CSV/Power/icecleared_' + ICE_Data.lower().replace(
                '_', '') + '_' + date_str + '.dat'
        try:
            url_data = requests.get(url, cookies=response.cookies, auth=HTTPBasicAuth(user, pwd)).content
            data = io.StringIO(url_data.decode("utf-8"))
            df_api = pd.read_csv(data, sep='|')
            df_api.rename(columns={'TRADE DATE': 'TRADE_DATE', 'CONTRACT TYPE': 'CONTRACT_TYPE',
                                   'SETTLEMENT PRICE': 'SETTLEMENT_PRICE', 'NET CHANGE': 'NET_CHANGE',
                                   'EXPIRATION DATE': 'EXPIRATION_DATE', 'PRODUCT ID': 'PRODUCT_ID'}, inplace=True)
            # df_api = df_api.loc[np.where(df_api['CONTRACT']=='TFM')]
            if len(df_api) > 1000:
                CF.insert_update(db, table, df_api)
                print('Inserted ' + date_str)
        except:
            print('No file for ' + date_str)
    db = 'WX1-GC'
    sql1 = """
            select distinct str_to_date(a.TRADE_DATE, '%c/%e/%Y') as TRADE_DATE, 
                    a.HUB, 
                     a.PRODUCT, 
                     str_to_date(a.STRIP, '%c/%e/%Y') as STRIP, 
                     a.CONTRACT,
                     a.CONTRACT_TYPE,
                     a.STRIKE,
                     a.SETTLEMENT_PRICE,
                     a.NET_CHANGE,
                     str_to_date(a.EXPIRATION_DATE, '%c/%e/%Y') as EXPIRATION_DATE, 
                     a.PRODUCT_ID,
                     a.OPTION_VOLATILITY,
                     a.DELTA_FACTOR,
                     CURRENT_TIMESTAMP() as DateUpdated,
                     CURRENT_USER() as UserUpdated
                  from WX1.ICE_""" + ICE_Data + """_Loader a
                  where strike is not null
                    and contract='PDA'
            """
    conn = xdb.make_conn(db, stay_open=True)
    df = conn.query(sql1)
    conn.close()
    CF.insert_update(db, 'ICE_' + ICE_Data + '_Settlement', df)
    print('Inserted to ICE_' + ICE_Data + '_Settlement')

def load_exchange_marks():
    db = 'WX2-GC'
    sql="""
        select WX_ID, 
            MTM_DATE, 
            INDEX_VALUE, 
            DELTA, 
            GAMMA, 
            VEGA,
            null as GAS_DELTA,
            null as GAS_GAMMA,
            null as GAS_VEGA,
            null as POWER_DELTA,
            null as POWER_GAMMA,
            null as POWER_VEGA
        from WX2.MTM_CME_DAILY_V

        union all

        select WX_ID,
            MTM_DATE,
            INDEX_VALUE,
            null as DELTA,
            null as GAMMA,
            null as VEGA,
            null as GAS_DELTA,
            null as GAS_GAMMA,
            null as GAS_VEGA,
            POWER_DELTA,
            POWER_GAMMA,
            POWER_VEGA
        from WX2.MTM_ICE_DAILY_V
    """
    conn = xdb.make_conn(db, stay_open=True)
    mtm = conn.query(sql)
    conn.close()
    mtm['FV'] = np.nan
    mtm['WX_CNHG'] = np.nan
    mtm['WXVOL_CHNG'] = np.nan
    mtm['PX_CHNG'] = np.nan
    mtm['PXVOL_CNHG'] = np.nan
    mtm['WX_MEAN'] = np.nan
    mtm['WX_VOL'] = np.nan
    mtm['PX_MEAN'] = np.nan
    mtm['PX_VOL'] = np.nan
    mtm = mtm[['WX_ID','MTM_DATE','FV','DELTA','GAMMA','VEGA','INDEX_VALUE','GAS_DELTA','GAS_GAMMA','GAS_VEGA','POWER_DELTA','POWER_GAMMA','POWER_VEGA','WX_CNHG','WXVOL_CHNG','PX_CHNG','PXVOL_CNHG','WX_MEAN','WX_VOL','PX_MEAN','PX_VOL']]
    table = 'MTM_DAILY'
    CF.insert_update(db,table,mtm,'N')
    print('Succesfully loaded ICE and CME marks to MTM_DAILY')