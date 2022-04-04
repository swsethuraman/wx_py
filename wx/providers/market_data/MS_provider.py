import pandas as pd
import numpy as np
import json
import requests
from requests.auth import HTTPBasicAuth
import io
from datetime import date, timedelta, datetime
import dateutil.relativedelta
import time
import pytz
from xlib import xdb
from wx.providers.common import Common_Functions as CF

def get_publisher_list(list_name, start_date, end_date, trader_only='N'):
    if trader_only == 'Y':
        user='swami.sethuraman@laurioncap.com'
        pwd='L0ndon2302'
    else:
        user='riley.day@laurioncap.com'
        pwd='ForSharingMS1'
    r1 = requests.post('https://mp.morningstarcommodity.com/lds/lists/'+list_name+'/tickets?fromDateTime='+start_date+'&toDateTime='+end_date+'&history=true', auth=HTTPBasicAuth(user, pwd))
    ticketId = json.loads(r1.content)['ticketId']
    for i in range(30):
        r2 = requests.get('https://mp.morningstarcommodity.com/lds/lists/tickets/' + ticketId, auth=HTTPBasicAuth(user, pwd))
        status = json.loads(r2.content)['status']
        if status == 'DONE':
            try:
                r3 = requests.get('http://mp.morningstarcommodity.com/lds/lists/'+list_name+'/content?ticketId='+ticketId, auth=HTTPBasicAuth(user, pwd))
                break
            except:
                print('Failed to get data for ' + list_name)
                break
        else:
            time.sleep(10)
            print('timed out')
            continue
    data = io.StringIO(r3.content.decode("utf-8"))
    df_api = pd.read_csv(data,sep=',')
    return df_api

def get_list_of_publisher_lists(python_job):
    sql_lists = "select distinct publisher_list from WX1.Util_MSPublisherLists where python_job='" + python_job + "'"
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    lists = conn.query(sql_lists)
    conn.close()
    list_name = lists['publisher_list'].to_list()
    return list_name

def pjm_da_lmp(start_date, end_date):
    list_name = get_list_of_publisher_lists('pjm_da_lmp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'TotalLMP')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'LMP','Keys':'PNODE_ID','Name':'LOCATION','Type':'LOCATION_TYPE'},inplace=True)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PNODE_ID','LOCATION','LOCATION_TYPE','LMP']]
        db = 'WX1-GC'
        table='MS_PJM_DA_LMP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def pjm_rt_lmp(start_date, end_date):
    list_name = get_list_of_publisher_lists('pjm_rt_lmp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Interval')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'OPR_HOUR','Keys':'PNODE_ID','Location':'LOCATION'},inplace=True)
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'TotalLMP')], how='left', left_on=['OPR_DATE','PNODE_ID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LMP'},inplace=True)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PNODE_ID','LOCATION','LMP']]
        db = 'WX1-GC'
        table='MS_PJM_RT_LMP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def pjm_rt_lmp_prelim(start_date, end_date):
    list_name = get_list_of_publisher_lists('pjm_rt_lmp_prelim')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Lmp')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'LMP','Keys':'PNODE_ID','Location':'LOCATION'},inplace=True)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PNODE_ID','LOCATION','LMP']]
        db = 'WX1-GC'
        table='MS_PJM_RT_LMP_PRELIM'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def isone_da_lmp(start_date, end_date):
    list_name = get_list_of_publisher_lists('isone_da_lmp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Location')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'LOCATION','Keys':'LOCATION_ID'},inplace=True)
        df_ins = df_ins[['OPR_DATE','LOCATION_ID','LOCATION']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LmpTotal')], how='left', left_on=['OPR_DATE','LOCATION_ID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','LOCATION_ID','LOCATION','LMP']]
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LMP']]
        db = 'WX1-GC'
        table='MS_ISONE_DA_LMP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def isone_rt_lmp(start_date, end_date):
    list_name = get_list_of_publisher_lists('isone_rt_lmp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Published_Interval')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'OPR_HOUR','Keys':'LOCATION_ID'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Location_Name')], how='left', left_on=['OPR_DATE','LOCATION_ID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LOCATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Locational_Marginal_Price')], how='left', left_on=['OPR_DATE','LOCATION_ID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LMP']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LMP']]
        db = 'WX1-GC'
        table='MS_ISONE_RT_LMP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def isone_rt_lmp_prelim(start_date, end_date):
    list_name = get_list_of_publisher_lists('isone_rt_lmp_prelim')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Published_Interval')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'OPR_HOUR','Keys':'LOCATION_ID'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Location_Name')], how='left', left_on=['OPR_DATE','LOCATION_ID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LOCATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Location_Type')], how='left', left_on=['OPR_DATE','LOCATION_ID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LOCATION_TYPE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LOCATION_TYPE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Real_Time_Locational_Marginal_Price')], how='left', left_on=['OPR_DATE','LOCATION_ID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LOCATION_TYPE','LMP']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LOCATION_TYPE','LMP']]
        db = 'WX1-GC'
        table='MS_ISONE_RT_LMP_PRELIM'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def ercot_dam_spp(start_date, end_date):
    list_name = get_list_of_publisher_lists('ercot_dam_spp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'SettlementPointPrice')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'SPP','Keys':'SETTLEMENT_POINT'},inplace=True)
        df_ins = df_ins[['OPR_DATE','SETTLEMENT_POINT','SPP']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'DSTFlag')], how='left', left_on=['OPR_DATE','SETTLEMENT_POINT'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'DST_FLAG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','SETTLEMENT_POINT','SPP','DST_FLAG']]
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','SETTLEMENT_POINT','SPP','DST_FLAG']]
        db = 'WX1-GC'
        table='MS_ERCOT_DAM_SPP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def ercot_rt_spp(start_date, end_date):
    list_name = get_list_of_publisher_lists('ercot_rt_spp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'DeliveryHour')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'OPR_HOUR'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'DeliveryInterval')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'INTERVAL_15MIN'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','INTERVAL_15MIN','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'SettlementPointPrice')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'SPP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','INTERVAL_15MIN','SPP','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'DSTFlag')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'DST_FLAG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','INTERVAL_15MIN','SPP','DST_FLAG','Keys']]
        df_ins = pd.concat([df_ins[['OPR_DATE','OPR_HOUR','INTERVAL_15MIN','SPP','DST_FLAG']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins.rename(columns={0:'SETTLEMENT_POINT',1:'SETTLEMENT_POINT_TYPE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','INTERVAL_15MIN','SETTLEMENT_POINT','SETTLEMENT_POINT_TYPE','SPP','DST_FLAG']]
        db = 'WX1-GC'
        table='MS_ERCOT_RT_SPP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def nyiso_da_lbmp(start_date, end_date):
    list_name = get_list_of_publisher_lists('nyiso_da_lbmp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Name')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'LOCATION','Keys':'PTID'},inplace=True)
        df_ins = df_ins[['OPR_DATE','PTID','LOCATION']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LBMP_MWHr')], how='left', left_on=['OPR_DATE','PTID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LBMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','PTID','LOCATION','LBMP']]
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PTID','LOCATION','LBMP']]
        db = 'WX1-GC'
        table='MS_NYISO_DA_LBMP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def nyiso_rt_lbmp(start_date, end_date):
    list_name = get_list_of_publisher_lists('nyiso_rt_lbmp')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Name')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'LOCATION','Keys':'PTID'},inplace=True)
        df_ins = df_ins[['OPR_DATE','PTID','LOCATION']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LBMP_MWHr')], how='left', left_on=['OPR_DATE','PTID'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LBMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','PTID','LOCATION','LBMP']]
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PTID','LOCATION','LBMP']]
        db = 'WX1-GC'
        table='MS_NYISO_RT_LBMP'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def platts_gd(start_date, end_date):
    list_name = get_list_of_publisher_lists('platts_gd')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date, 'Y')
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'High')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'HIGH','Keys':'CODE','Description':'DESCRIPTION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','CODE','DESCRIPTION','HIGH']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Low')], how='left', left_on=['OPR_DATE','CODE'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'LOW'},inplace=True)
        df_ins = df_ins[['OPR_DATE','CODE','DESCRIPTION','HIGH','LOW']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Close')], how='left', left_on=['OPR_DATE','CODE'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'CLOSE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','CODE','DESCRIPTION','HIGH','LOW','CLOSE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Index')], how='left', left_on=['OPR_DATE','CODE'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'INDEX_VALUE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','CODE','DESCRIPTION','HIGH','LOW','CLOSE','INDEX_VALUE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Volume')], how='left', left_on=['OPR_DATE','CODE'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'VOLUME'},inplace=True)
        df_ins = df_ins[['OPR_DATE','CODE','DESCRIPTION','HIGH','LOW','CLOSE','INDEX_VALUE','VOLUME']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        db = 'WX1-GC'
        table='MS_PLATTS_GD'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def cme(start_date, end_date):
    list_name = get_list_of_publisher_lists('cme')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Settlement_Price')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'SETTLEMENT_PRICE','Keys':'CODE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','CODE','SETTLEMENT_PRICE']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        db = 'WX1-GC'
        table='MS_CME_FUTURES'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def ice_cleared_gas(start_date, end_date):
    list_name = get_list_of_publisher_lists('ice_cleared_gas')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Expiration_Date')]
        df_ins = df_ins.reset_index(drop=True)
        df_ins.rename(columns={'PUBDATE':'OPR_DATE','PUBVAL':'EXPIRATION_DATE'},inplace=True)
        df_ins['DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['DATE2'] = [datetime.date(d) for d in pd.to_datetime(df_ins['EXPIRATION_DATE'])]
        df_ins = df_ins.loc[np.where((df_ins.DATE2 >= df_ins.DATE) & (df_ins.EXPIRATION_DATE != 'nan'))]
        df_ins = df_ins[['OPR_DATE','Keys','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Hub')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'HUB'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Product')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'PRODUCT'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Contract_Type')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'CONTRACT_TYPE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Settlement_Price')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'SETTLEMENT_PRICE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Net_Change')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'NET_CHANGE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Product_Id')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'PRODUCT_ID'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE','PRODUCT_ID']]
        df_ins = pd.concat([df_ins[['OPR_DATE','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE','PRODUCT_ID']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0:'CONTRACT',1:'STRIP'},inplace=True)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['STRIP'] = [datetime.date(d) for d in pd.to_datetime(df_ins['STRIP'])]
        df_ins['EXPIRATION_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['EXPIRATION_DATE'])]
        df_ins = df_ins[['OPR_DATE','CONTRACT','HUB','PRODUCT','STRIP','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE','PRODUCT_ID']]
        db = 'WX1-GC'
        table='MS_ICE_CLEARED_GAS'
        CF.insert_update(db,table,df_ins)
        print('Done '+ list_name[i] + ' ' +str(start_date))

def ice_cleared_power(start_date, end_date):
    list_name = get_list_of_publisher_lists('ice_cleared_power')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Expiration_Date')]
        df_ins = df_ins.reset_index(drop=True)
        df_ins.rename(columns={'PUBDATE':'OPR_DATE','PUBVAL':'EXPIRATION_DATE'},inplace=True)
        df_ins['DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['DATE2'] = [datetime.date(d) for d in pd.to_datetime(df_ins['EXPIRATION_DATE'])]
        df_ins = df_ins.loc[np.where((df_ins.DATE2 >= df_ins.DATE) & (df_ins.EXPIRATION_DATE != 'nan'))]
        df_ins = df_ins[['OPR_DATE','Keys','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Hub')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'HUB'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Product')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'PRODUCT'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Contract_Type')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'CONTRACT_TYPE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Settlement_Price')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'SETTLEMENT_PRICE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Net_Change')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'NET_CHANGE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Product_Id')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'PRODUCT_ID'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE','PRODUCT_ID']]
        df_ins = pd.concat([df_ins[['OPR_DATE','HUB','PRODUCT','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE','PRODUCT_ID']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0:'CONTRACT',1:'STRIP'},inplace=True)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['STRIP'] = [datetime.date(d) for d in pd.to_datetime(df_ins['STRIP'])]
        df_ins['EXPIRATION_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['EXPIRATION_DATE'])]
        df_ins = df_ins[['OPR_DATE','CONTRACT','HUB','PRODUCT','STRIP','CONTRACT_TYPE','EXPIRATION_DATE','SETTLEMENT_PRICE','NET_CHANGE','PRODUCT_ID']]
        db = 'WX1-GC'
        table='MS_ICE_CLEARED_POWER'
        CF.insert_update(db,table,df_ins)
        print('Done '+ list_name[i] + ' ' +str(start_date))

def ice_cleared_power_options(start_date, end_date):
    list_name = get_list_of_publisher_lists('ice_cleared_power_options')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Expiration_Date')]
        df_ins = df_ins.reset_index(drop=True)
        df_ins.rename(columns={'PUBDATE':'OPR_DATE','PUBVAL':'EXPIRATION_DATE'},inplace=True)
        df_ins['DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['DATE2'] = [datetime.date(d) for d in pd.to_datetime(df_ins['EXPIRATION_DATE'])]
        df_ins = df_ins.loc[np.where((df_ins.DATE2>=df_ins.DATE) & (df_ins.EXPIRATION_DATE!='nan'))]
        df_ins = df_ins[['OPR_DATE','Keys','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Hub')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'HUB'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Product')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'PRODUCT'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','EXPIRATION_DATE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Settlement_Price')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'SETTLEMENT_PRICE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','EXPIRATION_DATE','SETTLEMENT_PRICE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Option_Volatility')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'OPTION_VOLATILITY'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','EXPIRATION_DATE','SETTLEMENT_PRICE','OPTION_VOLATILITY']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Delta_Factor')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'DELTA_FACTOR'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','HUB','PRODUCT','EXPIRATION_DATE','SETTLEMENT_PRICE','OPTION_VOLATILITY','DELTA_FACTOR']]
        df_ins = pd.concat([df_ins[['OPR_DATE','HUB','PRODUCT','EXPIRATION_DATE','SETTLEMENT_PRICE','OPTION_VOLATILITY','DELTA_FACTOR']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0:'CONTRACT',1:'STRIP',2:'OPTION_TYPE',3:'STRIKE'},inplace=True)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['STRIP'] = [datetime.date(d) for d in pd.to_datetime(df_ins['STRIP'])]
        df_ins['EXPIRATION_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['EXPIRATION_DATE'])]
        df_ins = df_ins[['OPR_DATE','CONTRACT','HUB','PRODUCT','OPTION_TYPE','STRIP','STRIKE','EXPIRATION_DATE','SETTLEMENT_PRICE','OPTION_VOLATILITY','DELTA_FACTOR']]
        db = 'WX1-GC'
        table='MS_ICE_CLEARED_POWER_OPT'
        CF.insert_update(db,table,df_ins)
        print('Done '+ list_name[i] + ' ' +str(start_date))

def ercot_wind_act(start_date, end_date):
    list_name = get_list_of_publisher_lists('ercot_wind_act')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Value')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'PRODUCTION','Keys':'REGION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','REGION','PRODUCTION']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'DSTFlag')], how='left', left_on=['OPR_DATE','REGION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'DST_FLAG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','REGION','PRODUCTION','DST_FLAG']]
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','REGION','PRODUCTION','DST_FLAG']]
        db = 'WX1-GC'
        table='MS_ERCOT_WIND_ACT'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def ercot_wind_fcst(start_date, end_date):
    list_name = get_list_of_publisher_lists('ercot_wind_fcst')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Published_Date')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'PUBLISHED_DATE'},inplace=True)
        df_ins = df_ins[['OPR_DATE','PUBLISHED_DATE','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Value')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'PRODUCTION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','PUBLISHED_DATE','PRODUCTION','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'DSTFlag')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'DST_FLAG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','PUBLISHED_DATE','PRODUCTION','DST_FLAG','Keys']]
        df_ins = pd.concat([df_ins[['OPR_DATE','PUBLISHED_DATE','PRODUCTION','DST_FLAG']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['PUBLISHED_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['PUBLISHED_DATE'])]
        df_ins.rename(columns={0:'REGION',1:'DAYS_AHEAD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','REGION','DAYS_AHEAD','PUBLISHED_DATE','PRODUCTION','DST_FLAG']]
        db = 'WX1-GC'
        table='MS_ERCOT_WIND_FCST'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def ercot_st_sys_adeq(start_date, end_date):
    list_name = get_list_of_publisher_lists('ercot_st_sys_adeq')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'TotalCapGenRes')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'TOTAL_CAP_GEN_RES'},inplace=True)
        df_ins = df_ins[['OPR_DATE','TOTAL_CAP_GEN_RES','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'TotalCapLoadRes')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TOTAL_CAP_LOAD_RES'},inplace=True)
        df_ins = df_ins[['OPR_DATE','TOTAL_CAP_GEN_RES','TOTAL_CAP_LOAD_RES','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'OfflineAvailableMW')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'OFFLINE_AVAILABLE_MW'},inplace=True)
        df_ins = df_ins[['OPR_DATE','TOTAL_CAP_GEN_RES','TOTAL_CAP_LOAD_RES','OFFLINE_AVAILABLE_MW','Keys']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'DSTFlag')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'DST_FLAG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','TOTAL_CAP_GEN_RES','TOTAL_CAP_LOAD_RES','OFFLINE_AVAILABLE_MW','DST_FLAG','Keys']]
        df_ins = pd.concat([df_ins[['OPR_DATE','TOTAL_CAP_GEN_RES','TOTAL_CAP_LOAD_RES','OFFLINE_AVAILABLE_MW','DST_FLAG']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0:'PUBLISHED_DATE',1:'PUBLISHED_HOUR'},inplace=True)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['PUBLISHED_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['PUBLISHED_DATE'])]
        df_ins['PUBLISHED_HOUR'] = df_ins['PUBLISHED_HOUR'].astype(int) + 1
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PUBLISHED_DATE','PUBLISHED_HOUR','TOTAL_CAP_GEN_RES','TOTAL_CAP_LOAD_RES','OFFLINE_AVAILABLE_MW','DST_FLAG']]
        db = 'WX1-GC'
        table='MS_ERCOT_ST_SYS_ADEQ'
        CF.insert_update(db,table,df_ins)
        print('Done '+str(start_date))

def noaa_madis_hourly(start_date, end_date):
    list_name = get_list_of_publisher_lists('noaa_madis_hourly')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'AirTemperature_kelvin')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'TEMP_K', 'Keys': 'STATION'}, inplace=True)
        df_ins['TIME_ZONE'] = 'US/CENTRAL'
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','STATION','TEMP_K','TIME_ZONE']]
        db = 'WX1-GC'
        table='MS_NOAA_MADIS_HOURLY'
        CF.insert_update(db,table,df_ins)
        print('Done '+ list_name[i] + ' ' + str(start_date))

def noaa_ghcnd(start_date, end_date):
    list_name = get_list_of_publisher_lists('noaa_ghcnd')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'TMIN_10th_of_Degree_C')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'TMIN','Keys':'STATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'TMAX_10th_of_Degree_C')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TMAX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'TAVG_10th_of_Degree_C')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TAVG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','TAVG']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','TAVG']]
        db = 'WX1-GC'
        table='MS_NOAA_GHCND'
        CF.insert_update(db,table,df_ins)
        print('Done '+ list_name[i] + ' ' + str(start_date))

def cwg_fcst_na(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_fcst_na')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Mn')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'FCST_MN'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','FCST_MN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Mx')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'FCST_MX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','FCST_MN','FCST_MX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Avg')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'FCST_AVG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','FCST_MN','FCST_MX','FCST_AVG']]
        df_ins = pd.concat([df_ins[['OPR_DATE','FCST_MN','FCST_MX','FCST_AVG']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0:'STATION',1:'FORECAST_DAY'},inplace=True)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','FORECAST_DAY','STATION','FCST_MN','FCST_MX','FCST_AVG']]
        db = 'WX1-GC'
        table='MS_CWG_FCST_NA'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))

def cwg_fcst_eu(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_fcst_eu')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Mn')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'FCST_MN'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'FCST_MN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Mx')], how='left', left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FCST_MX'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'FCST_MN', 'FCST_MX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Avg')], how='left', left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FCST_AVG'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'FCST_MN', 'FCST_MX', 'FCST_AVG']]
        df_ins = pd.concat([df_ins[['OPR_DATE', 'FCST_MN', 'FCST_MX', 'FCST_AVG']], df_ins['Keys'].str.split(',', expand=True)],axis=1)
        df_ins.rename(columns={0: 'STATION', 1: 'FORECAST_DAY'}, inplace=True)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE', 'FORECAST_DAY', 'STATION', 'FCST_MN', 'FCST_MX', 'FCST_AVG']]
        db = 'WX1-GC'
        table = 'MS_CWG_FCST_EU'
        CF.insert_update(db, table, df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def cwg_fcst_ap(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_fcst_ap')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Mn')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'FCST_MN'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','FCST_MN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Mx')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'FCST_MX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','FCST_MN','FCST_MX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Fcst_Avg')], how='left', left_on=['OPR_DATE','Keys'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'FCST_AVG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','Keys','FCST_MN','FCST_MX','FCST_AVG']]
        df_ins = pd.concat([df_ins[['OPR_DATE','FCST_MN','FCST_MX','FCST_AVG']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0:'STATION',1:'FORECAST_DAY'},inplace=True)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','FORECAST_DAY','STATION','FCST_MN','FCST_MX','FCST_AVG']]
        db = 'WX1-GC'
        table='MS_CWG_FCST_AP'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))

def cwg_obs_na_f(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_obs_na_f')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'MinTemp')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'TMIN','Keys':'STATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'MaxTemp')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TMAX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'HDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'HDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'CDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'CDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD','CDD']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD','CDD']]
        db = 'WX1-GC'
        table='MS_CWG_OBS_NA_F'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))

def cwg_obs_na_c(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_obs_na_c')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'MinTemp')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'TMIN','Keys':'STATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'MaxTemp')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TMAX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'AvgTemp')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TAVG'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','TAVG']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'HDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'HDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','TAVG','HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'CDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'CDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','TAVG','HDD','CDD']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','TAVG','HDD','CDD']]
        db = 'WX1-GC'
        table='MS_CWG_OBS_NA_C'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))

def cwg_obs_eu(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_obs_eu')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'MinTemp')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'TMIN','Keys':'STATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'MaxTemp')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TMAX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'HDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'HDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'CDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'CDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD','CDD']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD','CDD']]
        db = 'WX1-GC'
        table='MS_CWG_OBS_EU'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))

def cwg_obs_ap(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_obs_ap')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'MinTemp')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'TMIN','Keys':'STATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'MaxTemp')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'TMAX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'HDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'HDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'CDD')], how='left', left_on=['OPR_DATE','STATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'CDD'},inplace=True)
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD','CDD']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','STATION','TMIN','TMAX','HDD','CDD']]
        db = 'WX1-GC'
        table='MS_CWG_OBS_AP'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))


def cwg_wdd_national(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_wdd_national')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'NG_HDD')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_CDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)

        df_ins = pd.concat([df_ins[['OPR_DATE', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD',
                                    '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD',
                                    '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD', 'FORECAST_DATE']],
                            df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0: 'REGION_NAME', 1: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'FORECAST_DAY', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD',
             'POP_CDD', '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD',
             'LAST_Y_ELEC_CDD', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_WDD_NATIONAL'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))


def cwg_wdd_iso(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_wdd_iso')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'POP_HDD')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_CDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)

        df_ins = pd.concat([df_ins[['OPR_DATE', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD',
                                    '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'FORECAST_DATE']],
                            df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0: 'REGION_NAME', 1: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'FORECAST_DAY', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD',
             'POP_CDD', '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_WDD_ISO'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))


def cwg_wdd_state(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_wdd_state')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'POP_HDD')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_CDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'GAS_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'GAS_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'GAS_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELCT_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELCT_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT']]

        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)

        df_ins = pd.concat([df_ins[['OPR_DATE', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD', 'LAST_Y_POP_HDD', 'POP_CDD',
                                    '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT',
                                    'POP_WEIGHT', 'FORECAST_DATE']], df_ins['Keys'].str.split(',', expand=True)],
                           axis=1)
        df_ins.rename(columns={0: 'REGION_NAME', 1: 'STATE_NAME', 2: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'STATE_NAME', 'FORECAST_DAY', 'POP_HDD', '30Y_POP_HDD', '10Y_POP_HDD',
             'LAST_Y_POP_HDD', 'POP_CDD', '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT',
             'POP_WEIGHT', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_WDD_STATE'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def cwg_wdd_9region(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_wdd_9region')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'NG_HDD')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_CDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD']]

        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'GAS_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'GAS_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELCT_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELCT_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT', 'ELCT_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT']]

        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)

        df_ins = pd.concat([df_ins[['OPR_DATE', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD',
                                    '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD',
                                    '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT',
                                    'FORECAST_DATE']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0: 'REGION_NAME', 1: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'FORECAST_DAY', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD',
             'POP_CDD', '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD',
             'LAST_Y_ELEC_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_WDD_9REGION'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def cwg_wdd_5region(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_wdd_5region')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'NG_HDD')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_CDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD']]

        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'GAS_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'GAS_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELCT_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELCT_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT', 'ELCT_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT']]

        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)

        df_ins = pd.concat([df_ins[['OPR_DATE', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD',
                                    '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD',
                                    '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT',
                                    'FORECAST_DATE']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0: 'REGION_NAME', 1: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'FORECAST_DAY', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD',
             'POP_CDD', '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD',
             'LAST_Y_ELEC_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_WDD_5REGION'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def cwg_wdd_3region(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_wdd_3region')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'NG_HDD')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_NG_HDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_NG_HDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_CDD'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_POP_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_POP_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_Y_ELEC_CDD')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_Y_ELEC_CDD'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD']]

        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'GAS_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'GAS_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'ELCT_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'ELCT_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT', 'ELCT_WEIGHT']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'POP_WEIGHT')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'POP_WEIGHT'}, inplace=True)
        df_ins = df_ins[
            ['OPR_DATE', 'Keys', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD', '30Y_POP_CDD',
             '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD',
             'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT']]

        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)

        df_ins = pd.concat([df_ins[['OPR_DATE', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD', 'POP_CDD',
                                    '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD',
                                    '10Y_ELEC_CDD', 'LAST_Y_ELEC_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT',
                                    'FORECAST_DATE']], df_ins['Keys'].str.split(',', expand=True)], axis=1)
        df_ins.rename(columns={0: 'REGION_NAME', 1: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'FORECAST_DAY', 'NG_HDD', '30Y_NG_HDD', '10Y_NG_HDD', 'LAST_Y_NG_HDD',
             'POP_CDD', '30Y_POP_CDD', '10Y_POP_CDD', 'LAST_Y_POP_CDD', 'ELEC_CDD', '30Y_ELEC_CDD', '10Y_ELEC_CDD',
             'LAST_Y_ELEC_CDD', 'GAS_WEIGHT', 'ELCT_WEIGHT', 'POP_WEIGHT', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_WDD_3REGION'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))


def cwg_elec_cdd_daily(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_elec_cdd_daily')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'DAY_OF_WEEK')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'DAY_OF_WEEK'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'CHANGE')], how='left', left_on=['OPR_DATE', 'Keys'],
                              right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_CHANGE'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_YEAR')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_YEAR'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y')], how='left', left_on=['OPR_DATE', 'Keys'],
                              right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR', '30Y']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y')], how='left', left_on=['OPR_DATE', 'Keys'],
                              right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR', '30Y', '10Y']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)
        df_ins = pd.concat([df_ins[['OPR_DATE', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR', '30Y', '10Y',
                                    'FORECAST_DATE']], df_ins['Keys'].str.split(',', expand=True)], axis=1)

        df_ins.rename(columns={0: 'REGION_NAME', 1: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'FORECAST_DAY', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR',
             '30Y', '10Y', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_ELEC_CDD_BAL_DAILY'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))


def cwg_elec_cdd_daily_next(start_date, end_date):
    list_name = get_list_of_publisher_lists('cwg_elec_cdd_daily_next')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'DAY_OF_WEEK')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE', 'PUBVAL': 'DAY_OF_WEEK'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'CHANGE')], how='left', left_on=['OPR_DATE', 'Keys'],
                              right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_CHANGE'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'LAST_YEAR')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'LAST_YEAR'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '30Y')], how='left', left_on=['OPR_DATE', 'Keys'],
                              right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '30Y'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR', '30Y']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == '10Y')], how='left', left_on=['OPR_DATE', 'Keys'],
                              right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': '10Y'}, inplace=True)
        df_ins = df_ins[['OPR_DATE', 'Keys', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR', '30Y', '10Y']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'FORECAST_DATE')], how='left',
                              left_on=['OPR_DATE', 'Keys'], right_on=['PUBDATE', 'Keys'])
        df_ins.rename(columns={'PUBVAL': 'FORECAST_DATE'}, inplace=True)
        df_ins = pd.concat([df_ins[['OPR_DATE', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR', '30Y', '10Y',
                                    'FORECAST_DATE']], df_ins['Keys'].str.split(',', expand=True)], axis=1)

        df_ins.rename(columns={0: 'REGION_NAME', 1: 'FORECAST_DAY'}, inplace=True)

        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[
            ['OPR_DATE', 'REGION_NAME', 'FORECAST_DAY', 'DAY_OF_WEEK', 'FORECAST', 'FORECAST_CHANGE', 'LAST_YEAR',
             '30Y', '10Y', 'FORECAST_DATE']]
        db = 'WX1-GC'
        table = 'MS_CWG_ELEC_CDD_BAL_DAILY_NEXT'
        CF.insert_update(db,table,df_ins)
        print('Done ' + str(list_name[i]) + ' ' + str(start_date))

def exchange_rates(start_date, end_date):
    list_name = get_list_of_publisher_lists('exchange_rates')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Rate')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'RATE','Keys':'FX'},inplace=True)
        df_ins = df_ins[['OPR_DATE','FX','RATE']]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','FX','RATE']]
        db = 'WX1-GC'
        table='MS_BOC_EXCHANGE_RATES'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))

def apx_hourly(start_date, end_date):
    list_name = get_list_of_publisher_lists('apx_hourly')
    for i in range(len(list_name)):
        df_api = get_publisher_list(list_name[i], start_date, end_date)
        df_ins = df_api.loc[np.where(df_api.PUBCOL == 'Price')]
        df_ins.rename(columns={'PUBDATE': 'OPR_DATE','PUBVAL':'PRICE','Keys':'LOCATION'},inplace=True)
        df_ins = df_ins[['OPR_DATE','LOCATION','PRICE']]
        df_ins = df_ins.merge(df_api.loc[np.where((df_api.PUBCOL == 'Volume') | (df_api.PUBCOL == 'Net_Volume'))], how='left', left_on=['OPR_DATE','LOCATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'VOLUME'},inplace=True)
        df_ins = df_ins[['OPR_DATE','LOCATION','PRICE','VOLUME']]
        df_ins = df_ins.merge(df_api.loc[np.where(df_api.PUBCOL == 'Published_Interval')], how='left', left_on=['OPR_DATE','LOCATION'], right_on = ['PUBDATE','Keys'])
        df_ins.rename(columns={'PUBVAL':'PUBLISHED_INTERVAL'},inplace=True)
        df_ins = df_ins[['OPR_DATE','LOCATION','PRICE','VOLUME','PUBLISHED_INTERVAL']]
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['OPR_DATE'])]
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION','PRICE','VOLUME','PUBLISHED_INTERVAL']]
        db = 'WX1-GC'
        table='MS_APX_HOURLY'
        CF.insert_update(db,table,df_ins)
        print('Done '+ str(list_name[i]) + ' ' + str(start_date))