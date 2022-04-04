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

def get_history(feed_name, key_name, key, obs, start_date, end_date):
    user='riley.day@laurioncap.com'
    pwd='ForSharingMS1'
    obs_str=''
    for i in obs:
        obs_str += '&cols='+i
    key_str = ''
    for j in range(len(key)):
        split_key = key[j].split(',',len(key_name))
    for k in range(len(key_name)):
        key_str += key_name[k]+'='+split_key[k]+'&'
    key_str=key_str[:-1]
    #r1 = requests.get('http://mp.morningstarcommodity.com/lds/feeds/'+feed_name+'/ts?'+key_name+'='+key+obs_str+'&fromDateTime='+start_date+'&toDateTime='+end_date, auth=HTTPBasicAuth(user, pwd))
    r1 = requests.get('http://mp.morningstarcommodity.com/lds/feeds/'+feed_name+'/ts?'+key_str+obs_str+'&fromDateTime='+start_date+'&toDateTime='+end_date, auth=HTTPBasicAuth(user, pwd))
    data = io.StringIO(r1.content.decode("utf-8"))
    df_api = pd.read_csv(data,sep=',')

def isone_da_lmp_hist(key_list, start_date, end_date):
    feed_name='Isone_LmpsDaHourly'
    key_name = ['LocID']
    data_cols = ['Location','LmpTotal']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['Date'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['LOCATION_ID'] = x
        df_ins.rename(columns={'Location('+x+')': 'LOCATION','LmpTotal('+x+')':'LMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LMP']]
        db = 'WX1-GC'
        table='MS_ISONE_DA_LMP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)

def isone_rt_lmp_hist(key_list, start_date, end_date):
    feed_name='Isone_FinalRealTimeLmp'
    key_name = ['Location_ID']
    data_cols = ['Location_Name','Location_Type','Locational_Marginal_Price','Published_Interval']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['LOCATION_ID'] = x
        df_ins.rename(columns={'Location_Name('+x+')': 'LOCATION','Published_Interval('+x+')': 'OPR_HOUR','Locational_Marginal_Price('+x+')':'LMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','LOCATION_ID','LOCATION','LMP']]
        db = 'WX1-GC'
        table='MS_ISONE_RT_LMP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)

def pjm_rt_lmp_hist(key_list, start_date, end_date):
    feed_name='PJM_Rt_Hourly_Lmp'
    key_name = ['PnodeID']
    data_cols = ['Interval','TotalLMP']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['PNODE_ID'] = x
        df_ins['LOCATION'] = ''
        df_ins.rename(columns={'Location_Name('+x+')': 'LOCATION','Interval('+x+')': 'OPR_HOUR','TotalLMP('+x+')':'LMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PNODE_ID','LOCATION','LMP']]
        db = 'WX1-GC'
        table='MS_PJM_RT_LMP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)

def pjm_da_lmp_hist(key_list, start_date, end_date):
    feed_name='PJM_Da_Hourly_Lmp'
    key_name = ['PnodeID']
    data_cols = ['TotalLMP']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['Date'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['PNODE_ID'] = x
        df_ins['LOCATION'] = ''
        df_ins['LOCATION_TYPE'] = ''
        df_ins.rename(columns={'Location_Name('+x+')': 'LOCATION','TotalLMP('+x+')':'LMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PNODE_ID','LOCATION','LOCATION_TYPE','LMP']]
        db = 'WX1-GC'
        table='MS_PJM_DA_LMP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)

def ercot_dam_spp_hist(key_list, start_date, end_date):
    feed_name='ERCOT_DamSettlementPointPrices'
    key_name = ['SettlementPoint']
    data_cols = ['SettlementPointPrice','DSTFlag']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['Date'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['SETTLEMENT_POINT'] = x
        df_ins['LOCATION'] = ''
        df_ins['LOCATION_TYPE'] = ''
        df_ins.rename(columns={'DSTFlag('+x+')': 'DST_FLAG','SettlementPointPrice('+x+')':'SPP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','SETTLEMENT_POINT','SPP','DST_FLAG']]
        db = 'WX1-GC'
        table='MS_ERCOT_DAM_SPP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)

def ercot_rt_spp_hist(key_list, start_date, end_date):
    feed_name='ERCOT_SettlementPointPrices'
    key_name = ['SettlementPointName','SettlementPointType']
    data_cols = ['DeliveryInterval','SettlementPointPrice','DSTFlag','DeliveryHour']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['SETTLEMENT_POINT'] = x[:x.find(',')]
        df_ins['SETTLEMENT_POINT_TYPE'] = x[x.find(',')+1:]
        df_ins.rename(columns={'DeliveryInterval('+x.replace(',',';')+')': 'INTERVAL_15MIN','SettlementPointPrice('+x.replace(',',';')+')': 'SPP','DSTFlag('+x.replace(',',';')+')': 'DST_FLAG','DeliveryHour('+x.replace(',',';')+')':'OPR_HOUR'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','INTERVAL_15MIN','SETTLEMENT_POINT','SETTLEMENT_POINT_TYPE','SPP','DST_FLAG']]
        db = 'WX1-GC'
        table='MS_ERCOT_RT_SPP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)

def nyiso_da_lbmp_hist(key_list, start_date, end_date):
    feed_name='Nyiso_DamlbmpZone'
    key_name = ['PTID']
    data_cols = ['Name','LBMP_MWHr']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['Date'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['PTID'] = x
        df_ins.rename(columns={'Name('+x+')': 'LOCATION','LBMP_MWHr('+x+')':'LBMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PTID','LOCATION','LBMP']]
        db = 'WX1-GC'
        table='MS_NYISO_DA_LBMP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)

def nyiso_rt_lbmp_hist(key_list, start_date, end_date):
    feed_name='Nyiso_RtlbmpZone'
    key_name = ['PTID']
    data_cols = ['Name','LBMP_MWHr']
    for i in key_list:
        x=i[0]
        df_ins = get_history(feed_name, key_name, i, data_cols, start_date, end_date)
        df_ins['OPR_HOUR'] = [int(str(datetime.time(d))[:2])+1 for d in pd.to_datetime(df_ins['Date'])]
        df_ins['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_ins['Date'])]
        df_ins['PTID'] = x
        df_ins.rename(columns={'Name('+x+')': 'LOCATION','LBMP_MWHr('+x+')':'LBMP'},inplace=True)
        df_ins = df_ins[['OPR_DATE','OPR_HOUR','PTID','LOCATION','LBMP']]
        db = 'WX1-GC'
        table='MS_NYISO_RT_LBMP'
        CF.insert_update(db,table,df_ins)
        print ('Inserted date for ' + x)