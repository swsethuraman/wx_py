import requests
import json
import pandas as pd
import numpy as np
import os
from datetime import date, datetime, timedelta
from io import StringIO
import time
import io
from requests.auth import HTTPBasicAuth
from dateutil import parser
from xlib import xdb
from wx.providers.common import Common_Functions as CF
import openpyxl as op
import zipfile
import csv

def get_noaa_ghcnd(start_date, end_date):
    db = 'WX1-GC'
    table = 'NOAA_Daily_Tempr'
    sql = """
            select distinct stationid from WX1.NOAA_Daily_Tempr where recorddate>date_sub(curdate(),interval 30 day)
        """
    conn = xdb.make_conn(db, stay_open=True)
    df = conn.query(sql)
    conn.close()
    stations = df.to_numpy()
    url_base = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    token = 'YznOUAOeEnTCIZKbNHAooghTSfbVdUMm'
    data_set_id = 'GHCND'
    for i in stations:
        try:
            station_id = i[0]
            payload = {
                'datasetid': data_set_id,
                'stationid': data_set_id + ':' + station_id,
                'datatypeid': ['TMAX', 'TMIN'],
                'startdate': start_date.astype(str),
                'enddate': end_date.astype(str),
                'units': 'standard',
                'limit': '1000',
            }
            headers = {
                'token': token
            }
            url_data = requests.get(url_base, params=payload, headers=headers).content
            url_data = json.loads(url_data)
            df = pd.DataFrame(url_data['results'])
            df = df.pivot(index='date', columns='datatype')['value'].reset_index()
            df = df.rename(columns={'date': 'RecordDate', 'TMAX': 'TMax', 'TMIN': 'TMin'})
            df['RecordDate'] = pd.to_datetime(df['RecordDate'], unit='ns').astype(str)
            df.insert(0, 'StationID', station_id)
            df.insert(0, 'DataSetID', data_set_id)
            CF.insert_update(db,table,df)
            print('Inserted ' + i + ' ' + str())
        except:
            print('Failed ' + i + ' ' + str())

def get_meteofrance(month):
    try:
        url='https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.' + month + '.csv.gz'
        r = requests.get(url, allow_redirects=True)
        content = r.content.decode("utf-8")
        content = StringIO(content)
        df=pd.read_csv(content, sep=';')
        df = df[['numer_sta','date','t','tn12','tn24','tx12','tx24']]
        df['opr_date'] = df['date'].astype(str).str[:8]
        df['opr_hour'] = df['date'].astype(str).str[8:10]
        df['t'] = np.where(df['t']=='mq','',df['t'])
        df['tn12'] = np.where(df['tn12']=='mq',np.nan,df['tn12'])
        df['tn24'] = np.where(df['tn24']=='mq',np.nan,df['tn24'])
        df['tx12'] = np.where(df['tx12']=='mq',np.nan,df['tx12'])
        df['tx24'] = np.where(df['tx24']=='mq',np.nan,df['tx24'])
        df = df[['opr_date','opr_hour','numer_sta','t','tn12','tn24','tx12','tx24']]
        df.rename(columns={'numer_sta':'station'},inplace=True)
        db = 'WX1-GC'
        table='METEOFRANCE_OMM_SYNOP_3HR'
        CF.insert_update(db,table,df)
        print('Inserted data for ' + month)
    except:
        print('failed on '+ month)

def get_knmi_klimatologie(start_date, end_date):
    start=start_date
    end=end_date
    try:
        url = 'http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi'
        myobj = {'stns': 'ALL', 'vars': 'FG:TG:TN:TX:Q', 'byear': str(start.year), 'bmonth': str(start.month),
                 'bday': str(start.day), 'eyear': str(end.day), 'emonth': str(end.month), 'eday': str(end.day)}
        x = requests.post(url, data=myobj)
        df = pd.DataFrame(x.content.decode("utf-8").split('\r\n'))
        df = df.iloc[np.where(df[0].astype(str).str[0] != '#')]
        df = df[0].str.split(',', expand=True)
        df = df.rename(columns={0: 'STATION', 1: 'OPR_DATE', 2: 'FG', 3: 'TG', 4: 'TN', 5: 'TX', 6: 'Q'})
        df = df.reset_index(drop=True)
        df = df.iloc[
            np.where((df['OPR_DATE'].astype(str).str[:2] == '20') | (df['OPR_DATE'].astype(str).str[:2] == '19'))]
        df['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df['OPR_DATE'])]
        df = df.reset_index(drop=True)
        df = df[['OPR_DATE', 'STATION', 'FG', 'TG', 'TN', 'TX', 'Q']]
        db = 'WX1-GC'
        table = 'KNMI_KLIMATOLOGIE_DAILY'
        CF.insert_update(db,table,df)
        print('Inserted data for ' + str(start) + ' to ' + str(end))
    except:
        print('failed on ' + str(start) + ' to ' + str(end))

def knmi_download_files(base_url,save_to):
    sql_locs = """
        select distinct STATION from WX1.KNMI_KLIMATOLOGIE_DAILY
        where opr_date>date'2021-1-1'
    """
    db='WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    locs = conn.query(sql_locs).to_numpy()
    conn.close()
    for loc in locs:
        loc = loc[0].replace(' ','')
        url = base_url + loc + '.zip'
        r = requests.get(url, stream=True)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(save_to)

def knmi_update_db(save_to):
    for txt in os.listdir(save_to):
        if txt.endswith('.txt'):
            print(txt)
            file = open(save_to+txt, 'r')
            stn_file=txt.replace('etmgeg_','').replace('.txt','')
            df = pd.DataFrame(columns=['LINE'])
            for line in file.readlines():
                stn_line=line[:line.find(',')].replace(' ','')
                if stn_file==stn_line:
                    new_row = {'LINE':line}
                    df = df.append(new_row, ignore_index=True)
            df = df['LINE'].str.split(',',expand=True)
            df = df.rename(columns={0:'STATION',1:'OPR_DATE',4:'FG',11:'TG',12:'TN',14:'TX',20:'Q'})
            df['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df['OPR_DATE'])]
            df = df[['OPR_DATE','STATION','FG','TG','TN','TX','Q']]
            df = df.iloc[np.where(pd.DatetimeIndex(df['OPR_DATE']).year>=2020)].reset_index(drop=True)
            db='WX1-GC'
            table='KNMI_KLIMATOLOGIE_DAILY'
            CF.insert_update(db,table,df,'Y')

def get_dwd_data(history):
    db = 'WX1-GC'
    table = 'DWD_CLIMATE_DAILY_KL'
    if history=='Y':
        dir = '/data/weather/dwd/historical-extracted/'
    else:
        dir = '/data/weather/dwd/recent-extracted/'
    for file in os.listdir(dir):
        print(file)
        df = pd.read_csv(dir+file, sep=';')
        for col in df.columns:
            df = df.rename(columns={col:col.strip()})
        df = df[['MESS_DATUM','STATIONS_ID','TXK','TNK']]
        df['OPR_DATE'] = pd.to_datetime(df['MESS_DATUM'], format='%Y%m%d')
        df = df[['OPR_DATE','STATIONS_ID','TXK','TNK']]
        CF.insert_update(db,table,df,'Y')

def get_cme_stlalt(run_date):
    try:
        year = str(run_date.year)
        month = str('0' + str(run_date.month))[-2:]
        day = str('0' + str(run_date.day))[-2:]
        path = '/data/cme/stlalt/' + year + '/web-stlalt-' + year + '-' + month + '-' + day
        x=0
        lines=[]
        with open(path, "r") as f:
            for line in f:
                if x>1 and str(line)[:5]!='TOTAL':
                    lines.append(str(line).replace('\n',''))
                x+=1
        lines = lines[:-1]
        df = pd.DataFrame(lines)
        df = df[0].str.split('  +',expand=True)
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        df.insert(0,'PRODUCT',None)
        df['PRODUCT'] = np.where(df['OPEN'].isnull(),df['STRIKE'],None)
        for i in range(100):
            df['PRODUCT'] = np.where(df['PRODUCT'].isnull(),df['PRODUCT'].shift(1),df['PRODUCT'])
        df['STRIKE'] = np.where(df['PRODUCT']==df['STRIKE'],None,df['STRIKE'])
        df = df.iloc[np.where(df['STRIKE'].isnull()==False)]
        df = df.reset_index(drop=True)
        df = df.iloc[:, :-3]
        df = df[['PRODUCT','STRIKE','SETT','CHGE','EST.VOL']]
        df['CHGE'] = df['CHGE'].replace('UNCH','0')
        df['CHGE'] = df['CHGE'].str.replace('+','')
        df['OPR_DATE'] = year + month + day
        df['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df['OPR_DATE'])]
        df = df.rename(columns={'EST.VOL':'EST_VOL'})
        df = df[['OPR_DATE','PRODUCT','STRIKE','SETT','CHGE','EST_VOL']]
        db = 'WX1-GC'
        table='CME_STLALT'
        CF.insert_update(db,table,df)
        print('Inserted data for ' + str(run_date))
    except:
        print('failed on ' + str(run_date))


def cwg_city_obs_f(start_date, end_date, region_list, url_base):
    date_list=[]
    regions=region_list
    startDate = start_date
    endDate = end_date
    for i in range((endDate-startDate).days + 1):
        date_list.append(startDate + timedelta(days=i))
    for i in regions:
        for j in date_list:
            try:
                url = url_base
                url = url + '/' + i + '_observations_final_'
                url = url + str(j.year) + str(('0' + str(j.month)).replace('00','0'))[-2:] + str(('0' + str(j.day)).replace('00','0'))[-2:] +'.csv'
                url_data = requests.get(url).content
                data = io.StringIO(url_data.decode("utf-8"))
                df_api = pd.read_csv(data,sep=',')
                df_api.rename(columns={'date': 'EFFECTIVE_DATE'},inplace=True)
                df_api.rename(columns={'MinTemp': 'MIN_TEMP'},inplace=True)
                df_api.rename(columns={'MaxTemp': 'MAX_TEMP'},inplace=True)
                db = 'WX1-GC'
                table='CWG_CITY_OBS_F'
                CF.insert_update(db,table,df_api)
                time.sleep(5)
                print('Successfully inserted ' + i + ' for date ' +str(j))
            except:
                print('Failed inserting ' + i + ' for date ' +str(j))


def cwg_city_obs_c(start_date, end_date, region_list, url_base):
    date_list=[]
    regions=region_list
    startDate = start_date
    endDate = end_date
    for i in range((endDate-startDate).days + 1):
        date_list.append(startDate + timedelta(days=i))
    for i in regions:
        for j in date_list:
            try:
                url = url_base
                url = url + '/' + i + '_observations_final_'
                url = url + str(j.year) + str(('0' + str(j.month)).replace('00','0'))[-2:] + str(('0' + str(j.day)).replace('00','0'))[-2:] +'_C.csv'
                url_data = requests.get(url).content
                data = io.StringIO(url_data.decode("utf-8"))
                df_api = pd.read_csv(data,sep=',')
                df_api.rename(columns={'date': 'EFFECTIVE_DATE'},inplace=True)
                df_api.rename(columns={'MinTemp': 'MIN_TEMP'},inplace=True)
                df_api.rename(columns={'MaxTemp': 'MAX_TEMP'},inplace=True)
                df_api.rename(columns={'AvgTemp': 'AVG_TEMP'},inplace=True)
                db = 'WX1-GC'
                table='CWG_CITY_OBS_C'
                CF.insert_update(db,table,df_api)
                time.sleep(5)
                print('Successfully inserted ' + i + ' for date ' +str(j))
            except:
                print('Failed inserting ' + i + ' for date ' +str(j))


def cwg_city_fcst(start_date, end_date, region_list, url_base):
    date_list=[]
    regions=region_list
    startDate = start_date
    endDate = end_date
    for i in range((endDate-startDate).days + 1):
        date_list.append(startDate + timedelta(days=i))
    for i in regions:
        for j in date_list:
            try:
                url = url_base
                url = url + '/city15dfcst_' + i + '_'
                url = url + (str('0' + str(j.month)).replace('00','0'))[-2:] + (str('0' + str(j.day)).replace('00','0'))[-2:] + str(j.year)[-2:] +'.csv'
                url = url.replace('_northamerica_','')
                url_data = requests.get(url).content
                data = io.StringIO(url_data.decode("utf-8"))
                df_api = pd.read_csv(data,sep=',')
                df_api.rename(columns={'Date': 'FORECAST_DATE'},inplace=True)
                df_api.rename(columns={'Production Date': 'EFFECTIVE_DATE'},inplace=True)
                df_api.rename(columns={'Fcst Mn': 'FCST_MIN'},inplace=True)
                df_api.rename(columns={'Fcst Mx': 'FCST_MAX'},inplace=True)
                df_api.rename(columns={'Fcst Avg': 'FCST_AVG'},inplace=True)
                df_api.rename(columns={'Norm Mn': 'NORM_MIN'},inplace=True)
                df_api.rename(columns={'Norm Max': 'NORM_MAX'},inplace=True)
                df_api['EFFECTIVE_DATE']=pd.to_datetime(df_api['EFFECTIVE_DATE'])
                df_api['FORECAST_DATE']=pd.to_datetime(df_api['FORECAST_DATE'])
                db = 'WX1-GC'
                table='CWG_CITY_FCST_'
                table = table + i.upper()
                CF.insert_update(db,table,df_api)
                time.sleep(5)
                print('Successfully inserted ' + i + ' for date ' +str(j))
            except:
                print('Failed inserting ' + i + ' for date ' +str(j))

def madis2daily_getraw(start_date, end_date, database):
    sql="""
        select OPR_DATE,
            STATION,
            TEMP_K
        from (
            select date(date_add(date_add(OPR_DATE, interval OPR_HOUR hour), interval b.OFFSET_FROM_CENTRAL hour)) as OPR_DATE, 
                a.STATION, 
                a.TEMP_K 
            from (
                select * from WX1.MS_NOAA_MADIS_HOURLY
                where opr_date between date_sub(date'"""+start_date+"""',interval 1 day) and date_add(date'"""+end_date+"""',interval 1 day)
            ) a
            inner join WX1.NOAA_MADIS_TZ_REF b
            on a.STATION=b.STATION
        ) c
        where c.opr_date between date'"""+start_date+"""' and date'"""+end_date+"""'
        """
    #print(sql)
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df = conn.query(sql)
    conn.close()
    return df

def madis2daily_formatdata(df):
    df_max = df[['OPR_DATE','STATION','TEMP_K']].groupby(['OPR_DATE','STATION']).max()
    df_max = df_max.reset_index()
    df_max = df_max.rename(columns={'TEMP_K':'TMAX_K'})
    df_min = df[['OPR_DATE','STATION','TEMP_K']].groupby(['OPR_DATE','STATION']).min()
    df_min = df_min.reset_index()
    df_min = df_min.rename(columns={'TEMP_K':'TMIN_K'})
    df_ins=df[['OPR_DATE','STATION']]
    df_ins=pd.merge(df_ins,df_max,how='left',on=['OPR_DATE','STATION'])
    df_ins=pd.merge(df_ins,df_min,how='left',on=['OPR_DATE','STATION'])
    df_ins['TMAX_C']=df_ins['TMAX_K']-273.15
    df_ins['TMIN_C']=df_ins['TMIN_K']-273.15
    df_ins['TMAX_F']=(1.8*df_ins['TMAX_C'])+32
    df_ins['TMIN_F']=(1.8*df_ins['TMIN_C'])+32
    df_ins = df_ins[['OPR_DATE','STATION','TMAX_C','TMIN_C','TMAX_F','TMIN_F']]
    return df_ins

def get_jma_data(run_date,base_url_max,base_url_min):
    year = str(run_date.year)
    month = str('0'+str(run_date.month))[-2:]
    day = str('0'+str(run_date.day))[-2:]
    for i in range(24):
        hour = i
        hour = str('0'+str(hour))[-2:]
        try:
            url_max = base_url_max+year+month+day+hour+'00.csv'
            url_min = base_url_min+year+month+day+hour+'00.csv'
            r_max = requests.get(url_max).content
            r_min = requests.get(url_min).content
            df_max = pd.read_csv(io.StringIO(r_max.decode('utf-8','ignore')))
            df_min = pd.read_csv(io.StringIO(r_min.decode('utf-8','ignore')))
            df_max['STATION_ID'] = df_max[df_max.columns[0]]
            df_max['OPR_DATE'] = df_max[df_max.columns[5]].astype(str) + '/' + df_max[df_max.columns[6]].astype(str) + '/' + df_max[df_max.columns[4]].astype(str)
            df_max['OPR_HOUR'] = df_max[df_max.columns[7]]
            df_max['TMAX'] = df_max[df_max.columns[9]]
            df_max['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_max['OPR_DATE'])]
            df_max = df_max[['OPR_DATE','OPR_HOUR','STATION_ID','TMAX']]
            df_min['STATION_ID'] = df_min[df_min.columns[0]]
            df_min['OPR_DATE'] = df_min[df_min.columns[5]].astype(str) + '/' + df_min[df_min.columns[6]].astype(str) + '/' + df_min[df_min.columns[4]].astype(str)
            df_min['OPR_HOUR'] = df_min[df_min.columns[7]]
            df_min['TMIN'] = df_min[df_min.columns[9]]
            df_min['OPR_DATE'] = [datetime.date(d) for d in pd.to_datetime(df_min['OPR_DATE'])]
            df_min = df_min[['OPR_DATE','OPR_HOUR','STATION_ID','TMIN']]
            df = pd.merge(df_max,df_min,how='inner',on=['OPR_DATE','OPR_HOUR','STATION_ID'])
            db='WX1-GC'
            table = 'JMA_HOURLY_WEATHER'
            CF.insert_update(db,table,df,'Y')
            print('Inserted data for '+year+month+day+hour)
        except:
            print('Failed getting data for '+year+month+day+hour)