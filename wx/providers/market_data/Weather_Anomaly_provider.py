import pandas as pd
import numpy as np
from xlib import xdb
import sqlite3
import xlsxwriter
from datetime import datetime
from wx.providers.common import Common_Functions as CF
db='WX1-GC'
distance_threshold = '1'
min_date = '2000-1-1'
min_dev_threshold = 2

#PREPROCESSING
def pre_processing(db,min_date,distance_threshold):
    sql_stations = """
        select icao
        from WX1.WX_WEATHER_DAILY_CLEANED
        where opr_date>date_sub(curdate(),interval 10 year)
            and icao like 'K%'
            and tmin is not null and tmax is not null
        group by icao
        having count(*)>3600
    """
    conn = xdb.make_conn(db, stay_open=True)
    stations = conn.query(sql_stations)
    conn.close()
    stations = stations.to_numpy()
    for i in stations:
        main_station=i[0]
        pair_station='XXXX'
        try:
            df=get_raw(main_station, pair_station, min_date, distance_threshold)
        except:
            print('Failed to find data or pair for ' + main_station)
        try:
            df_monthly_anom=get_monthly_anomalies(df)
        except:
            'Failed to calculate anomalies for ' + main_station
        try:
            CF.insert_update(db,'WX_STATION_ANOMALIES',df_monthly_anom,'N')
            print('Inserted data for '+main_station)
        except:
            print('Failed inserting data for ' + main_station)

def process_anomalies(min_dev_threshold):
    df_hist = get_processed_data('WX_STATION_ANOMALIES')
    df_stats = get_processed_stats(df_hist)
    final_processing(df_stats, min_dev_threshold)

def create_send_report():
    df_final = get_final_processed()
    df_final_anom = df_final.iloc[np.where(df_final['MEAN_DEV_P1'] >= min_dev_threshold)].reset_index(drop=True)
    df_final_anom = df_final_anom.iloc[
        np.where(df_final_anom['MEAN_DEV_P2'] > df_final_anom['MEAN_DEV_P3'])].reset_index(drop=True)
    df_final_cme = df_final.iloc[np.where(df_final['MAIN_P1'].isin(
        ['KLGA', 'KATL', 'KCVG', 'KORD', 'KMSP', 'KDFW', 'KLAS', 'KSAC', 'KPDX']))].reset_index(drop=True)
    stations1 = df_final_anom['MAIN_P1'].to_numpy()
    stations2 = df_final_anom['PAIR_P1'].to_numpy()
    stations_anom = np.append(stations1, stations2, axis=0)
    stations1 = df_final_cme['MAIN_P1'].to_numpy()
    stations2 = df_final_cme['PAIR_P1'].to_numpy()
    stations_cme = np.append(stations1, stations2, axis=0)
    df_data_anom = get_report_raw(stations_anom)
    df_data_cme = get_report_raw(stations_cme)
    df_report_anom = create_report(df_data_anom, df_final_anom)
    df_report_cme = create_report(df_data_cme, df_final_cme)
    # CREATE EXCEL FILE AND EMAIL
    today = datetime.today()
    mon = '0' + str(today.month)
    mon = mon[-2:]
    yr = str(today.year)
    day = '0' + str(today.day)
    day = day[-2:]
    writer = pd.ExcelWriter(
        '/home/rday/data/Weather_Station_Anomaly/wx_station_anomaly_report' + yr + mon + day + '.xlsx',
        engine='xlsxwriter')
    df_final_anom.to_excel(writer, sheet_name='AnomalousStations', index=False)
    df_report_anom.to_excel(writer, sheet_name='AnomalousStationsDetails', index=False)
    df_final_cme.to_excel(writer, sheet_name='CMEStations', index=False)
    df_report_cme.to_excel(writer, sheet_name='CMEStationDetails', index=False)
    writer.save()
    report ='Station Anomaly Report'
    current_file = '/home/rday/data/Weather_Station_Anomaly/wx_station_anomaly_report'+yr+mon+day+'.xlsx'
    CF.emailer(report,current_file,yr,mon,day)

#UNDERLYING FUNCTIONS
def get_raw(main_station, pair_station, min_date, distance_threshold):
    db='WX1-GC'
    #TRY PART SHOULD WORK IF pair_stations WERE ENTERED. EXCEPT SHOULD BE TRIGGERED IF pair_stations=[]
    lat_query =  "select lat from WX1.NOAA_WMO_WBAN where ICAO='" + main_station + "'"
    lon_query =  "select lon from WX1.NOAA_WMO_WBAN where ICAO='" + main_station + "'"
    conn = xdb.make_conn(db, stay_open=True)
    df_lat= conn.query(lat_query)
    df_lon= conn.query(lon_query)
    conn.close()
    lat = df_lat.values[0][0]
    lon = df_lon.values[0][0]
    sql_main = """
        select opr_date, 
            icao, 
            (tmin+tmax)/2 as tavg 
        from WX1.WX_WEATHER_DAILY_CLEANED
        where icao='""" + main_station + """'
        and opr_date>=date'""" + min_date + """'
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_main= conn.query(sql_main)
    conn.close()
    sql_pairs = """
    select opr_date,
        icao,
        (tmin+tmax)/2 as tavg
    from WX1.WX_WEATHER_DAILY_CLEANED
    where icao in (
        select distinct icao 
        from WX1.NOAA_WMO_WBAN
        where icao<>'""" + main_station + """'
        and icao<>'""" + pair_station + """'
        and abs((lat) - """ + str(lat) + """) < """ + distance_threshold + """
        and abs((lon) - """ + str(lon) + """) < """ + distance_threshold + """
    )
    and icao in (
        select icao
        from WX1.WX_WEATHER_DAILY_CLEANED
        where opr_date>date_sub(curdate(),interval 10 year)
            and tmin is not null 
            and tmax is not null
        group by icao
        having count(*)>3600
    )
    and opr_date>=date'""" + min_date + """' 
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_pairs= conn.query(sql_pairs)
    conn.close()
    df_pair_options = pd.merge(df_main,df_pairs,how='left',on=['opr_date'])
    return df_pair_options

def get_monthly_anomalies(df, force_pair='XXXX'):
    df_corrs=df[['icao_y','tavg_x','tavg_y']].groupby('icao_y').corr().reset_index()
    df_corrs=df_corrs[['icao_y','tavg_x']].iloc[np.where(df_corrs['tavg_x']<1)].reset_index().sort_values(by=['tavg_x'], ascending=False)
    if force_pair!='XXXX':
        df_corrs=df_corrs.iloc[np.where(df_corrs['icao_y']==force_pair)]
    pair=df_corrs['icao_y'].values[0]
    df_pair=df[['opr_date','icao_y','tavg_y']].iloc[np.where(df['icao_y']==pair)].reset_index(drop=True)
    df_main=df[['opr_date','icao_x','tavg_x']].drop_duplicates().reset_index(drop=True)
    df_comb=pd.merge(df_main,df_pair,how='inner',on='opr_date')
    temp_conn = sqlite3.connect(':memory:')
    df_comb.to_sql('comb', temp_conn, index=False)
    query = """
        select a.opr_month,
            a.opr_year,
            a.main,
            a.pair,
            a.monthly_delta - avg(b.monthly_delta) as drift_from_tenyr,
            a.num_days
        from (
            select cast(strftime('%m', opr_date) as decimal) as opr_month,
                cast(strftime('%Y', opr_date) as decimal) as opr_year,
                icao_x as main,
                icao_y as pair,
                avg(tavg_x - tavg_y) as monthly_delta,
                count(*) as num_days
            from comb
            group by strftime('%m', opr_date), strftime('%Y', opr_date), icao_x, icao_y
        ) a
        inner join (
            select strftime('%m', opr_date) as opr_month,
                strftime('%Y', opr_date) as opr_year,
                icao_x as main,
                icao_y as pair,
                avg(tavg_x - tavg_y) as monthly_delta
            from comb
            group by strftime('%m', opr_date), strftime('%Y', opr_date), icao_x, icao_y
        ) b
        on a.opr_month=b.opr_month
        and a.opr_year>b.opr_year
        and a.opr_year<b.opr_year+11
        group by a.opr_month,a.opr_year,a.main,a.pair,a.monthly_delta, a.num_days
        having count(*)=10
        order by a.opr_year, a.opr_month
    """
    df_ten_yr = pd.read_sql_query(query, temp_conn)
    temp_conn.close()
    return df_ten_yr

def get_processed_data(table):
    #NEED TO FIX FIRST QUERY TO BE ROLLING 10YR
    sql_hist = """
        select date(concat(cast(OPR_YEAR as char),'-',cast(OPR_MONTH as char),'-1')) as OPR_DATE, 
            MAIN, 
            PAIR, 
            NUM_DAYS,
            DRIFT_FROM_TENYR
        from WX1.""" + table + """
        where (MAIN, PAIR) in (
            select MAIN, PAIR from WX1.""" + table + """ 
            where OPR_YEAR>=2010 
            group by MAIN, PAIR 
            having count(*)>120
        )
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_hist = conn.query(sql_hist)
    conn.close()
    temp_conn = sqlite3.connect(':memory:')
    df_hist.to_sql('hist', temp_conn, index=False)
    query_hist = """
        select c.OPR_DATE,
            c.MAIN,
            c.PAIR,
            c.NUM_DAYS,
            c.DRIFT_FROM_TENYR,
            c.DRIFT_3M,
            c.DRIFT_6M,
            avg(d.DRIFT_FROM_TENYR) as DRIFT_12M
        from (
            select a.OPR_DATE,
                a.MAIN,
                a.PAIR,
                a.NUM_DAYS,
                a.DRIFT_FROM_TENYR,
                a.DRIFT_3M,
                avg(b.DRIFT_FROM_TENYR) as DRIFT_6M
            from (
                select a1.OPR_DATE,
                    a1.MAIN,
                    a1.PAIR,
                    a1.NUM_DAYS,
                    a1.DRIFT_FROM_TENYR,
                    avg(a2.DRIFT_FROM_TENYR) as DRIFT_3M
                from hist a1
                inner join hist a2
                on a1.MAIN=a2.MAIN
                    and a1.PAIR=a2.PAIR
                    and a1.OPR_DATE>a2.OPR_DATE
                    and a1.OPR_DATE<=date(a2.OPR_DATE,'+3 months')
                group by a1.OPR_DATE, a1.MAIN, a1.PAIR, a1.NUM_DAYS, a1.DRIFT_FROM_TENYR
            ) a
            inner join hist b
            on a.MAIN=b.MAIN
                and a.PAIR=b.PAIR
                and a.OPR_DATE>b.OPR_DATE
                and a.OPR_DATE<=date(b.OPR_DATE,'+6 months')
            group by a.OPR_DATE, a.MAIN, a.PAIR, a.NUM_DAYS, a.DRIFT_FROM_TENYR, a.DRIFT_3M
        ) c
        inner join hist d
        on c.MAIN=d.MAIN
            and c.PAIR=d.PAIR
            and c.OPR_DATE>d.OPR_DATE
            and c.OPR_DATE<=date(d.OPR_DATE,'+12 months')
        group by c.OPR_DATE, c.MAIN, c.PAIR, c.NUM_DAYS, c.DRIFT_FROM_TENYR, c.DRIFT_3M, c.DRIFT_6M   
    """
    df_hist = pd.read_sql_query(query_hist, temp_conn)
    temp_conn.close()
    return df_hist
    #CONSIDER REMOVING CURRENT MONTH, BUT THEN NEED TO REMOVE FIRST LINE OF FINAL PROCESSING

def get_processed_stats(df_hist):
    df_std = df_hist[['MAIN','PAIR','DRIFT_FROM_TENYR','DRIFT_3M','DRIFT_6M','DRIFT_12M']].groupby(['MAIN','PAIR']).std().reset_index()
    df_avg = df_hist[['MAIN','PAIR','DRIFT_FROM_TENYR','DRIFT_3M','DRIFT_6M','DRIFT_12M']].groupby(['MAIN','PAIR']).mean().reset_index()
    df_stats = pd.merge(df_hist,df_avg,how='inner',on=['MAIN','PAIR'])
    df_stats = df_stats.rename(columns={'DRIFT_FROM_TENYR_x':'DRIFT_FROM_TENYR','DRIFT_3M_x':'DRIFT_3M','DRIFT_6M_x':'DRIFT_6M','DRIFT_12M_x':'DRIFT_12M'})
    df_stats = df_stats.rename(columns={'DRIFT_FROM_TENYR_y':'DRIFT_FROM_TENYR_MEAN','DRIFT_3M_y':'DRIFT_3M_MEAN','DRIFT_6M_y':'DRIFT_6M_MEAN','DRIFT_12M_y':'DRIFT_12M_MEAN'})
    df_stats = pd.merge(df_stats,df_std,how='inner',on=['MAIN','PAIR'])
    df_stats = df_stats.rename(columns={'DRIFT_FROM_TENYR_x':'DRIFT_FROM_TENYR','DRIFT_3M_x':'DRIFT_3M','DRIFT_6M_x':'DRIFT_6M','DRIFT_12M_x':'DRIFT_12M'})
    df_stats = df_stats.rename(columns={'DRIFT_FROM_TENYR_y':'DRIFT_FROM_TENYR_STD','DRIFT_3M_y':'DRIFT_3M_STD','DRIFT_6M_y':'DRIFT_6M_STD','DRIFT_12M_y':'DRIFT_12M_STD'})
    df_stats['DRIFT_FROM_TENYR_DEV'] = abs((df_stats['DRIFT_FROM_TENYR'] - df_stats['DRIFT_FROM_TENYR_MEAN']) / df_stats['DRIFT_FROM_TENYR_STD'])
    df_stats['DRIFT_3M_DEV'] = abs((df_stats['DRIFT_3M'] - df_stats['DRIFT_3M_MEAN']) / df_stats['DRIFT_3M_STD'])
    df_stats['DRIFT_6M_DEV'] = abs((df_stats['DRIFT_6M'] - df_stats['DRIFT_6M_MEAN']) / df_stats['DRIFT_6M_STD'])
    df_stats['DRIFT_12M_DEV'] = abs((df_stats['DRIFT_12M'] - df_stats['DRIFT_12M_MEAN']) / df_stats['DRIFT_12M_STD'])
    df_stats['MEAN_DEV'] = (df_stats['DRIFT_FROM_TENYR_DEV'] + df_stats['DRIFT_3M_DEV'] + df_stats['DRIFT_6M_DEV'] + df_stats['DRIFT_12M_DEV'])/4
    df_stats = df_stats[['OPR_DATE','MAIN','PAIR','NUM_DAYS','DRIFT_FROM_TENYR','DRIFT_3M','DRIFT_6M','DRIFT_12M','DRIFT_FROM_TENYR_DEV','DRIFT_3M_DEV','DRIFT_6M_DEV','DRIFT_12M_DEV','MEAN_DEV']]
    return df_stats

def final_processing(df_stats,min_dev_threshold):
    #SELECT MOST RECENT COMPLETE MONTH & CURRENT MONTH
    latest_period = df_stats['OPR_DATE'].iloc[np.where(df_stats['OPR_DATE']!=df_stats['OPR_DATE'].max())].reset_index(drop=True).max()
    df_stations = df_stats.iloc[np.where(df_stats['OPR_DATE']>=latest_period)].reset_index(drop=True)
    #GET WEIGHTED AVERAGE OF MOST RECENT COMPLETE MONTH & CURRENT MONTH STATS
    df_stations['OPR_DATE'] = df_stations['OPR_DATE'].astype('datetime64[ns]')
    df_stations['NEXT_MONTH'] = df_stations['OPR_DATE'] + pd.DateOffset(months=1)
    df_stations = pd.merge(df_stations,df_stations,how='inner',left_on=['OPR_DATE','MAIN','PAIR'],right_on=['NEXT_MONTH','MAIN','PAIR'],suffixes=('','_y'))
    df_stations['DRIFT_FROM_TENYR'] = ((df_stations['NUM_DAYS']*df_stations['DRIFT_FROM_TENYR']) + (df_stations['NUM_DAYS_y']*df_stations['DRIFT_FROM_TENYR_y'])) /(df_stations['NUM_DAYS']+df_stations['NUM_DAYS_y'])
    df_stations['DRIFT_FROM_TENYR_DEV'] = ((df_stations['NUM_DAYS']*df_stations['DRIFT_FROM_TENYR_DEV']) + (df_stations['NUM_DAYS_y']*df_stations['DRIFT_FROM_TENYR_DEV_y'])) /(df_stations['NUM_DAYS']+df_stations['NUM_DAYS_y'])
    df_stations['MEAN_DEV'] = (df_stations['DRIFT_FROM_TENYR_DEV'] + df_stations['DRIFT_3M_DEV'] + df_stations['DRIFT_6M_DEV'] + df_stations['DRIFT_12M_DEV'])/4
    df_stations = df_stations[['OPR_DATE','MAIN','PAIR','DRIFT_FROM_TENYR','DRIFT_3M','DRIFT_6M','DRIFT_12M','DRIFT_FROM_TENYR_DEV','DRIFT_3M_DEV','DRIFT_6M_DEV','DRIFT_12M_DEV','MEAN_DEV']]
    #SELECT STATIONS WITH LARGEST ANOMALY
    df_stations = df_stations.iloc[np.where(df_stations['MEAN_DEV']>=min_dev_threshold)].reset_index(drop=True)
    df_stations = df_stations[['MAIN','PAIR']].drop_duplicates()
    #SELECT CME CITIES
    sql_cme = """
        select distinct MAIN, PAIR from WX1.WX_STATION_ANOMALIES where MAIN in ('KLGA','KATL','KCVG','KORD','KMSP','KDFW','KLAS','KSAC','KPDX')
    """
    conn = xdb.make_conn('WX1-GC', stay_open=True)
    df_stations_cme = conn.query(sql_cme)
    conn.close()
    df_stations = df_stations.append(df_stations_cme)
    #RUN LOOP FOR ALL CITIES OF INTEREST
    sus_stations = df_stations.to_numpy()
    for i in sus_stations:
        main=i[0]
        pair=i[1]
        print(main)
        try:
            df=get_raw(main, pair, min_date, distance_threshold)
            print('1')
            try:
                df_monthly_anom=get_monthly_anomalies(df)
                print('2')
                pair2 = df_monthly_anom['pair'].drop_duplicates().to_numpy()
                pair2 = pair2[0]
                print('3')
                try:
                    CF.insert_update(db,'WX_STATION_ANOMALIES_TIER2',df_monthly_anom,'N')
                    print('Inserted data for '+main)
                    try:
                        df2=get_raw(pair, main, min_date, str(int(distance_threshold)*2))
                        print('4')
                        try:
                            df_monthly_anom2=get_monthly_anomalies(df2, pair2)
                            print('5')
                            try:
                                CF.insert_update(db,'WX_STATION_ANOMALIES_TIER3',df_monthly_anom2,'N')
                                print('Inserted data for '+pair)
                            except:
                                print('Failed inserting data for ' + pair)
                        except:
                            print('Failed to calculate anomalies for ' + pair)
                    except:
                        print('Failed to find data or pair for ' + pair)
                except:
                    print('Failed inserting data for ' + main)
            except:
                print('Failed to calculate anomalies for ' + main)
        except:
            print('Failed to find data or pair for ' + main)

def get_final_processed():
    df_get1 = get_processed_data('WX_STATION_ANOMALIES')
    df_get2 = get_processed_data('WX_STATION_ANOMALIES_TIER2')
    df_get3 = get_processed_data('WX_STATION_ANOMALIES_TIER3')
    df_final1 = get_processed_stats(df_get1)
    df_final2 = get_processed_stats(df_get2)
    df_final3 = get_processed_stats(df_get3)
    temp_conn = sqlite3.connect(':memory:')
    df_final1.to_sql('anom', temp_conn, index=False)
    df_final2.to_sql('anom_t2', temp_conn, index=False)
    df_final3.to_sql('anom_t3', temp_conn, index=False)
    query = """
        select a.OPR_DATE,
            a.MAIN as MAIN_P1,
            a.PAIR as PAIR_P1,
            round(a.DRIFT_FROM_TENYR_DEV,1) as DRIFT_1M_P1,
            round(a.DRIFT_3M_DEV,1) as DRIFT_3M_P1,
            round(a.DRIFT_6M_DEV,1) as DRIFT_6M_P1,
            round(a.DRIFT_12M_DEV,1) as DRIFT_12M_P1,
            round(a.MEAN_DEV,1) as MEAN_DEV_P1,
            b.MAIN as MAIN_P2,
            b.PAIR as PAIR_P2,
            round(b.DRIFT_FROM_TENYR_DEV,1) as DRIFT_1M_P2,
            round(b.DRIFT_3M_DEV,1) as DRIFT_3M_P2,
            round(b.DRIFT_6M_DEV,1) as DRIFT_6M_P2,
            round(b.DRIFT_12M_DEV,1) as DRIFT_12M_P2,
            round(b.MEAN_DEV,1) as MEAN_DEV_P2,
            c.MAIN as MAIN_P3,
            c.PAIR as PAIR_P3,
            round(c.DRIFT_FROM_TENYR_DEV,1) as DRIFT_1M_P3,
            round(c.DRIFT_3M_DEV,1) as DRIFT_3M_P3,
            round(c.DRIFT_6M_DEV,1) as DRIFT_6M_P3,
            round(c.DRIFT_12M_DEV,1) as DRIFT_12M_P3,
            round(c.MEAN_DEV,1) as MEAN_DEV_P3
        from anom a
        inner join anom_t2 b
        on a.OPR_DATE=b.OPR_DATE
            and a.MAIN=b.MAIN
        inner join anom_t3 c
        on a.OPR_DATE=c.OPR_DATE
            and a.PAIR=c.MAIN
            and b.PAIR=c.PAIR
    """
    df_final = pd.read_sql_query(query, temp_conn)
    temp_conn.close()
    df_final = df_final.iloc[np.where(df_final['OPR_DATE']==df_final['OPR_DATE'].max())].reset_index(drop=True)
    return df_final

def get_report_raw(stations):
    df_data = pd.DataFrame(columns=['OPR_DATE','ICAO','TAVG','COUNT'])
    for i in stations:
        sql_data ="""
            select date(concat(cast(extract(YEAR FROM OPR_DATE) as char),'-',cast(extract(MONTH FROM OPR_DATE) as char),'-1')) as OPR_DATE, 
                ICAO, 
                avg((tmin+tmax)/2) as TAVG,
                count(*) as COUNT
            from WX1.WX_WEATHER_DAILY_CLEANED
            where icao='""" + i + """'
            and opr_date>=date_sub(curdate(),interval 25 month)
            group by date(concat(cast(extract(YEAR FROM OPR_DATE) as char),'-',cast(extract(MONTH FROM OPR_DATE) as char),'-1'))
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_temp = conn.query(sql_data)
        conn.close()
        df_data = df_data.append(df_temp)
    return df_data

def create_report(df_data,df_final):
    df_data = df_data.iloc[np.where(df_data['COUNT']>=28)].reset_index(drop=True)
    df_stations = df_final[['MAIN_P1','PAIR_P1']].rename(columns={'MAIN_P1':'MAIN','PAIR_P1':'PAIR'})
    df_report = pd.merge(df_stations,df_data,how='inner',left_on='MAIN',right_on='ICAO')
    df_report = pd.merge(df_report,df_data,how='inner',left_on=['PAIR','OPR_DATE'],right_on=['ICAO','OPR_DATE'])
    df_report['TAVG_DELTA'] = df_report['TAVG_x'] - df_report['TAVG_y']
    df_report['TAVG_DELTA'] = df_report['TAVG_DELTA'].round(2)
    df_report = df_report[['OPR_DATE','MAIN','PAIR','TAVG_DELTA']]
    sql_drifts = """
        select date(concat(cast(OPR_YEAR as char),'-',cast(OPR_MONTH as char),'-1')) as OPR_DATE,
            MAIN,
            DRIFT_FROM_TENYR
        from WX1.WX_STATION_ANOMALIES
    """
    conn = xdb.make_conn(db, stay_open=True)
    df_drifts = conn.query(sql_drifts)
    conn.close()
    df_report = pd.merge(df_report,df_drifts,how='inner',on=['OPR_DATE','MAIN'])
    return df_report

