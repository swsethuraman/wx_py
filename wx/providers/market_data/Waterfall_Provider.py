import pandas as pd
import numpy as np
from xlib import xdb
from wx.providers.common import Common_Functions as CF

###START OF WATERFALL FUNCTIONS
def dates(start_date, end_date):
    sql_dates = """
        select OPR_DATE
        from WX1.Util_DateList
        where OPR_DATE between date'""" + start_date + """' and date'""" + end_date + """'
    """
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_dates = conn.query(sql_dates)
    conn.close()
    return df_dates

def cwg_ref():
    sql_cwg_ref = """
        select STATION, 
            WMO, 
            WBAN 
        from WX1.CWG_REF
    """
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_cwg_ref = conn.query(sql_cwg_ref)
    conn.close()
    return df_cwg_ref

def original_daily(station, original_source, start_date):
    if original_source == 'NOAA':
        sql_original = """
            select OPR_DATE, 
                round((0.18*TMIN)+32,2) as TMIN, 
                round((0.18*TMAX)+32,2) as TMAX
            from WX1.MS_NOAA_GHCND
            where STATION like '%""" + station + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'KNMI':
        sql_original = """
            select OPR_DATE, 
                round(TMIN,2) as TMIN, 
                round(TMAX,2) as TMAX
            from WX1.KNMI_KLIMATOLOGIE_DAILY_V
            where STATION_ID=""" + station + """
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'METEOFRANCE':
        #REVIEW THIS, THE DATA IN TABLE DOESN'T LOOK RIGHT
        sql_original = """
            select OPR_DATE, 
                round(min(t)-273.15,2) as TMIN,
                round(max(t)-273.15,2) as TMAX
            from WX1.METEOFRANCE_OMM_SYNOP_3HR
            where OPR_DATE>=date'""" + start_date + """'
            and t>0
            and station='""" + station + """'
            group by opr_date
        """
    elif original_source == 'DWD':
        #REVIEW THIS, THE DATA IN TABLE DOESN'T LOOK RIGHT
        sql_original = """
            select OPR_DATE, 
                round(TMIN,2) as TMIN,
                round(TMAX,2) as TMAX
            from WX1.DWD_CLIMATE_DAILY_KL_V
            where OPR_DATE>=date'""" + start_date + """'
            and WMO='""" + station + """'
        """
    elif original_source == 'CWG_EU':
        sql_original = """
            select OPR_DATE, 
                round((TMIN-32)/1.8,2) as TMIN, 
                round((TMAX-32)/1.8,2) as TMAX
            from WX1.MS_CWG_OBS_EU
            where STATION='""" + station + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'CWG_AP':
        sql_original = """
            select OPR_DATE, 
                TMIN, 
                TMAX
            from WX1.MS_CWG_OBS_AP
            where STATION='""" + station + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'JMA':
        sql_original = """
                select OPR_DATE, 
                    TEMPR as TMIN, 
                    TEMPR as TMAX
                from WX1.JMA_DAILY_WEATHER_V
                where WMO='""" + station + """'
                and OPR_DATE>=date'""" + start_date + """'
            """
    else:
        print('NOT A VALID ORIGINAL SOURCE')
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_original = conn.query(sql_original)
    conn.close()
    return df_original

def noaa_hourly(icao, original_source, start_date):
    if original_source == 'NOAA':
        sql_noaa_hourly = """
            select OPR_DATE,
                TMIN_F as TMIN,
                TMAX_F as TMAX
            from WX1.NOAA_MADIS_DAILY_MINMAX
            where STATION='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    if original_source == 'KNMI' or  original_source == 'METEOFRANCE' or original_source == 'CWG':
        #THIS HAS BEEN ALTERED TO INTENTIONALLY RETURN NOTHING
        sql_noaa_hourly = """
            select OPR_DATE,
                TMIN_C as TMIN,
                TMAX_C as TMAX
            from WX1.NOAA_MADIS_DAILY_MINMAX
            where STATION='""" + icao + """'
            and STATION!='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_noaa_hourly = conn.query(sql_noaa_hourly)
    conn.close()
    return df_noaa_hourly

def cwg_obs(icao, original_source, start_date):
    if original_source == 'NOAA':
        sql_cwg_obs = """
            select OPR_DATE, 
                TMIN, 
                TMAX 
            from WX1.MS_CWG_OBS_NA_F 
            where STATION='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'KNMI' or  original_source == 'METEOFRANCE' or  original_source == 'DWD' or  original_source == 'CWG_EU':
        sql_cwg_obs = """
            select OPR_DATE, 
                round((TMIN-32)/1.8,2) as TMIN, 
                round((TMAX-32)/1.8,2) as TMAX
            from WX1.MS_CWG_OBS_EU 
            where STATION='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'CWG_AP' or original_source == 'JMA':
        sql_cwg_obs = """
            select OPR_DATE, 
                TMIN, 
                TMAX
            from WX1.MS_CWG_OBS_AP 
            where STATION='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_cwg_obs = conn.query(sql_cwg_obs)
    conn.close()
    return df_cwg_obs

def cwg_fcst(icao, original_source, start_date):
    if original_source == 'NOAA':
        sql_cwg_fcst = """
            select OPR_DATE, 
                FCST_MN as TMIN, 
                FCST_MX as TMAX
            from WX1.MS_CWG_FCST_NA
            where FORECAST_DAY='D0'
            and STATION='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'KNMI' or  original_source == 'METEOFRANCE' or original_source == 'DWD' or original_source == 'CWG_EU':
        sql_cwg_fcst = """
            select OPR_DATE, 
                FCST_MN as TMIN, 
                FCST_MX as TMAX
            from WX1.MS_CWG_FCST_EU
            where FORECAST_DAY='D0'
            and STATION='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    elif original_source == 'CWG_AP' or original_source == 'JMA':
        sql_cwg_fcst = """
            select OPR_DATE, 
                FCST_MN as TMIN, 
                FCST_MX as TMAX
            from WX1.MS_CWG_FCST_AP
            where FORECAST_DAY='D0'
            and STATION='""" + icao + """'
            and OPR_DATE>=date'""" + start_date + """'
        """
    db = 'WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_cwg_fcst = conn.query(sql_cwg_fcst)
    conn.close()
    return df_cwg_fcst

def waterfall_steps(df_dates, df_original, df_noaa_hourly, df_cwg_ref, df_cwg_obs, df_cwg_fcst, wmo, wban, icao, station):
    #COMBINE DATA INTO SINGLE DATAFRAME
    df_clean = pd.merge(df_dates, df_original, how='left', on = ['OPR_DATE'])
    df_clean.rename(columns={'TMIN': 'TMIN_1','TMAX':'TMAX_1'},inplace=True)
    df_clean = pd.merge(df_clean, df_noaa_hourly, how='left', on = ['OPR_DATE'])
    df_clean.rename(columns={'TMIN': 'TMIN_2','TMAX':'TMAX_2'},inplace=True)
    df_clean = pd.merge(df_clean, df_cwg_obs, how='left', on = ['OPR_DATE'])
    df_clean.rename(columns={'TMIN': 'TMIN_3','TMAX':'TMAX_3'},inplace=True)
    df_clean = pd.merge(df_clean, df_cwg_fcst, how='left', on = ['OPR_DATE'])
    df_clean.rename(columns={'TMIN': 'TMIN_4','TMAX':'TMAX_4'},inplace=True)
    #FILL GAPS BY PRIORITIZATION
    df_clean[['TMAX']] = df_clean[['TMAX_1']]
    df_clean[['TMAX']] = np.where(df_clean[['TMAX']].isnull(), df_clean[['TMAX_2']], df_clean[['TMAX']])
    df_clean[['TMAX']] = np.where(df_clean[['TMAX']].isnull(), df_clean[['TMAX_3']], df_clean[['TMAX']])
    df_clean[['TMAX']] = np.where(df_clean[['TMAX']].isnull(), df_clean[['TMAX_4']], df_clean[['TMAX']])
    df_clean[['TMIN']] = df_clean[['TMIN_1']]
    df_clean[['TMIN']] = np.where(df_clean[['TMIN']].isnull(), df_clean[['TMIN_2']], df_clean[['TMIN']])
    df_clean[['TMIN']] = np.where(df_clean[['TMIN']].isnull(), df_clean[['TMIN_3']], df_clean[['TMIN']])
    df_clean[['TMIN']] = np.where(df_clean[['TMIN']].isnull(), df_clean[['TMIN_4']], df_clean[['TMIN']])
    #PICK OUT IMPORTANT COLUMNS AND CREATE ID COLUMNS
    df_clean = df_clean[['OPR_DATE','TMIN','TMAX']]
    df_clean['ORIGINAL_ID']=station
    df_clean['WMO']=wmo
    df_clean['WBAN']=wban
    df_clean['ICAO']=icao
    df_clean = df_clean[['OPR_DATE','ORIGINAL_ID','WMO','WBAN','ICAO','TMIN','TMAX']]
    #FILTER OUR DATES BEFORE EARLIEST DATE AND AFTER MOST RECENT DATE
    df_clean = df_clean[df_clean.index>df_clean['TMIN'].first_valid_index()-1]
    df_clean = df_clean[df_clean.index<df_clean['TMIN'].last_valid_index()+1]
    #SET STATION, WBAN, WMO TO MOST RECENT NON NAN VALUES
    #APPLY LINEAR INTERPOLATION
    df_clean['TMIN'] = df_clean['TMIN'].interpolate(method='linear', limit=5)
    df_clean['TMAX'] = df_clean['TMAX'].interpolate(method='linear', limit=5)
    df_clean
    return df_clean

def waterfall(original_source, station, pairs, start_date, end_date):
    db = 'WX1-GC'
    #GET REAL WMO & WBAN. IF THIS RETURNS NONE FOR BOTH THEN NEED TO UPDATE WX1.NOAA_WMO_WBAN OR WHATEVER REF TABLE
    if original_source == 'NOAA':
        uom='F'
        sql_icao = """
            select ICAO, WMO, WBAN from WX1.NOAA_WMO_WBAN where STATIONID like '%""" + station + """'
        """
    elif original_source == 'KNMI':
        uom='C'
        sql_icao = """
            select STATION as ICAO, WMO, WBAN from WX1.CWG_REF
            where WMO in (select WMO from WX1.KNMI_REF where KNMI_ID='""" + station + """')
        """
    elif original_source == 'METEOFRANCE':
        uom='C'
        sql_icao = """
            select STATION as ICAO, WMO, WBAN from WX1.CWG_REF
            where WMO in (select MF_ID from WX1.METEOFRANCE_REF where MF_ID='""" + station + """')
        """
    elif original_source == 'CWG_EU' or original_source == 'CWG_AP':
        uom='C'
        sql_icao = """
            select STATION as ICAO, WMO, WBAN from WX1.CWG_REF where STATION='""" + station + """'
        """
    elif original_source == 'DWD':
        uom='C'
        sql_icao = """
            select ICAO, WMO, null as WBAN from WX1.DWD_REF
            where WMO='""" + station + """'
        """
    elif original_source == 'JMA':
        uom = 'C'
        sql_icao = """
                select STATION as ICAO, WMO, WBAN from WX1.CWG_REF
                where WMO='""" + station + """'
            """
    conn = xdb.make_conn(db, stay_open=True)
    df_icao = conn.query(sql_icao)
    conn.close()
    try:
        icao = str(df_icao.ICAO[0])
    except:
        icao = None
    try:
        wmo = str(df_icao.WMO[0])
    except:
        wmo = None
    try:
        wban = str(df_icao.WBAN[0])
    except:
        wban = None
    if icao==None:
        print('Need to add station to reference table')
        icao = 'NO_ICAO_CODE'
    else:
        sql_cwg_icao="""
            select STATION from WX1.CWG_REF where station='""" + icao + """'
        """
        conn = xdb.make_conn(db, stay_open=True)
        df_cwg_icao = conn.query(sql_cwg_icao)
        conn.close()
        try:
            cwg_icao = str(df_cwg_icao.STATION[0])
        except:
            cwg_icao = None
        if cwg_icao==None:
            print('No code found for hourly or CWG data, recommend adding to WX1.CWG_REF')
        df_dates = dates(start_date,end_date)
        df_cwg_ref = cwg_ref()
        df_original = original_daily(station, original_source, start_date)
        df_noaa_hourly = noaa_hourly(icao, original_source, start_date)
        df_cwg_obs = cwg_obs(icao, original_source, start_date)
        df_cwg_fcst = cwg_fcst(icao, original_source, start_date)
        df_clean = waterfall_steps(df_dates, df_original, df_noaa_hourly, df_cwg_ref, df_cwg_obs, df_cwg_fcst, wmo, wban, icao, station)
        df_clean['ORIGINAL_SOURCE']=original_source
        df_clean['UOM']=uom
        db = 'WX1-GC'
        table = 'WX_WEATHER_DAILY_CLEANED'
        CF.insert_update(db, table, df_clean,'N')

###END OF WATERFALL FUNCTIONS