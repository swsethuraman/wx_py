import pandas as pd
import numpy as np
from xlib import xdb
from datetime import datetime, timedelta
import openpyxl
import openpyxl as op
from openpyxl import load_workbook
import os
from wx.providers.common import Common_Functions as CF

def style_xlsx(current_file,sheet):
    #LOAD WORKSHEET
    wb = op.load_workbook(filename=current_file)
    ws = wb[sheet]
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 10
    ws.column_dimensions['C'].width = 10
    ws.column_dimensions['D'].width = 10
    ws.column_dimensions['E'].width = 10
    ws.column_dimensions['F'].width = 10
    ws.column_dimensions['G'].width = 10
    ws.column_dimensions['H'].width = 10
    ws.column_dimensions['I'].width = 10
    ws.column_dimensions['J'].width = 10
    ws.column_dimensions['K'].width = 10
    wb.save(filename=current_file)

def get_data(locations, fcst_days):
    loc_str = ''
    for i in locations:
        loc_str += '\'' + i + '\','
    loc_str = loc_str[:-1]
    sql_real = """
        select OPR_DATE,
            STATION,
            TMIN,
            TMAX,
            (TMIN + TMAX)/2 as TAVG
        from WX1.MS_CWG_OBS_NA_F
        where OPR_DATE>date_sub(curdate(), interval 1 year)
        and STATION in ("""+loc_str+""")
    """
    sql_fcst = """
        select 
            -- OPR_DATE,
            date_add(OPR_DATE, interval cast(replace(FORECAST_DAY,'D','') as signed) day) as OPR_DATE,
            FORECAST_DAY,
            STATION,
            FCST_MN,
            FCST_MX,
            FCST_AVG
        from WX1.MS_CWG_FCST_NA
        where OPR_DATE>date_sub(curdate(), interval 1 year)
        and STATION in ("""+loc_str+""")
    """
    db='WX1-GC'
    conn = xdb.make_conn(db, stay_open=True)
    df_real = conn.query(sql_real)
    df_fcst = conn.query(sql_fcst)
    conn.close()
    df = pd.merge(df_real,df_fcst, how='left', on=['OPR_DATE','STATION'])
    df['TMIN_BIAS'] = df['TMIN'] - df['FCST_MN']
    df['TMAX_BIAS'] = df['TMAX'] - df['FCST_MX']
    df['TAVG_BIAS'] = df['TAVG'] - df['FCST_AVG']
    df = df[['OPR_DATE','STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']]
    df = df.iloc[np.where(df['FORECAST_DAY'].isin(fcst_days))]
    max_day = df_real['OPR_DATE'].max()
    return df, max_day

def separate_intervals(max_day, df, locations, fcst_days, current_file):
    df_fcst_days = pd.DataFrame(fcst_days,columns=['FORECAST_DAY'])
    one_day = max_day
    one_week = max_day - timedelta(days=7)
    one_month = max_day - timedelta(days=30)
    three_month = max_day - timedelta(days=90)
    six_month = max_day - timedelta(days=180)
    df_1d = df[(df.OPR_DATE==one_day)].reset_index(drop=True)
    df_1w = df[(df.OPR_DATE>one_week)].reset_index(drop=True)
    df_1m = df[(df.OPR_DATE>one_month)].reset_index(drop=True)
    df_3m = df[(df.OPR_DATE>three_month)].reset_index(drop=True)
    df_6m = df[(df.OPR_DATE>six_month)].reset_index(drop=True)
    mean_1d = df_1d[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).mean().reset_index()
    mean_1w = df_1w[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).mean().reset_index()
    mean_1m = df_1m[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).mean().reset_index()
    mean_3m = df_3m[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).mean().reset_index()
    mean_6m = df_6m[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).mean().reset_index()
    std_1d = df_1d[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).std().reset_index()
    std_1w = df_1w[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).std().reset_index()
    std_1m = df_1m[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).std().reset_index()
    std_3m = df_3m[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).std().reset_index()
    std_6m = df_6m[['STATION','FORECAST_DAY','TMIN_BIAS','TMAX_BIAS','TAVG_BIAS']].groupby(['STATION','FORECAST_DAY']).std().reset_index()
    try:
        os.remove(current_file)
    except:
        pass
    for loc in locations:
        df_temp_1d = mean_1d[(mean_1d.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_1d = df_temp_1d.rename(columns={'TAVG_BIAS':'1D_BIAS'})
        df_temp_1d['1D_BIAS'] = df_temp_1d['1D_BIAS'].round(2)
        df_temp_1w = mean_1w[(mean_1w.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_1w = df_temp_1w.rename(columns={'TAVG_BIAS':'1W_BIAS'})
        df_temp_1w['1W_BIAS'] = df_temp_1w['1W_BIAS'].round(2)
        df_temp_1m = mean_1m[(mean_1m.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_1m = df_temp_1m.rename(columns={'TAVG_BIAS':'1M_BIAS'})
        df_temp_1m['1M_BIAS'] = df_temp_1m['1M_BIAS'].round(2)
        df_temp_3m = mean_3m[(mean_3m.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_3m = df_temp_3m.rename(columns={'TAVG_BIAS':'3M_BIAS'})
        df_temp_3m['3M_BIAS'] = df_temp_3m['3M_BIAS'].round(2)
        df_temp_6m = mean_6m[(mean_6m.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_6m = df_temp_6m.rename(columns={'TAVG_BIAS':'6M_BIAS'})
        df_temp_6m['6M_BIAS'] = df_temp_6m['6M_BIAS'].round(2)
        df_temp = pd.merge(df_fcst_days,df_temp_1d,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_1w,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_1m,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_3m,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_6m,how='left',on=['FORECAST_DAY'])
        df_temp_1d = std_1d[(std_1d.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_1d = df_temp_1d.rename(columns={'TAVG_BIAS':'1D_VOL'})
        df_temp_1w = std_1w[(std_1w.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_1w = df_temp_1w.rename(columns={'TAVG_BIAS':'1W_VOL'})
        df_temp_1w['1W_VOL'] = df_temp_1w['1W_VOL'].round(2)
        df_temp_1m = std_1m[(std_1m.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_1m = df_temp_1m.rename(columns={'TAVG_BIAS':'1M_VOL'})
        df_temp_1m['1M_VOL'] = df_temp_1m['1M_VOL'].round(2)
        df_temp_3m = std_3m[(std_3m.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_3m = df_temp_3m.rename(columns={'TAVG_BIAS':'3M_VOL'})
        df_temp_3m['3M_VOL'] = df_temp_3m['3M_VOL'].round(2)
        df_temp_6m = std_6m[(std_6m.STATION==loc)][['FORECAST_DAY','TAVG_BIAS']]
        df_temp_6m = df_temp_6m.rename(columns={'TAVG_BIAS':'6M_VOL'})
        df_temp_6m['6M_VOL'] = df_temp_6m['6M_VOL'].round(2)
        df_temp = pd.merge(df_temp,df_temp_1d,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_1w,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_1m,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_3m,how='left',on=['FORECAST_DAY'])
        df_temp = pd.merge(df_temp,df_temp_6m,how='left',on=['FORECAST_DAY'])
        CF.df_to_xlsx(df_temp,current_file,loc,0,0)
        style_xlsx(current_file,loc)

